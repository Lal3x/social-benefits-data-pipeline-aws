"""
social-benefits-month-detector
---------------------------
Lambda que detecta a publicação de um novo mês de dados.

Responsabilidade única: decidir se existe um mês novo publicado na API do
Portal da Transparência e, em caso afirmativo, disparar a state machine
`social-benefits-ingestion` com {"ano": AAAA, "mes": MM}.

NÃO reprocessa a lógica pesada do pipeline. NÃO altera a state machine.

Estratégia de idempotência (duas camadas):
  1. Lógica: só considera "candidato" o mês seguinte ao último _SUCCESS.
     Se o candidato já tiver _SUCCESS, nem sonda a API.
  2. Estrutural: StartExecution usa `name` determinístico
     ("ingestao-<ano><mes>"). Step Functions Standard rejeita um segundo
     StartExecution com o mesmo nome (ExecutionAlreadyExists) -> mesmo que
     o detector rode em paralelo ou seja invocado 10x no mesmo dia, no
     máximo 1 execução por mês é criada.

Compatibilidade com o worker existente (ingestion_worker/lambda_function.py):
  - mesmo endpoint: /novo-bolsa-familia-por-municipio
  - mesmo formato de mesAno: "{ano}{mes:02d}" (ex.: "202604" - ano primeiro)
  - mesmo header de autenticação: chave-api-dados
  - mesma estratégia de retry/backoff em 429, honrando Retry-After
  - MESMO secret do Secrets Manager (SECRET_NAME), campo "chave-api-dados"
    (com fallback pra string pura, igual ao get_chave() do worker) --
    o detector não cria nem duplica segredo, só lê o que já existe
  - _SUCCESS confirmado em raw/bolsa_familia/ano=/mes=/_SUCCESS (worker grava
    isso ao concluir os 5.571 municípios do mês)
  - município sentinela vem da dim_municipios.csv (dimension_loader/lambda_function.py);
    usamos São Paulo/SP como referência por ser o município com maior
    probabilidade de já ter registro assim que o mês é publicado.

Variáveis de ambiente esperadas:
  BUCKET_NAME             -> bucket onde ficam os _SUCCESS (raw/bolsa_familia/...)
  RAW_PREFIX              -> prefixo raiz dos dados brutos (default: raw/bolsa_familia/)
  STATE_MACHINE_ARN       -> ARN da state machine social-benefits-ingestion
  SECRET_NAME             -> nome do segredo no Secrets Manager
  API_HOST                -> host da API do Portal da Transparência
  MUNICIPIO_REF_NOME      -> nome do município sentinela (log/observabilidade)
  MUNICIPIO_REF_IBGE      -> codigoIbge do município sentinela

Evento de teste (opcional), para forçar um mês sem esperar a virada real:
  { "mes_forcado": "202605" }   # formato AAAAMM
"""

import json
import logging
import os
import time

import boto3
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")
sfn = boto3.client("stepfunctions")
secrets = boto3.client("secretsmanager")

BUCKET_NAME = os.environ["BUCKET_NAME"]
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw/bolsa_familia/")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]
SECRET_NAME = os.environ["SECRET_NAME"]
API_HOST = os.environ.get("API_HOST", "api.portaldatransparencia.gov.br")
MUNICIPIO_REF_NOME = os.environ.get("MUNICIPIO_REF_NOME", "Sao Paulo/SP")
MUNICIPIO_REF_IBGE = os.environ.get("MUNICIPIO_REF_IBGE", "3550308")

BASE_URL = f"https://{API_HOST}/api-de-dados"
ENDPOINT = "/novo-bolsa-familia-por-municipio"

MAX_TENTATIVAS = 5
BACKOFF_429_SEG = 5.0

_chave_cache: str | None = None


def _listar_meses_com_sucesso():
    """Varre o S3 sob RAW_PREFIX e retorna o conjunto de (ano, mes) que já
    têm o marcador _SUCCESS. Paginado, pois pode crescer com o tempo."""
    meses = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=RAW_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("_SUCCESS"):
                # espera algo como raw/bolsa_familia/ano=2026/mes=04/_SUCCESS
                partes = key.split("/")
                ano = mes = None
                for parte in partes:
                    if parte.startswith("ano="):
                        ano = int(parte.split("=", 1)[1])
                    elif parte.startswith("mes="):
                        mes = int(parte.split("=", 1)[1])
                if ano and mes:
                    meses.add((ano, mes))
    return meses


def _ultimo_mes_processado(meses_com_sucesso):
    if not meses_com_sucesso:
        return None
    return max(meses_com_sucesso)  # tupla (ano, mes) ordena naturalmente


def _proximo_mes(ano, mes):
    """Vira o ano corretamente: dez -> jan do ano seguinte."""
    if mes == 12:
        return ano + 1, 1
    return ano, mes + 1


def _mes_tem_sucesso(meses_com_sucesso, ano, mes):
    return (ano, mes) in meses_com_sucesso


def _obter_api_key():
    """Espelha get_chave() do worker (worker): mesmo secret, aceita tanto
    string pura quanto JSON {"chave-api-dados": "..."}, cacheado entre
    invocações quentes da mesma execução da Lambda."""
    global _chave_cache
    if _chave_cache:
        return _chave_cache
    resp = secrets.get_secret_value(SecretId=SECRET_NAME)
    segredo = resp["SecretString"]
    try:
        segredo = json.loads(segredo).get("chave-api-dados", segredo)
    except (json.JSONDecodeError, AttributeError):
        pass
    _chave_cache = segredo
    return _chave_cache


def _sondar_api(sessao, ano, mes, api_key):
    """1 único GET com o município sentinela, no mesmo endpoint/formato do
    coletor (ingestao_api.py). Resposta não-vazia => mês existe.

    Reaproveita a lógica de retry/backoff em 429 do coletor -- mesmo sendo
    uma única sonda, a API é a mesma com o mesmo rate limit, então vale a
    mesma proteção."""
    mes_ano = f"{ano}{mes:02d}"  # formato ANOMES, igual ao coletor
    url = f"{BASE_URL}{ENDPOINT}"
    params = {"mesAno": mes_ano, "codigoIbge": MUNICIPIO_REF_IBGE, "pagina": 1}
    headers = {"accept": "*/*", "chave-api-dados": api_key}

    for tentativa in range(1, MAX_TENTATIVAS + 1):
        resp = sessao.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 200:
            return bool(resp.json())  # lista vazia [] => mês ainda não publicado
        if resp.status_code == 429:
            espera = float(
                resp.headers.get("Retry-After", BACKOFF_429_SEG * (2 ** (tentativa - 1)))
            )
            logger.info(
                "rate_limit_429 municipio=%s tentativa=%s/%s aguardando=%.1fs",
                MUNICIPIO_REF_IBGE, tentativa, MAX_TENTATIVAS, espera,
            )
            time.sleep(espera)
            continue
        resp.raise_for_status()

    raise RuntimeError(f"Falha ao sondar {mes_ano} apos {MAX_TENTATIVAS} tentativas (429 persistente)")


def _disparar_state_machine(ano, mes):
    """StartExecution com nome determinístico -> proteção estrutural contra
    disparo duplicado. Se já existir execução com este nome, o SDK levanta
    ExecutionAlreadyExists e tratamos como não-erro (idempotência)."""
    nome_execucao = f"ingestao-{ano}{mes:02d}"
    entrada = json.dumps({"ano": ano, "mes": mes})

    try:
        sfn.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=nome_execucao,
            input=entrada,
        )
        logger.info(
            "execucao_disparada mes=%s%02d nome_execucao=%s",
            ano, mes, nome_execucao,
        )
        return True
    except sfn.exceptions.ExecutionAlreadyExists:
        logger.info(
            "execucao_ja_existe mes=%s%02d nome_execucao=%s -- idempotencia ok, nada a fazer",
            ano, mes, nome_execucao,
        )
        return False


def lambda_handler(event, context):
    event = event or {}

    meses_com_sucesso = _listar_meses_com_sucesso()
    ultimo = _ultimo_mes_processado(meses_com_sucesso)

    if "mes_forcado" in event:
        # Modo de teste: permite forçar o candidato sem esperar a virada real,
        # conforme o roteiro do ticket (apagar _SUCCESS de um mês conhecido).
        aaaamm = str(event["mes_forcado"])
        if len(aaaamm) != 6 or not aaaamm.isdigit():
            raise ValueError("mes_forcado deve usar o formato AAAAMM")
        ano_candidato, mes_candidato = int(aaaamm[:4]), int(aaaamm[4:6])
        if not 1 <= mes_candidato <= 12:
            raise ValueError("mes_forcado deve conter um mês entre 01 e 12")
    elif ultimo is None:
        logger.warning("nenhum_mes_processado_ainda -- configure um ponto de partida manual")
        return {"acao": "nenhuma", "motivo": "sem_historico"}
    else:
        ano_candidato, mes_candidato = _proximo_mes(*ultimo)

    logger.info(
        "ultimo_processado=%s candidato=%s-%02d",
        ultimo, ano_candidato, mes_candidato,
    )

    # Camada 1 de idempotência: já processado? nem sonda a API.
    if _mes_tem_sucesso(meses_com_sucesso, ano_candidato, mes_candidato):
        logger.info("mes_ja_processado mes=%s%02d", ano_candidato, mes_candidato)
        return {"acao": "nenhuma", "motivo": "mes_ja_processado"}

    api_key = _obter_api_key()
    sessao = requests.Session()
    publicado = _sondar_api(sessao, ano_candidato, mes_candidato, api_key)

    if not publicado:
        logger.info(
            "mes_ainda_nao_publicado mes=%s%02d municipio_ref=%s (%s)",
            ano_candidato, mes_candidato, MUNICIPIO_REF_NOME, MUNICIPIO_REF_IBGE,
        )
        return {"acao": "nenhuma", "motivo": "mes_ainda_nao_publicado"}

    # Camada 2 de idempotência: name determinístico no StartExecution.
    disparou = _disparar_state_machine(ano_candidato, mes_candidato)
    return {
        "acao": "disparou" if disparou else "nenhuma",
        "ano": ano_candidato,
        "mes": mes_candidato,
    }
