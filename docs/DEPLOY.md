# Deploy de referência

O deploy é opcional: o repositório foi desenhado para avaliação de portfólio e
não depende de infraestrutura ativa.

## Pré-requisitos

- AWS CLI autenticada;
- AWS SAM CLI;
- Python 3.12;
- bucket de artefatos para o script do Glue;
- segredo existente no Secrets Manager.

O segredo pode conter uma string simples ou:

```json
{"chave-api-dados": "valor-do-segredo"}
```

## Validação

```bash
sam validate --lint --template-file infrastructure/template.yaml
pytest
```

## Publicação do script Glue

```bash
aws s3 cp glue/social_benefits_transform.py \
  s3://SEU_BUCKET_DE_ARTEFATOS/scripts/social_benefits_transform.py
```

## Build e deploy

Como os caminhos do template são relativos ao diretório `infrastructure`,
execute:

```bash
cd infrastructure
sam build --template-file template.yaml
sam deploy --guided \
  --parameter-overrides \
    PortalApiSecretName=SEU_SEGREDO \
    GlueScriptUri=s3://SEU_BUCKET_DE_ARTEFATOS/scripts/social_benefits_transform.py
```

O modo guiado solicita nome da stack, região e permissões IAM. Use uma conta de
sandbox e revise custos antes do deploy.

## Inicialização

1. Execute manualmente `DimensionLoaderFunction` uma vez.
2. Crie as tabelas usando `athena/create_tables.sql`.
3. Para teste controlado, execute o detector com `{"mes_forcado":"AAAAMM"}`.
4. Confira os objetos raw, o marcador `_SUCCESS`, o job Glue e a partição no Athena.

## Remoção

```bash
sam delete
```

O bucket precisa estar vazio para ser removido. Dados e recursos geram custos
enquanto permanecerem provisionados.
