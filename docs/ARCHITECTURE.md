# Arquitetura

## Visão geral

A solução usa serviços gerenciados para executar um pipeline mensal sem
servidores permanentes. O domínio é composto por dados públicos agregados por
município e competência.

| Componente | Responsabilidade |
| --- | --- |
| EventBridge Scheduler | Executar a verificação diária |
| Lambda Detector | Identificar a próxima competência publicada |
| Step Functions | Repetir lotes e iniciar a transformação |
| Lambda Worker | Coletar municípios, aplicar retry e salvar checkpoints |
| Lambda Dimension Loader | Obter a dimensão oficial de municípios do IBGE |
| Amazon S3 | Armazenar raw, checkpoints, marcadores e curated |
| AWS Glue | Transformar JSON em Parquet particionado |
| Glue Data Catalog | Registrar schema e partições |
| Amazon Athena | Executar consultas SQL |
| Secrets Manager | Armazenar a chave da API externa |

## Organização do data lake

```text
raw/
├── dim_municipios/dim_municipios.csv
└── bolsa_familia/
    └── ano=AAAA/mes=MM/uf=UF/municipio=CODIGO.json
curated/
└── bolsa_familia/ano=AAAA/mes=MM/*.parquet
_checkpoints/
└── AAAAMM.json
```

O marcador `_SUCCESS` indica que todos os municípios daquela competência
foram coletados. O checkpoint registra o próximo offset e permite retomar uma
execução interrompida.

## Decisões técnicas

- Step Functions Standard fornece histórico e coordenação durável.
- O nome determinístico da execução reduz disparos duplicados.
- Parquet reduz leitura e custo das consultas em comparação com JSON.
- Partições por ano e mês restringem o volume lido pelo Athena.
- A chave da API nunca é enviada no evento nem armazenada no S3.
- O template usa parâmetros e pseudo-parâmetros, sem ARNs reais.
