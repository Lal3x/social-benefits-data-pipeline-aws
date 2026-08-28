# Modelo de dados

## Fato `bolsa_familia`

| Coluna | Tipo | Descrição |
| --- | --- | --- |
| `id` | bigint | Identificador fornecido pela API |
| `data_referencia` | date | Competência do pagamento |
| `codigo_ibge` | string | Código oficial do município |
| `municipio` | string | Nome do município |
| `uf_sigla` | string | Unidade federativa |
| `regiao_nome` | string | Região brasileira |
| `programa` | string | Descrição do programa |
| `valor` | double | Valor agregado |
| `qtd_beneficiados` | bigint | Quantidade agregada de beneficiários |
| `ano` | int | Partição anual |
| `mes` | int | Partição mensal |

## Dimensão `dim_municipios`

Contém código IBGE, município, UF, região, mesorregião e microrregião. A fonte é
a API pública de Localidades do IBGE.
