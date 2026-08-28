# Segurança

## Controles aplicados

- bucket com acesso público bloqueado, criptografia e versionamento;
- chave da API armazenada no Secrets Manager;
- políticas separadas para leitura, escrita e orquestração;
- parâmetros e pseudo-parâmetros em vez de números de conta e ARNs reais;
- ausência de snapshots exportados do console;
- logs sem conteúdo do segredo;
- dependências e código validados no CI.

## Modelo de dados

O pipeline trabalha com valores e quantidades agregados por município. Não há
necessidade de armazenar CPF, nome de beneficiário ou outro identificador
individual neste projeto.

## Recomendações para produção

- usar KMS gerenciado pelo cliente quando exigido;
- definir retenção dos logs;
- ativar alertas para falhas da Step Function e do Glue;
- restringir a origem permitida no Secrets Manager;
- habilitar análise de dependências e secret scanning;
- revisar os termos e limites das APIs oficiais;
- usar uma conta AWS de sandbox separada.
