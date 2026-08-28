CREATE DATABASE IF NOT EXISTS transparencia;

-- fato: Parquet particionado
CREATE EXTERNAL TABLE IF NOT EXISTS transparencia.bolsa_familia (
  id bigint, data_referencia date, codigo_ibge string, municipio string,
  uf_sigla string, regiao_nome string, programa string,
  valor double, qtd_beneficiados bigint
) PARTITIONED BY (ano int, mes int)
STORED AS PARQUET
LOCATION 's3://<BUCKET_NAME>/curated/bolsa_familia/';

-- descobre as partições ano=/mes= já existentes (sem isso, a tabela retorna 0 linhas)
MSCK REPAIR TABLE transparencia.bolsa_familia;

-- dimensão: CSV com cabeçalho
CREATE EXTERNAL TABLE IF NOT EXISTS transparencia.dim_municipios (
  codigo_ibge string, municipio string, uf_sigla string, uf_nome string,
  uf_codigo string, regiao_sigla string, regiao_nome string,
  mesorregiao string, microrregiao string
) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 's3://<BUCKET_NAME>/raw/dim_municipios/'
TBLPROPERTIES ('skip.header.line.count'='1');

WITH fato_ano AS (
  SELECT codigo_ibge, SUM(valor) AS valor_ano
  FROM transparencia.bolsa_familia WHERE ano = YEAR(CURRENT_DATE) GROUP BY codigo_ibge
)
SELECT d.municipio, d.uf_sigla, f.valor_ano
FROM fato_ano f
JOIN transparencia.dim_municipios d ON d.codigo_ibge = f.codigo_ibge
ORDER BY f.valor_ano DESC
LIMIT 15;


