# Lakehouse Architecture (PRD v2.0)

## Camadas obrigatórias

### RAW
- Sempre antes de qualquer transformação
- Arquivo original comprimido (gzip)
- Estrutura:
  /raw/source={source}/YYYY/MM/DD/batch_{id}.ext.gz
- Usado apenas para auditoria e reprocessamento

### BRONZE
- Formato: Parquet
- Versionamento: Apache Iceberg
- Imutável (nunca sobrescrever)
- Estrutura:
  /bronze/source={source}/year=YYYY/month=MM/day=DD/

Colunas obrigatórias:
- ingestion_timestamp
- source
- batch_id
- raw_path

### QUARENTENA
- Local: /bronze/quarantine/
- Dados inválidos NÃO param pipeline
- Deve incluir coluna validation_error

---

## Ingestão padrão (OBRIGATÓRIO)

Extract  
→ Preserve Raw  
→ Normalize  
→ Validate Schema (YAML)  
→ Load Bronze  
→ Quarantine  

---

## Regras críticas

- Nunca pular RAW
- Nunca escrever direto no Bronze sem validação
- Schema YAML obrigatório por fonte
- Nenhuma credencial no código (usar Airflow Connections)
- Pipeline nunca pode parar por erro de dado
- Dados inválidos vão para quarentena

---

## Stack

- Airflow (orquestração)
- MinIO (data lake)
- Iceberg (versionamento)
- dbt (transformação)
- ClickHouse (gold)

---

## Padrões

- Código modular
- snake_case
- logs obrigatórios
- pronto para escala