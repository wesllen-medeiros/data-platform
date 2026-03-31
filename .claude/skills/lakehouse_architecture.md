# Lakehouse Architecture Skill

Este projeto segue arquitetura lakehouse com medallion:

## Camadas

- Raw:
  - dados originais gzip no MinIO
  - nunca modificados
  - usados para reprocessamento

- Bronze:
  - Parquet + Apache Iceberg
  - imutável
  - contém colunas:
    ingestion_timestamp, source, batch_id, raw_path

- Silver:
  - transformação via dbt
  - limpeza, deduplicação, tipagem
  - nomes snake_case

- Gold:
  - modelagem analítica
  - ClickHouse
  - tabelas fato e dimensão

## Regras obrigatórias

- Raw sempre deve existir antes de qualquer transformação
- Schema deve ser validado via YAML antes do Bronze
- Dados inválidos devem ir para quarentena
- Nenhum pipeline pode parar por erro de dado
- Sem credenciais no código (usar Airflow Connections)

## Stack

- Airflow (orquestração)
- MinIO (data lake)
- Iceberg (versionamento)
- dbt (transformação)
- ClickHouse (warehouse)
- Superset (BI)

## Padrões

- Código modular
- Separação por responsabilidade
- Pronto para escala