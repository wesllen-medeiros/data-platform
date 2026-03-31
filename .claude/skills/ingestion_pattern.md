# Ingestion Pattern

Todo pipeline deve seguir:

Extract
→ Raw (gzip no MinIO)
→ Normalize (Parquet)
→ Validate Schema (YAML)
→ Bronze (Iceberg)
→ Quarantine (se inválido)

Regras:
- Nunca pular Raw
- Nunca escrever direto no Bronze sem validação
- Sempre incluir colunas de controle