# Ingesta a Bronze

Script: `src/ingest_bronze.py`. Lee las fuentes de `config/sources.yaml`.

## Organización en S3
bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/feed.xml
bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/manifest.json
control/ingestion_runs/run_id=<id>.json   (resumen del lote)

## Identificador de lote
`run_id` = fecha y hora UTC de inicio, formato AAAAMMDDTHHMMSSZ.

## Estados por fuente
- OK: feed válido con noticias.
- EMPTY: feed válido sin noticias.
- HTTP_ERROR: el servidor respondió un código distinto de 200.
- EMPTY_RESPONSE: respuesta 200 sin contenido.
- MALFORMED_XML: el contenido no es XML válido (se guarda igual para auditoría).
- NETWORK_ERROR: no hubo conexión con la fuente.

## Estado del lote
COMPLETE (todas OK), PARTIAL (algunas fallaron), FAILED (todas fallaron).
Una fuente caída queda registrada como error, nunca como "sin noticias".

## Idempotencia
El manifest se escribe al final. Si al correr un lote ya existe su manifest
con estado OK, la fuente no se vuelve a descargar. Reintentar un lote con
`--run-id` solo vuelve a intentar las fuentes que fallaron.
