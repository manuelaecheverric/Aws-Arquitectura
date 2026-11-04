"""
Ingesta de feeds RSS a la capa Bronze de S3.

Uso:
  python src/ingest_bronze.py                      -> lote nuevo
  python src/ingest_bronze.py --run-id 20261022T130000Z  -> reintentar un lote existente
"""
import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime, timezone
from xml.etree import ElementTree as ET

import boto3
import requests
import yaml
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ingesta")
s3 = boto3.client("s3")

# Estados posibles de una fuente en un lote
ESTADOS_OK = ("OK", "EMPTY")   # la descarga funcionó (EMPTY = feed válido pero sin noticias)
# Estados de error: HTTP_ERROR, EMPTY_RESPONSE, MALFORMED_XML, NETWORK_ERROR


def nuevo_run_id():
    """Identificador del lote: fecha y hora en UTC, ej. 20261022T130000Z."""
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def fecha_de_run_id(run_id):
    """20261022T130000Z -> 2026-10-22 (para la carpeta ingestion_date=)."""
    return f"{run_id[0:4]}-{run_id[4:6]}-{run_id[6:8]}"


def cargar_fuentes(ruta):
    with open(ruta, encoding="utf-8") as f:
        return yaml.safe_load(f)["sources"]


def leer_json_s3(bucket, key):
    """Devuelve el JSON guardado en S3, o None si el archivo no existe."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(obj["Body"].read())


def escribir_json_s3(bucket, key, datos):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(datos, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def ingestar_fuente(fuente, run_id, bucket):
    sid = fuente["source_id"]
    etiqueta = f"[run_id={run_id}][source={sid}]"
    prefijo = f"bronze/source={sid}/ingestion_date={fecha_de_run_id(run_id)}/run_id={run_id}/"
    key_manifest = prefijo + "manifest.json"

    # 1. Idempotencia: si este lote ya se descargó bien para esta fuente, no se repite
    previo = leer_json_s3(bucket, key_manifest)
    if previo and previo.get("status") in ESTADOS_OK:
        log.info("%s ya existe en Bronze (status=%s): no se vuelve a descargar", etiqueta, previo["status"])
        return previo

    manifest = {
        "run_id": run_id,
        "source_id": sid,
        "medio": fuente["medio"],
        "seccion": fuente["seccion"],
        "url": fuente["url"],
        "ingested_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "http_status": None,
        "content_sha256": None,
        "bytes": 0,
        "items_received": 0,
        "s3_uri": None,
        "status": None,
        "error": None,
    }

    try:
        # 2. Descargar el feed
        resp = requests.get(fuente["url"], timeout=20,
                            headers={"User-Agent": "Mozilla/5.0 (proyecto academico EIA)"})
        manifest["http_status"] = resp.status_code
        cuerpo = resp.content

        if resp.status_code != 200:
            manifest["status"] = "HTTP_ERROR"
            manifest["error"] = f"El servidor respondió HTTP {resp.status_code}"
        elif len(cuerpo) == 0:
            manifest["status"] = "EMPTY_RESPONSE"
            manifest["error"] = "Respuesta 200 sin contenido"
        else:
            manifest["bytes"] = len(cuerpo)
            manifest["content_sha256"] = hashlib.sha256(cuerpo).hexdigest()

            # 3. Comprobar que es XML válido y contar noticias
            try:
                raiz = ET.fromstring(cuerpo)
                n = len(raiz.findall(".//item"))
                manifest["items_received"] = n
                manifest["status"] = "OK" if n > 0 else "EMPTY"
            except ET.ParseError as e:
                manifest["status"] = "MALFORMED_XML"
                manifest["error"] = f"XML inválido: {e}"

            # 4. Guardar el original tal cual (también si está dañado, para poder auditarlo)
            key_xml = prefijo + "feed.xml"
            s3.put_object(Bucket=bucket, Key=key_xml, Body=cuerpo, ContentType="application/xml")
            manifest["s3_uri"] = f"s3://{bucket}/{key_xml}"

    except requests.RequestException as e:
        manifest["status"] = "NETWORK_ERROR"
        manifest["error"] = f"No se pudo conectar: {e}"

    # 5. El manifest se escribe AL FINAL: si existe con status OK, esa fuente está completa
    escribir_json_s3(bucket, key_manifest, manifest)

    if manifest["status"] in ESTADOS_OK:
        log.info("%s status=%s http=%s noticias=%s", etiqueta, manifest["status"],
                 manifest["http_status"], manifest["items_received"])
    else:
        log.error("%s status=%s error=%s", etiqueta, manifest["status"], manifest["error"])
    return manifest


def main():
    p = argparse.ArgumentParser(description="Ingesta de feeds RSS a Bronze")
    p.add_argument("--config", default="config/sources.yaml", help="archivo de fuentes")
    p.add_argument("--bucket", default=os.environ.get("DATALAKE_BUCKET"), help="bucket S3")
    p.add_argument("--run-id", help="lote existente a reintentar (si no se da, se crea uno nuevo)")
    args = p.parse_args()

    if not args.bucket:
        sys.exit("Falta el bucket: usa --bucket o la variable DATALAKE_BUCKET")

    run_id = args.run_id or nuevo_run_id()
    fuentes = cargar_fuentes(args.config)
    log.info("[run_id=%s] Inicia ingesta de %d fuentes", run_id, len(fuentes))

    resultados = [ingestar_fuente(f, run_id, args.bucket) for f in fuentes]

    # 6. Estado del lote: COMPLETE / PARTIAL / FAILED
    buenas = sum(r["status"] in ESTADOS_OK for r in resultados)
    if buenas == len(resultados):
        estado = "COMPLETE"
    elif buenas == 0:
        estado = "FAILED"
    else:
        estado = "PARTIAL"

    resumen = {
        "run_id": run_id,
        "estado": estado,
        "terminado_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "fuentes_ok": buenas,
        "fuentes_total": len(resultados),
        "noticias_recibidas": sum(r["items_received"] for r in resultados),
        "fuentes": [{"source_id": r["source_id"], "status": r["status"],
                     "items_received": r["items_received"], "error": r["error"]}
                    for r in resultados],
    }
    escribir_json_s3(args.bucket, f"control/ingestion_runs/run_id={run_id}.json", resumen)

    print("\n=== Resumen del lote ===")
    print(f"run_id: {run_id}   estado: {estado}")
    for r in resultados:
        print(f"  {r['source_id']:<25} {r['status']:<15} noticias={r['items_received']}")

    # Si fallaron todas, el script termina con error (Airflow lo marcará en rojo y reintentará)
    sys.exit(1 if estado == "FAILED" else 0)


if __name__ == "__main__":
    main()
