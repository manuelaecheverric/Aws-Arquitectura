"""Crea un lote de prueba en Bronze (copia controlada de un lote real).

Uso:
  python src/make_test_batch.py            # dry-run (no escribe nada)
  python src/make_test_batch.py --apply    # escribe el lote en S3
"""
import argparse
import copy
import hashlib
import json
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("DATALAKE_BUCKET", "data-lake-sct")
BASE_RUN = "20261031T001429Z"
SOURCES = ["eltiempo_justicia", "eltiempo_arte_teatro", "eltiempo_politica"]
SHARED_LINK = "https://www.eltiempo.com/prueba/noticia-en-dos-feeds"
s3 = boto3.client("s3", region_name="us-east-1")


def list_keys(prefix):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def get_bytes(key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def exists(key):
    return bool(list_keys(key))


def set_text(item, tag, text):
    el = item.find(tag)
    if el is None:
        el = ET.SubElement(item, tag)
    el.text = text


def new_item(template, title, link, desc, pub):
    it = copy.deepcopy(template)
    set_text(it, "title", title)
    set_text(it, "link", link)
    set_text(it, "guid", link)  # en estos feeds guid == link
    set_text(it, "description", desc)
    set_text(it, "pubDate", pub)
    return it


def mutate(sid, root, cambios):
    channel = root.find("channel")
    items = channel.findall("item")
    tpl = items[-1]
    if sid == "eltiempo_justicia":
        items[0].remove(items[0].find("title"))
        cambios.append(f"{sid}: A sin título -> '{items[0].findtext('link')}'")
        items[1].find("link").text = "enlace-no-valido-prueba"
        cambios.append(f"{sid}: B link inválido (item 2)")
        d = items[2].find("description")
        d.text = (d.text or "") + " Texto ampliado en la copia controlada de prueba."
        cambios.append(f"{sid}: D descripción modificada -> {items[2].findtext('link')}")
        channel.append(new_item(
            tpl, "Prueba: noticia con fecha de publicación inválida",
            "https://www.eltiempo.com/prueba/sin-fecha-valida",
            "Noticia de prueba con pubDate no interpretable.", "no es una fecha"))
        cambios.append(f"{sid}: C nueva con pubDate inválido")
    else:
        channel.append(new_item(
            tpl, "Prueba: noticia publicada en dos secciones", SHARED_LINK,
            "Misma noticia de prueba presente en dos feeds.", "2026-10-31T10:00:00-05:00"))
        cambios.append(f"{sid}: E nueva (link compartido, pub 2026-10-31)")
    return len(channel.findall("item"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    ing_date = now.strftime("%Y-%m-%d")
    iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    cambios, writes, total = [], [], 0
    counts = {}

    for sid in SOURCES:
        mkey = next(k for k in list_keys(f"bronze/source={sid}/")
                    if k.endswith(f"run_id={BASE_RUN}/manifest.json"))
        manifest = json.loads(get_bytes(mkey))
        root = ET.fromstring(get_bytes(manifest["s3_uri"].split("/", 3)[3]))
        n = mutate(sid, root, cambios)
        xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        base = f"bronze/source={sid}/ingestion_date={ing_date}/run_id={run_id}"
        manifest.update({
            "run_id": run_id,
            "s3_uri": f"s3://{BUCKET}/{base}/feed.xml",
            "ingested_at_utc": iso,
            "content_sha256": hashlib.sha256(xml_bytes).hexdigest(),
            "bytes": len(xml_bytes),
            "items_received": n,
            "derived_from_run_id": BASE_RUN,
            "nota": "copia controlada para pruebas (sección 4.5)",
        })
        writes.append((f"{base}/feed.xml", xml_bytes))
        writes.append((f"{base}/manifest.json",
                       json.dumps(manifest, indent=2).encode("utf-8")))
        counts[sid] = n
        total += n

    summary = json.loads(get_bytes(f"control/ingestion_runs/run_id={BASE_RUN}.json"))
    summary.update({"run_id": run_id, "terminado_utc": iso,
                    "noticias_recibidas": total,
                    "nota": f"lote de prueba derivado de {BASE_RUN}"})
    for f in summary["fuentes"]:
        f["items_received"] = counts[f["source_id"]]
    # el marcador va al final: Silver solo ve el lote cuando está completo
    writes.append((f"control/ingestion_runs/run_id={run_id}.json",
                   json.dumps(summary, indent=2).encode("utf-8")))

    print(f"Lote de prueba: run_id={run_id} (base {BASE_RUN}), {total} items")
    for c in cambios:
        print("  -", c)
    for k, b in writes:
        print(f"  s3://{BUCKET}/{k}  ({len(b)} bytes)")

    existentes = [k for k, _ in writes if exists(k)]
    if existentes:
        raise SystemExit(f"ABORTA: ya existen {existentes} (sin versionado, no se sobrescribe)")
    if not a.apply:
        print("\nDRY-RUN: no se escribió nada. Usa --apply para crear el lote.")
        return
    for k, b in writes:
        s3.put_object(Bucket=BUCKET, Key=k, Body=b, ServerSideEncryption="AES256")
    print(f"\nLote {run_id} creado. Siguiente: silver_publish.py --dry-run")


if __name__ == "__main__":
    main()