"""Segundo lote de prueba: solo 2 noticias nuevas (para fallos controlados).

Uso:
  python src/make_batch_fallo.py            # dry-run
  python src/make_batch_fallo.py --apply
"""
import argparse
import hashlib
import json
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import make_test_batch as mt

BASE_RUN = "20261101T182352Z"  # lote de prueba anterior
NUEVAS = {
    "eltiempo_justicia": (
        "Prueba fallo 1: nueva noticia de justicia",
        "https://www.eltiempo.com/prueba/fallo-1-justicia",
        "Noticia nueva para probar el fallo controlado en Silver, enriquecimiento y Gold.",
        "2026-11-01T09:00:00-05:00"),
    "eltiempo_politica": (
        "Prueba fallo 2: nueva noticia de politica",
        "https://www.eltiempo.com/prueba/fallo-2-politica",
        "Segunda noticia nueva para la prueba de recuperacion y reejecucion.",
        "2026-10-31T15:00:00-05:00"),
}


def valido(it):
    title = (it.findtext("title") or "").strip()
    link = (it.findtext("link") or "").strip().lower()
    return bool(title) and link.startswith(("http://", "https://"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    ing_date = now.strftime("%Y-%m-%d")
    iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    writes, counts, total = [], {}, 0

    for sid in mt.SOURCES:
        mkey = next(k for k in mt.list_keys(f"bronze/source={sid}/")
                    if k.endswith(f"run_id={BASE_RUN}/manifest.json"))
        manifest = json.loads(mt.get_bytes(mkey))
        root = ET.fromstring(mt.get_bytes(manifest["s3_uri"].split("/", 3)[3]))
        channel = root.find("channel")
        for it in channel.findall("item"):
            if not valido(it):
                channel.remove(it)
        if sid in NUEVAS:
            title, link, desc, pub = NUEVAS[sid]
            tpl = channel.findall("item")[-1]
            channel.append(mt.new_item(tpl, title, link, desc, pub))
        n = len(channel.findall("item"))
        xml_bytes = ET.tostring(root, encoding="utf-8", xml_declaration=True)
        base = f"bronze/source={sid}/ingestion_date={ing_date}/run_id={run_id}"
        manifest.update({
            "run_id": run_id,
            "s3_uri": f"s3://{mt.BUCKET}/{base}/feed.xml",
            "ingested_at_utc": iso,
            "content_sha256": hashlib.sha256(xml_bytes).hexdigest(),
            "bytes": len(xml_bytes),
            "items_received": n,
            "derived_from_run_id": BASE_RUN,
            "nota": "copia controlada: lote para pruebas de fallo (sección 4.5)",
        })
        writes.append((f"{base}/feed.xml", xml_bytes))
        writes.append((f"{base}/manifest.json", json.dumps(manifest, indent=2).encode()))
        counts[sid] = n
        total += n

    summary = json.loads(mt.get_bytes(f"control/ingestion_runs/run_id={BASE_RUN}.json"))
    summary.update({"run_id": run_id, "terminado_utc": iso, "noticias_recibidas": total,
                    "nota": f"lote de prueba de fallos derivado de {BASE_RUN}"})
    for f in summary["fuentes"]:
        f["items_received"] = counts[f["source_id"]]
    writes.append((f"control/ingestion_runs/run_id={run_id}.json",
                   json.dumps(summary, indent=2).encode()))  # marcador al final

    print(f"Lote de fallos: run_id={run_id} (base {BASE_RUN}), {total} items {counts}")
    for k, b in writes:
        print(f"  s3://{mt.BUCKET}/{k}  ({len(b)} bytes)")
    existentes = [k for k, _ in writes if mt.exists(k)]
    if existentes:
        raise SystemExit(f"ABORTA: ya existen {existentes}")
    if not a.apply:
        print("\nDRY-RUN: no se escribió nada. Usa --apply.")
        return
    for k, b in writes:
        mt.s3.put_object(Bucket=mt.BUCKET, Key=k, Body=b, ServerSideEncryption="AES256")
    print(f"\nLote {run_id} creado.")


if __name__ == "__main__":
    main()