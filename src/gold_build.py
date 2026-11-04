#!/usr/bin/env python3
"""Gold: reconstruye agregados completos desde Silver y los publica de forma idempotente."""
import argparse, hashlib, io, json, os, re, sys
from datetime import datetime, timezone
import boto3
import pyarrow as pa
import pyarrow.parquet as pq

BUCKET = os.environ.get("DATALAKE_BUCKET")
s3 = boto3.client("s3", region_name="us-east-1")
PART_RE = re.compile(r"/pub_date=([^/]+)/")
# tabla -> columnas de dimension (pub_date va solo en la ruta)
TABLES = {
    "news_daily": ["medio", "seccion_principal"],
    "entity_daily": ["entity_type", "entity_text"],
    "key_phrase_section_daily": ["seccion_principal", "key_phrase"],
}


def keys(prefix):
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        out += [o["Key"] for o in page.get("Contents", [])]
    return out


def read_parquet_prefix(prefix):
    """Devuelve [(pub_date, fila_dict)] de todos los Parquet bajo prefix (sin _staging)."""
    rows = []
    for k in keys(prefix):
        m = PART_RE.search(k)
        if not m or "_staging" in k or not k.endswith(".parquet"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()
        rows += [(m.group(1), r) for r in pq.read_table(io.BytesIO(body)).to_pylist()]
    return rows


def build_gold():
    news = read_parquet_prefix("silver/news/")
    ents = read_parquet_prefix("silver/news_entities/")
    phrs = read_parquet_prefix("silver/news_key_phrases/")
    ids = [r["news_id"] for _, r in news]
    if len(ids) != len(set(ids)):
        sys.exit("ABORTA: news_id repetido en Silver")
    info = {r["news_id"]: (p, r.get("medio") or "sin_dato", r.get("seccion_principal") or "sin_dato")
            for p, r in news}
    huerf = [r["news_id"] for _, r in ents + phrs if r["news_id"] not in info]
    if huerf:
        sys.exit(f"ABORTA: {len(huerf)} entidades/frases sin noticia asociada")

    agg = {t: {} for t in TABLES}
    for nid, (p, medio, sec) in info.items():
        agg["news_daily"].setdefault((p, medio, sec), set()).add(nid)
    for _, r in ents:
        p = info[r["news_id"]][0]
        agg["entity_daily"].setdefault((p, r["entity_type"], r["entity_text"]), set()).add(r["news_id"])
    for _, r in phrs:
        p, _, sec = info[r["news_id"]]
        agg["key_phrase_section_daily"].setdefault((p, sec, r["key_phrase"]), set()).add(r["news_id"])

    gold = {t: sorted((*k, len(v)) for k, v in agg[t].items()) for t in TABLES}
    expected = {
        "news": len(news),
        "news_per_pub_date": {},
        "entity_rows": len({(r["news_id"], r["entity_type"], r["entity_text"]) for _, r in ents}),
        "phrase_rows": len({(r["news_id"], r["key_phrase"]) for _, r in phrs}),
    }
    for p, _ in news:
        expected["news_per_pub_date"][p] = expected["news_per_pub_date"].get(p, 0) + 1
    return gold, expected


def to_partitions(table, rows):
    dims = TABLES[table]
    parts = {}
    for row in rows:
        parts.setdefault(row[0], []).append(row[1:])
    out = {}
    for p, rs in parts.items():
        cols = {d: [r[i] for r in rs] for i, d in enumerate(dims)}
        cols["news_count"] = [r[-1] for r in rs]
        schema = pa.schema([(d, pa.string()) for d in dims] + [("news_count", pa.int64())])
        out[p] = pa.table(cols, schema=schema)
    return out


def last_success_fingerprint():
    for k in sorted(keys("control/gold_runs/"), reverse=True):
        if k.endswith(".json"):
            m = json.loads(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
            if m.get("status") == "SUCCESS":
                return m.get("fingerprint")
    return None


def validate_published(expected):
    """Lee Gold desde S3 y concilia contra los conteos de Silver."""
    res, ok = {}, True
    nd = read_parquet_prefix("gold/news_daily/")
    by_date = {}
    for p, r in nd:
        by_date[p] = by_date.get(p, 0) + r["news_count"]
    res["news_daily_total"] = sum(by_date.values())
    res["news_daily_vs_silver"] = res["news_daily_total"] == expected["news"]
    res["news_daily_por_fecha_ok"] = by_date == expected["news_per_pub_date"]
    res["entity_daily_total"] = sum(r["news_count"] for _, r in read_parquet_prefix("gold/entity_daily/"))
    res["entity_daily_vs_silver"] = res["entity_daily_total"] == expected["entity_rows"]
    res["key_phrase_total"] = sum(r["news_count"] for _, r in read_parquet_prefix("gold/key_phrase_section_daily/"))
    res["key_phrase_vs_silver"] = res["key_phrase_total"] == expected["phrase_rows"]
    ok = all(v for k, v in res.items() if k.endswith(("_silver", "_ok")))
    return ok, res


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--simular-fallo", action="store_true",
                    help="falla a mitad de la publicacion para probar la recuperacion")
    a = ap.parse_args()
    if not BUCKET:
        sys.exit("Falta export DATALAKE_BUCKET=data-lake-sct")

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    started = datetime.now(timezone.utc).isoformat()
    gold, expected = build_gold()
    parts = {t: to_partitions(t, gold[t]) for t in TABLES}
    fingerprint = hashlib.sha256(json.dumps(gold, sort_keys=True).encode()).hexdigest()
    resumen = {t: {"filas": len(gold[t]), "particiones": sorted(parts[t])} for t in TABLES}
    print(f"Silver: {expected['news']} noticias vigentes")
    for t in TABLES:
        print(f"  {t}: {resumen[t]['filas']} filas en {len(parts[t])} particiones")
    if a.dry_run:
        print("[dry-run] no se escribio nada")
        return

    existentes = set(keys("gold/"))
    esperadas = {f"gold/{t}/pub_date={p}/part-0000.parquet" for t in TABLES for p in parts[t]}
    if last_success_fingerprint() == fingerprint and esperadas <= existentes:
        print("Gold ya esta al dia (Silver no cambio). Nada que publicar.")
        return

    # 1) todo a staging
    for t in TABLES:
        for p, tbl in parts[t].items():
            buf = io.BytesIO()
            pq.write_table(tbl, buf, compression="snappy")
            s3.put_object(Bucket=BUCKET, Body=buf.getvalue(),
                          Key=f"gold/_staging/run_id={run_id}/{t}/pub_date={p}/part-0000.parquet")
    # 2) copia atomica por particion, un archivo de nombre fijo
    copiados = 0
    for t in TABLES:
        for p in parts[t]:
            src = f"gold/_staging/run_id={run_id}/{t}/pub_date={p}/part-0000.parquet"
            s3.copy_object(Bucket=BUCKET, Key=f"gold/{t}/pub_date={p}/part-0000.parquet",
                           CopySource={"Bucket": BUCKET, "Key": src})
            copiados += 1
            if a.simular_fallo and copiados == 1:
                raise RuntimeError("FALLO SIMULADO tras copiar la primera particion (sin marcador)")
    # 3) borrar particiones de Gold que ya no existen (se reconstruye completo)
    for t in TABLES:
        for k in keys(f"gold/{t}/"):
            m = PART_RE.search(k)
            if m and m.group(1) not in parts[t]:
                s3.delete_object(Bucket=BUCKET, Key=k)
    # 4) conciliar contra Silver leyendo lo publicado
    ok, rec = validate_published(expected)
    for k, v in rec.items():
        print(f"  {k}: {v}")
    # 5) marcador al final
    marker = {"run_id": run_id, "started_utc": started,
              "finished_utc": datetime.now(timezone.utc).isoformat(),
              "status": "SUCCESS" if ok else "FAILED_VALIDATION",
              "fingerprint": fingerprint, "silver_news": expected["news"],
              "tablas": resumen, "conciliacion": rec}
    s3.put_object(Bucket=BUCKET, Key=f"control/gold_runs/run_id={run_id}.json",
                  Body=json.dumps(marker, indent=2, ensure_ascii=False).encode())
    # 6) limpiar staging
    for k in keys("gold/_staging/"):
        s3.delete_object(Bucket=BUCKET, Key=k)
    print("Gold publicado OK" if ok else "CONCILIACION FALLO")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()