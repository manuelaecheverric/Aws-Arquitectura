#!/usr/bin/env python3
"""Silver, paso 3: enriquecimiento con Amazon Comprehend (entidades y frases clave).
Escribe SOLO en silver/, control/ y silver/_staging/. Nunca toca bronze/."""
import argparse, io, json, math, re, sys, time
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

import silver_publish as sp
from silver_classify import sha, list_keys, get_bytes

s3, BUCKET = sp.s3, sp.BUCKET
comprehend = boto3.client("comprehend", region_name="us-east-1",
                          config=Config(retries={"max_attempts": 5, "mode": "standard"}))
LANG = "es"
MAX_BYTES = 5000
WANT = {"PERSON", "ORGANIZATION", "LOCATION"}
USD_PER_UNIT = 0.0001      # estimado: verificar en la pagina de precios de Comprehend
DONE = ("ENRICHED", "CACHED", "TRUNCATED")

ENT_SCHEMA = pa.schema([("news_id", pa.string()), ("entity_text", pa.string()),
                        ("entity_type", pa.string()), ("score", pa.float64()),
                        ("text_hash", pa.string())])
KP_SCHEMA = pa.schema([("news_id", pa.string()), ("key_phrase", pa.string()),
                       ("score", pa.float64()), ("text_hash", pa.string())])

_memo = {}


def now_utc():
    return datetime.now(timezone.utc)


def build_text(row):
    t = (row["title"] or "").strip()
    d = (row["description"] or "").strip()
    text = f"{t}. {d}" if t and d else (t or d)
    raw = text.encode("utf-8")
    trunc = len(raw) > MAX_BYTES
    if trunc:
        text = raw[:MAX_BYTES].decode("utf-8", errors="ignore")
    return text, trunc


def cache_key(h):
    return f"control/comprehend_cache/text_hash={h}.json"


def get_cache(h):
    if h not in _memo:
        try:
            _memo[h] = json.loads(get_bytes(cache_key(h)))
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
                _memo[h] = None
            else:
                raise
    return _memo[h]


def analyze(text, h):
    ents = comprehend.detect_entities(Text=text, LanguageCode=LANG)["Entities"]
    kps = comprehend.detect_key_phrases(Text=text, LanguageCode=LANG)["KeyPhrases"]
    e_best, k_best = {}, {}
    for e in ents:
        if e["Type"] in WANT and e["Text"].strip():
            k = (e["Text"].strip().lower(), e["Type"])
            if k not in e_best or e["Score"] > e_best[k]["score"]:
                e_best[k] = {"text": e["Text"].strip(), "type": e["Type"], "score": float(e["Score"])}
    for p in kps:
        if p["Text"].strip():
            k = p["Text"].strip().lower()
            if k not in k_best or p["Score"] > k_best[k]["score"]:
                k_best[k] = {"text": p["Text"].strip(), "score": float(p["Score"])}
    return {"text_hash": h, "language_code": LANG, "analyzed_at_utc": now_utc().isoformat(),
            "entities": list(e_best.values()), "key_phrases": list(k_best.values())}


def write_parquet(rows, schema, key):
    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buf)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())


def existing_partitions(table):
    out = {}
    for k in list_keys(f"silver/{table}/"):
        m = re.search(r"pub_date=([^/]+)/part-0000\.parquet$", k)
        if m:
            out[m.group(1)] = k
    return out


def main():
    ap = argparse.ArgumentParser(description="Silver paso 3: Comprehend")
    ap.add_argument("--dry-run", action="store_true", help="solo estima, no llama a Comprehend ni escribe")
    ap.add_argument("--limit", type=int, help="procesar como maximo N noticias")
    ap.add_argument("--yes", action="store_true", help="no pedir confirmacion")
    ap.add_argument("--simular-fallo", action="store_true", help="falla a proposito antes de publicar news (prueba de recuperacion)")
    args = ap.parse_args()
    if not BUCKET:
        sys.exit("Falta la variable DATALAKE_BUCKET")

    state = sp.load_state()
    pending = sorted((r for r in state.values() if r["enrichment_status"] in ("PENDING", "FAILED")),
                     key=lambda r: r["news_id"])
    if args.limit:
        pending = pending[:args.limit]
    if not pending:
        print("Nada pendiente de enriquecer: no se llama a Comprehend.")
        return

    # --- plan y estimacion de costo ---
    n_empty = n_cached = n_call = units = 0
    for r in pending:
        text, _ = build_text(r)
        if not text:
            n_empty += 1
        elif get_cache(sha(text)):
            n_cached += 1
        else:
            n_call += 1
            units += 2 * max(3, math.ceil(len(text) / 100))
    print(f"Pendientes: {len(pending)} | texto vacio: {n_empty} | ya en cache: {n_cached} | "
          f"a enviar a Comprehend: {n_call}")
    print(f"Estimado maximo: {units} unidades ~ USD {units * USD_PER_UNIT:.4f}")
    if args.dry_run:
        print("DRY-RUN: no se llamo a Comprehend ni se escribio nada.")
        return
    if n_call and not args.yes and sys.stdin.isatty():
        if input("Escribe 'si' para continuar: ").strip().lower() != "si":
            print("Cancelado.")
            return

    # --- enriquecer ---
    ts = now_utc().strftime("%Y%m%dT%H%M%SZ")
    cnt = {"ENRICHED": 0, "CACHED": 0, "TRUNCATED": 0, "SKIPPED_EMPTY": 0, "FAILED": 0}
    llamadas = 0
    news_parts = set()
    for r in pending:
        news_parts.add(r["pub_date"])
        text, trunc = build_text(r)
        if not text:
            r.update(analyzed_text=None, text_hash=None, language_code=None,
                     analyzed_at_utc=now_utc().replace(tzinfo=None), enrichment_status="SKIPPED_EMPTY")
            cnt["SKIPPED_EMPTY"] += 1
            continue
        h = sha(text)
        res = get_cache(h)
        if res:
            status = "TRUNCATED" if trunc else "CACHED"
        else:
            try:
                res = analyze(text, h)
                res["truncated"] = trunc
                s3.put_object(Bucket=BUCKET, Key=cache_key(h),
                              Body=json.dumps(res, ensure_ascii=False).encode("utf-8"))
                _memo[h] = res
                llamadas += 1
                status = "TRUNCATED" if trunc else "ENRICHED"
                time.sleep(0.1)
            except (ClientError, BotoCoreError) as e:
                print(f"  FALLO {r['news_id'][:10]}: {e}")
                r["enrichment_status"] = "FAILED"
                cnt["FAILED"] += 1
                continue
        r.update(analyzed_text=text, text_hash=h, language_code=LANG,
                 analyzed_at_utc=datetime.fromisoformat(res["analyzed_at_utc"]).replace(tzinfo=None),
                 enrichment_status=status)
        cnt[status] += 1

    # --- reconstruir entidades y frases desde el estado + cache ---
    ent, kp = {}, {}
    for r in state.values():
        if r["enrichment_status"] not in DONE:
            continue
        res = get_cache(r["text_hash"])
        if not res:
            print(f"  aviso: sin cache para {r['news_id'][:10]}")
            continue
        pd_ = r["pub_date"]
        for e in res["entities"]:
            ent.setdefault(pd_, []).append({"news_id": r["news_id"], "entity_text": e["text"],
                                            "entity_type": e["type"], "score": e["score"], "text_hash": r["text_hash"]})
        for p in res["key_phrases"]:
            kp.setdefault(pd_, []).append({"news_id": r["news_id"], "key_phrase": p["text"],
                                           "score": p["score"], "text_hash": r["text_hash"]})

    # --- staging ---
    stg = f"silver/_staging/enrich-{ts}/"
    plan = []  # (key_staging, key_final)
    for table, data, schema, sortkey in (("news_entities", ent, ENT_SCHEMA, "entity_text"),
                                         ("news_key_phrases", kp, KP_SCHEMA, "key_phrase")):
        for pd_, rows in data.items():
            rows.sort(key=lambda x: (x["news_id"], x[sortkey]))
            k = f"{stg}{table}/pub_date={pd_}/part-0000.parquet"
            write_parquet(rows, schema, k)
            plan.append((k, f"silver/{table}/pub_date={pd_}/part-0000.parquet"))
    news_plan = []
    for pd_ in sorted(news_parts):
        rows = sorted((r for r in state.values() if r["pub_date"] == pd_), key=lambda r: r["news_id"])
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist([{k: r[k] for k in sp.COLS} for r in rows], schema=sp.SCHEMA), buf)
        k = f"{stg}news/pub_date={pd_}/part-0000.parquet"
        s3.put_object(Bucket=BUCKET, Key=k, Body=buf.getvalue())
        news_plan.append((k, f"silver/news/pub_date={pd_}/part-0000.parquet"))

    # --- publicar: entidades y frases primero, news al final (news es el punto de confirmacion) ---
    for k, final in plan:
        s3.copy_object(Bucket=BUCKET, Key=final, CopySource={"Bucket": BUCKET, "Key": k})
    for table, data in (("news_entities", ent), ("news_key_phrases", kp)):
        for pd_, key in existing_partitions(table).items():
            if pd_ not in data:   # particion que quedo sin filas
                s3.delete_object(Bucket=BUCKET, Key=key)
    if args.simular_fallo:
        raise RuntimeError("FALLO SIMULADO: entidades/frases publicadas, news sin actualizar")
    for k, final in news_plan:
        s3.copy_object(Bucket=BUCKET, Key=final, CopySource={"Bucket": BUCKET, "Key": k})

    # --- limpiar SOLO staging y registrar ---
    assert stg.startswith("silver/_staging/")
    keys = list_keys(stg)
    for i in range(0, len(keys), 1000):
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
    resumen = {"enrich_run": ts, "pendientes": len(pending), **cnt,
               "llamadas_comprehend_noticias": llamadas,
               "entidades": sum(len(v) for v in ent.values()),
               "frases_clave": sum(len(v) for v in kp.values()),
               "particiones_news_reescritas": len(news_plan), "terminado_utc": now_utc().isoformat()}
    s3.put_object(Bucket=BUCKET, Key=f"control/silver_enrichment_runs/enrich_{ts}.json",
                  Body=json.dumps(resumen, ensure_ascii=False, indent=2).encode("utf-8"))
    print("\nResultado:", json.dumps(resumen, ensure_ascii=False))


if __name__ == "__main__":
    main()
