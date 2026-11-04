#!/usr/bin/env python3
"""Silver, paso 2: publica la tabla news en S3 (sin Comprehend todavia).
Escribe SOLO en silver/, quarantine/silver/ y control/silver_runs/. Nunca toca bronze/."""
import argparse, io, json, os, re, sys
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

import silver_classify as sc
from silver_classify import (sha, normalize_link, clean_text, parse_date,
                             get_bytes, list_keys, list_runs, source_index, VALID)

s3 = sc.s3
BUCKET = sc.BUCKET

SCHEMA = pa.schema([
    ("news_id", pa.string()), ("guid", pa.string()), ("link", pa.string()),
    ("title", pa.string()), ("description", pa.string()),
    ("pub_date_original", pa.string()), ("published_at_utc", pa.timestamp("ms")),
    ("medio", pa.string()), ("seccion_principal", pa.string()),
    ("secciones", pa.list_(pa.string())), ("content_hash", pa.string()),
    ("first_seen_run_id", pa.string()), ("last_seen_run_id", pa.string()),
    ("updated_run_id", pa.string()), ("bronze_run_id", pa.string()),
    ("bronze_s3_uri", pa.string()), ("ingested_at_utc", pa.timestamp("ms")),
    ("analyzed_text", pa.string()), ("text_hash", pa.string()),
    ("language_code", pa.string()), ("analyzed_at_utc", pa.timestamp("ms")),
    ("enrichment_status", pa.string()),
])
COLS = SCHEMA.names  # pub_date NO va en el archivo: es la columna de particion (viene de la ruta)


def exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def naive_utc(dt):
    return dt.replace(tzinfo=None) if dt else None


def load_state():
    """Lee el Silver ya publicado: news_id -> fila."""
    state = {}
    for k in list_keys("silver/news/"):
        m = re.search(r"pub_date=([^/]+)/part-0000\.parquet$", k)
        if not m:
            continue
        for r in pq.read_table(io.BytesIO(get_bytes(k))).to_pylist():
            r["pub_date"] = m.group(1)
            state[r["news_id"]] = r
    return state


def add_seccion(row, sec):
    if sec and sec not in row["secciones"]:
        row["secciones"] = row["secciones"] + [sec]
        return True
    return False


def process_run(run_id, state):
    summary = json.loads(get_bytes(f"control/ingestion_runs/run_id={run_id}.json"))
    c = Counter()
    procesadas, omitidas, rechazos, affected, seen = [], [], {}, set(), set()

    for f in summary.get("fuentes", []):
        sid = f["source_id"]
        if sid.startswith("prueba_"):
            omitidas.append({"source_id": sid, "motivo": "FUENTE_DE_PRUEBA"})
            continue
        if f.get("status") not in VALID:
            omitidas.append({"source_id": sid, "motivo": f"STATUS_{f.get('status')}"})
            continue
        mkey = source_index(sid).get(run_id)
        if not mkey:
            omitidas.append({"source_id": sid, "motivo": "SIN_MANIFEST"})
            continue
        manifest = json.loads(get_bytes(mkey))
        if manifest.get("status") not in VALID or not manifest.get("s3_uri"):
            omitidas.append({"source_id": sid, "motivo": "MANIFEST_NO_VALIDO"})
            continue
        s3_uri = manifest["s3_uri"]
        try:
            root = ET.fromstring(get_bytes(s3_uri.split("/", 3)[3]))
        except ET.ParseError:
            omitidas.append({"source_id": sid, "motivo": "MALFORMED_XML"})
            continue
        procesadas.append(sid)
        sec = manifest.get("seccion")
        medio = manifest.get("medio")
        ingested = naive_utc(parse_date(manifest.get("ingested_at_utc")))

        for it in root.findall("./channel/item"):
            c["recibidas"] += 1
            title = clean_text(it.findtext("title"))
            desc = clean_text(it.findtext("description"))
            link_raw = (it.findtext("link") or "").strip()
            reason = None
            if not title:
                reason = "MISSING_TITLE"
            elif not link_raw:
                reason = "MISSING_LINK"
            elif not link_raw.lower().startswith(("http://", "https://")):
                reason = "INVALID_LINK"
            if reason:
                c["REJECTED"] += 1
                rechazos.setdefault(sid, []).append({
                    "run_id": run_id, "source_id": sid, "reason": reason,
                    "raw_item": {t: it.findtext(t) for t in ("guid", "title", "link", "description", "pubDate")},
                    "bronze_s3_uri": s3_uri,
                    "rejected_at_utc": datetime.now(timezone.utc).isoformat()})
                continue

            nid = sha(normalize_link(link_raw))
            chash = sha(title + "\n" + desc)
            dt = parse_date(it.findtext("pubDate"))
            pub_date = dt.date().isoformat() if dt else "unknown"
            fields = dict(
                guid=(it.findtext("guid") or "").strip() or None, link=link_raw,
                title=title, description=desc,
                pub_date_original=(it.findtext("pubDate") or "").strip() or None,
                published_at_utc=naive_utc(dt), medio=medio, content_hash=chash,
                bronze_run_id=run_id, bronze_s3_uri=s3_uri, ingested_at_utc=ingested,
                analyzed_text=None, text_hash=None, language_code=None,
                analyzed_at_utc=None, enrichment_status="PENDING")

            if nid in seen:  # repetida dentro del mismo lote
                c["DUPLICATE_IN_BATCH"] += 1
                row = state[nid]
                if add_seccion(row, sec):
                    affected.add(row["pub_date"])
                continue
            seen.add(nid)
            row = state.get(nid)

            if row is None:
                state[nid] = dict(fields, news_id=nid, pub_date=pub_date,
                                  seccion_principal=sec, secciones=[sec] if sec else [],
                                  first_seen_run_id=run_id, last_seen_run_id=run_id,
                                  updated_run_id=run_id)
                affected.add(pub_date)
                c["NEW"] += 1
                if pub_date == "unknown":
                    c["sin_fecha_valida"] += 1
            elif row["bronze_run_id"] > run_id:
                c["DUPLICATE"] += 1          # version mas vieja que la vigente: se ignora
                c["version_anterior_ignorada"] += 1
            elif row["content_hash"] != chash:
                old_pd = row["pub_date"]
                row.update(fields, pub_date=pub_date, updated_run_id=run_id)
                add_seccion(row, sec)
                if run_id > row["last_seen_run_id"]:
                    row["last_seen_run_id"] = run_id
                affected.update({old_pd, pub_date})
                c["UPDATED"] += 1
            else:
                c["DUPLICATE"] += 1
                changed = add_seccion(row, sec)
                if run_id > row["last_seen_run_id"]:
                    row["last_seen_run_id"] = run_id
                    changed = True
                if changed:
                    affected.add(row["pub_date"])

    total = sum(c[k] for k in ("NEW", "DUPLICATE", "DUPLICATE_IN_BATCH", "UPDATED", "REJECTED"))
    if not procesadas:
        estado = "SIN_FUENTES_VALIDAS"
    elif any(o["motivo"] != "FUENTE_DE_PRUEBA" for o in omitidas):
        estado = "PARTIAL"
    else:
        estado = "COMPLETE"
    resumen = {
        "run_id": run_id, "estado": estado,
        "fuentes_procesadas": procesadas, "fuentes_omitidas": omitidas,
        "recibidas": c["recibidas"], "nuevas": c["NEW"], "duplicadas": c["DUPLICATE"],
        "duplicadas_en_lote": c["DUPLICATE_IN_BATCH"], "actualizadas": c["UPDATED"],
        "rechazadas": c["REJECTED"], "sin_fecha_valida": c["sin_fecha_valida"],
        "version_anterior_ignorada": c["version_anterior_ignorada"],
        "pendientes_enriquecimiento": c["NEW"] + c["UPDATED"],
        "cuadre_ok": c["recibidas"] == total,
    }
    return resumen, affected, rechazos


def publish(run_id, state, affected, rechazos, simular_fallo):
    stg = f"silver/_staging/run_id={run_id}/"
    plan = []
    # 1) escribir en staging
    for pd_ in sorted(affected):
        rows = sorted((r for r in state.values() if r["pub_date"] == pd_), key=lambda r: r["news_id"])
        final = f"silver/news/pub_date={pd_}/part-0000.parquet"
        if not rows:
            plan.append((None, final))
            continue
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pylist([{k: r[k] for k in COLS} for r in rows], schema=SCHEMA), buf)
        key = f"{stg}pub_date={pd_}/part-0000.parquet"
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
        plan.append((key, final))
    if simular_fallo:
        raise RuntimeError("FALLO SIMULADO despues de escribir staging y antes de publicar")
    # 2) publicar: un objeto por particion, la copia es atomica
    for key, final in plan:
        if key:
            s3.copy_object(Bucket=BUCKET, Key=final, CopySource={"Bucket": BUCKET, "Key": key})
        else:  # particion que quedo vacia (una noticia cambio de fecha)
            s3.delete_object(Bucket=BUCKET, Key=final)
    # 3) cuarentena
    for sid, items in rechazos.items():
        body = "\n".join(json.dumps(x, ensure_ascii=False) for x in items).encode("utf-8")
        s3.put_object(Bucket=BUCKET, Key=f"quarantine/silver/run_id={run_id}/source={sid}/rejected.jsonl", Body=body)
    # 4) limpiar SOLO staging
    assert stg.startswith("silver/_staging/")
    try:
        keys = list_keys(stg)
        for i in range(0, len(keys), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys[i:i + 1000]]})
    except ClientError as e:
        print("  aviso: no pude limpiar staging:", e.response["Error"]["Code"])
    return len(plan)


def write_marker(run_id, resumen, reproceso):
    resumen["terminado_utc"] = datetime.now(timezone.utc).isoformat()
    key = f"control/silver_runs/run_id={run_id}.json"
    if reproceso and exists(key):  # no pisar el resumen original
        key = f"control/silver_runs/reprocess/run_id={run_id}/{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
        resumen["reprocesado"] = True
    s3.put_object(Bucket=BUCKET, Key=key, Body=json.dumps(resumen, ensure_ascii=False, indent=2).encode("utf-8"))
    return key


def main():
    ap = argparse.ArgumentParser(description="Silver paso 2: publicar news")
    ap.add_argument("--run-id", help="procesar/reprocesar este lote de Bronze")
    ap.add_argument("--dry-run", action="store_true", help="no escribe nada en S3")
    ap.add_argument("--simular-fallo", action="store_true", help="falla a proposito antes de publicar (prueba de recuperacion)")
    args = ap.parse_args()
    if not BUCKET:
        sys.exit("Falta la variable DATALAKE_BUCKET")

    runs = list_runs()
    if args.run_id:
        if args.run_id not in runs:
            sys.exit(f"Lote {args.run_id} no existe en Bronze. Validos: {runs}")
        todo = [args.run_id]
    else:
        todo = [r for r in runs if not exists(f"control/silver_runs/run_id={r}.json")]
    if not todo:
        print("No hay lotes pendientes: Silver ya esta al dia.")
        return

    print("DRY-RUN: no se escribe nada." if args.dry_run else "Publicando en S3.")
    print("Lotes a procesar:", todo)
    state = load_state()
    print(f"Silver actual: {len(state)} noticias vigentes")

    for r in todo:
        resumen, affected, rechazos = process_run(r, state)
        print(f"\n=== Lote {r} -> {resumen['estado']} ===")
        print(f"  recibidas={resumen['recibidas']} NEW={resumen['nuevas']} DUPLICATE={resumen['duplicadas']} "
              f"DUPLICATE_IN_BATCH={resumen['duplicadas_en_lote']} UPDATED={resumen['actualizadas']} "
              f"REJECTED={resumen['rechazadas']}  cuadre_ok={resumen['cuadre_ok']}")
        print(f"  particiones afectadas: {sorted(affected)}")
        if args.dry_run:
            continue
        n = publish(r, state, affected, rechazos, args.simular_fallo)
        resumen["particiones_escritas"] = n
        key = write_marker(r, resumen, bool(args.run_id))
        print(f"  publicadas {n} particiones. Resumen en s3://{BUCKET}/{key}")
    print(f"\nNoticias vigentes al final: {len(state)}")


if __name__ == "__main__":
    main()
