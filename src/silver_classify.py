#!/usr/bin/env python3
"""Silver, paso 1: lee Bronze y clasifica noticias. SOLO LECTURA (no escribe en S3)."""
import argparse, hashlib, html, json, os, re, sys
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import boto3

BUCKET = os.environ.get("DATALAKE_BUCKET")
RUN_RE = re.compile(r"^\d{8}T\d{6}Z$")
TAG_RE = re.compile(r"<[^>]+>")
VALID = {"OK", "EMPTY"}
s3 = boto3.client("s3", region_name="us-east-1")
_index_cache = {}


def sha(texto):
    return hashlib.sha256(texto.encode("utf-8")).hexdigest()


def list_keys(prefix):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def get_bytes(key):
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def list_runs():
    runs = []
    for k in list_keys("control/ingestion_runs/"):
        m = re.search(r"run_id=(.+)\.json$", k)
        if m and RUN_RE.match(m.group(1)):
            runs.append(m.group(1))
    return sorted(runs)


def source_index(sid):
    """run_id -> key del manifest.json de esa fuente."""
    if sid not in _index_cache:
        idx = {}
        for k in list_keys(f"bronze/source={sid}/"):
            m = re.search(r"run_id=([^/]+)/manifest\.json$", k)
            if m:
                idx[m.group(1)] = k
        _index_cache[sid] = idx
    return _index_cache[sid]


def clean_text(t):
    t = TAG_RE.sub(" ", t or "")
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def normalize_link(url):
    p = urlsplit(url.strip())
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
         if not k.lower().startswith("utm_") and k.lower() != "fbclid"]
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path.rstrip("/"), urlencode(q), ""))


def parse_date(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        try:
            dt = parsedate_to_datetime(s)
        except (TypeError, ValueError):
            return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


def process_run(run_id, state, detalle):
    summary = json.loads(get_bytes(f"control/ingestion_runs/run_id={run_id}.json"))
    print(f"\n=== Lote {run_id} (estado de ingesta: {summary.get('estado')}) ===")
    c = Counter()
    reasons = Counter()
    omitidas = []
    seen = {}  # news_id -> set de fuentes dentro de este lote

    for f in summary.get("fuentes", []):
        sid = f["source_id"]
        if sid.startswith("prueba_"):
            omitidas.append((sid, "FUENTE_DE_PRUEBA"))
            continue
        if f.get("status") not in VALID:
            omitidas.append((sid, f"STATUS_{f.get('status')}"))
            continue
        mkey = source_index(sid).get(run_id)
        if not mkey:
            omitidas.append((sid, "SIN_MANIFEST"))
            continue
        manifest = json.loads(get_bytes(mkey))
        if manifest.get("status") not in VALID or not manifest.get("s3_uri"):
            omitidas.append((sid, "MANIFEST_NO_VALIDO"))
            continue
        feed_key = manifest["s3_uri"].split("/", 3)[3]
        try:
            root = ET.fromstring(get_bytes(feed_key))
        except ET.ParseError:
            omitidas.append((sid, "MALFORMED_XML"))
            continue

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
                reasons[reason] += 1
                label = f"REJECTED ({reason})"
            else:
                nid = sha(normalize_link(link_raw))
                chash = sha(title + "\n" + desc)
                dt = parse_date(it.findtext("pubDate"))
                if dt is None:
                    c["sin_fecha_valida"] += 1
                if nid in seen:
                    label = "DUPLICATE_IN_BATCH"
                    seen[nid].add(sid)
                else:
                    seen[nid] = {sid}
                    if nid not in state:
                        label = "NEW"
                    elif state[nid] != chash:
                        label = "UPDATED"
                    else:
                        label = "DUPLICATE"
                    state[nid] = chash
                c[label] += 1
            if detalle:
                fecha = dt.date().isoformat() if not reason and dt else "unknown"
                print(f"  {label:<20} {sid:<22} {fecha}  {title[:60]}")

    for sid, motivo in omitidas:
        print(f"  fuente omitida: {sid} -> {motivo}")
    total = sum(c[k] for k in ("NEW", "DUPLICATE", "DUPLICATE_IN_BATCH", "UPDATED", "REJECTED"))
    print(f"  recibidas={c['recibidas']}  NEW={c['NEW']}  DUPLICATE={c['DUPLICATE']}  "
          f"DUPLICATE_IN_BATCH={c['DUPLICATE_IN_BATCH']}  UPDATED={c['UPDATED']}  REJECTED={c['REJECTED']}")
    print(f"  sin fecha valida (no se rechazan)={c['sin_fecha_valida']}  "
          f"noticias en mas de un feed={sum(1 for v in seen.values() if len(v) > 1)}")
    print(f"  control: recibidas == suma de estados -> {c['recibidas'] == total}")


def main():
    ap = argparse.ArgumentParser(description="Ensayo en seco de Silver (solo lectura)")
    ap.add_argument("--detalle", action="store_true", help="imprime cada noticia con su estado")
    args = ap.parse_args()
    if not BUCKET:
        sys.exit("Falta la variable DATALAKE_BUCKET")
    runs = list_runs()
    print("SOLO LECTURA: este script no escribe nada en S3.")
    print("Lotes validos encontrados:", runs)
    state = {}  # Silver simulado en memoria: news_id -> content_hash
    for r in runs:
        process_run(r, state, args.detalle)
    print(f"\nNoticias unicas vigentes (Silver simulado): {len(state)}")


if __name__ == "__main__":
    main()
