"""Runner de Athena: carga particiones (MSCK REPAIR) y ejecuta consultas.

Uso:
  python src/athena_run.py --repair
  python src/athena_run.py --sql "SELECT count(*) FROM silver_news"
  python src/athena_run.py --file sql/consultas_athena.sql
"""
import argparse
import sys
import time

import boto3

REGION = "us-east-1"
DB = "datalake_news"
WG = "datalake-wg"
TABLES = [
    "silver_news", "silver_news_entities", "silver_news_key_phrases",
    "gold_news_daily", "gold_entity_daily", "gold_key_phrase_section_daily",
]
athena = boto3.client("athena", region_name=REGION)


def run(sql):
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB},
        WorkGroup=WG,
    )["QueryExecutionId"]
    while True:
        q = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)
    if state != "SUCCEEDED":
        sys.exit(f"{state}: {q['Status'].get('StateChangeReason', '')}")
    return qid, q["Statistics"]


def show(qid, stats, max_rows=30):
    pages = athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid)
    rows = []
    for page in pages:
        for r in page["ResultSet"]["Rows"]:
            rows.append([c.get("VarCharValue", "") for c in r["Data"]])
    for r in rows[:max_rows + 1]:
        print(" | ".join(r))
    if len(rows) > max_rows + 1:
        print(f"... ({len(rows) - 1} filas en total)")
    print(f"[bytes_escaneados={stats.get('DataScannedInBytes', 0)} "
          f"tiempo_ms={stats.get('TotalExecutionTimeInMillis', 0)} query_id={qid}]\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--repair", action="store_true", help="MSCK REPAIR de las 6 tablas")
    ap.add_argument("--sql", help="una consulta SQL")
    ap.add_argument("--file", help="archivo .sql con consultas separadas por ;")
    a = ap.parse_args()
    if not (a.repair or a.sql or a.file):
        ap.error("indica --repair, --sql o --file")

    if a.repair:
        for t in TABLES:
            qid, st = run(f"MSCK REPAIR TABLE {t}")
            print(f"{t}: particiones cargadas (bytes={st.get('DataScannedInBytes', 0)})")
    queries = []
    if a.sql:
        queries.append(a.sql)
    if a.file:
        with open(a.file, encoding="utf-8") as f:
            queries += [q.strip() for q in f.read().split(";") if q.strip()]
    for q in queries:
        print(f">>> {q}\n")
        qid, st = run(q)
        show(qid, st)


if __name__ == "__main__":
    main()