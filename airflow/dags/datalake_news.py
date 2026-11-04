"""DAG datalake_news: orquestacion del Data Lake de noticias RSS (PDF 4.6).

Orden real: publicar Silver ANTES de enriquecer (el PDF lista al reves).
Cada tarea llama a los scripts de src/ y registra logs con fuente, lote y tarea.
"""
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import timedelta

import boto3
import pendulum
import yaml
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models.param import Param

log = logging.getLogger("airflow.task")

REPO = os.environ.get("DATALAKE_REPO", "/opt/airflow/repo")
BUCKET = os.environ.get("DATALAKE_BUCKET", "data-lake-sct")
GLUE_DB = "datalake_news"
WORKGROUP = "datalake-wg"
POOL = "escritura_datalake"          # pool de tamano 1: una escritura a la vez
RUN_ID_RE = re.compile(r"^\d{8}T\d{6}Z$")
# Columna de Gold con el conteo de noticias (ajustar si se llama distinto)
GOLD_COUNT_COL = os.environ.get("GOLD_COUNT_COL", "news_count")

REQUERIDAS = {
    "silver_news": ["news_id"],
    "silver_news_entities": ["news_id"],
    "silver_news_key_phrases": ["news_id"],
    "gold_news_daily": [GOLD_COUNT_COL],
}
HUERFANAS = (
    "SELECT count(*) FROM {t} e LEFT JOIN silver_news n "
    "ON e.news_id = n.news_id WHERE n.news_id IS NULL"
)
CHECKS = {
    "silver_total": "SELECT count(*) FROM silver_news",
    "duplicados_news_id": "SELECT count(*) - count(DISTINCT news_id) FROM silver_news",
    "entidades_huerfanas": HUERFANAS.format(t="silver_news_entities"),
    "frases_huerfanas": HUERFANAS.format(t="silver_news_key_phrases"),
    "gold_total": f"SELECT coalesce(sum({GOLD_COUNT_COL}), 0) FROM gold_news_daily",
}


# ---------------------------------------------------------------- utilidades
def _tag(ctx, lote="-", fuente="todas"):
    ti = ctx["ti"]
    return f"[fuente={fuente} lote={lote} tarea={ti.task_id} intento={ti.try_number}]"


def _ejecutar(script, args, tag):
    """Corre un script de src/ y vuelca su salida al log de Airflow."""
    cmd = [sys.executable, f"src/{script}", *args]
    log.info("%s Ejecutando: %s", tag, " ".join(cmd))
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    p = subprocess.Popen(cmd, cwd=REPO, env=env, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT, text=True, bufsize=1)
    for linea in p.stdout:
        log.info("%s %s", tag, linea.rstrip())
    rc = p.wait()
    if rc != 0:
        raise AirflowException(f"{tag} {script} termino con codigo {rc}")


def _lotes():
    s3 = boto3.client("s3")
    ids = []
    pag = s3.get_paginator("list_objects_v2")
    for pg in pag.paginate(Bucket=BUCKET, Prefix="control/ingestion_runs/"):
        for o in pg.get("Contents", []):
            m = re.search(r"run_id=(\d{8}T\d{6}Z)\.json$", o["Key"])
            if m:
                ids.append(m.group(1))
    return sorted(ids)


def _resumen(run_id):
    obj = boto3.client("s3").get_object(
        Bucket=BUCKET, Key=f"control/ingestion_runs/run_id={run_id}.json")
    return json.loads(obj["Body"].read())


def _athena(sql):
    a = boto3.client("athena")
    qid = a.start_query_execution(
        QueryString=sql, QueryExecutionContext={"Database": GLUE_DB},
        WorkGroup=WORKGROUP)["QueryExecutionId"]
    while True:
        st = a.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if st["State"] != "SUCCEEDED":
        raise AirflowException(f"Athena {st['State']}: {st.get('StateChangeReason')} | {sql}")
    filas = a.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    return int(float(filas[1]["Data"][0]["VarCharValue"]))


# -------------------------------------------------------------------- DAG
@dag(
    dag_id="datalake_news",
    description="Data Lake de noticias RSS: Bronze -> Silver -> Gold -> Athena",
    schedule="0 */6 * * *",                       # cada 6 horas
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,                            # una ejecucion a la vez
    is_paused_upon_creation=True,                 # no corre solo hasta activarlo
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "owner": "de-gr01-user-4",
        "retries": 2,                             # reintentos acotados
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "run_id_reproceso": Param(
            "", type="string",
            description="Vacio = ingesta normal. Con un run_id de Bronze "
                        "(ej. 20261101T182352Z) reprocesa ese lote sin consultar los RSS."),
    },
    tags=["datalake", "noticias"],
)
def datalake_news():

    # 1. Validacion de configuracion y acceso a servicios
    @task
    def validar_config_accesos(**ctx):
        tag = _tag(ctx)
        with open(os.path.join(REPO, "config", "sources.yaml")) as f:
            cfg = yaml.safe_load(f)
        fuentes = cfg
        if isinstance(cfg, dict):
            fuentes = cfg.get("sources") or cfg.get("fuentes") or cfg.get("feeds")
        if not fuentes:
            raise AirflowFailException(f"{tag} config/sources.yaml sin fuentes")
        items = fuentes.items() if isinstance(fuentes, dict) else fuentes
        for f_ in items:
            log.info("%s fuente configurada: %s", tag, f_)
        for s in ("ingest_bronze", "silver_classify", "silver_publish",
                  "silver_enrich", "gold_build", "athena_run"):
            if not os.path.exists(os.path.join(REPO, "src", f"{s}.py")):
                raise AirflowFailException(f"{tag} falta src/{s}.py")
        log.info("%s identidad AWS: %s", tag, boto3.client("sts").get_caller_identity()["Arn"])
        checks = [
            ("S3", lambda: boto3.client("s3").head_bucket(Bucket=BUCKET)),
            ("Glue", lambda: boto3.client("glue").get_database(Name=GLUE_DB)),
            ("Athena", lambda: boto3.client("athena").get_work_group(WorkGroup=WORKGROUP)),
        ]
        for nombre, fn in checks:
            try:
                fn()
                log.info("%s acceso OK: %s", tag, nombre)
            except Exception as e:
                raise AirflowException(f"{tag} acceso a {nombre} fallo: {e}")
        log.info("%s Comprehend se valida al enriquecer (evita costo extra)", tag)

    # 2. Extraccion de feeds y escritura en Bronze (o reproceso desde Bronze)
    @task(pool=POOL)
    def ingesta_bronze(**ctx):
        ti, dr = ctx["ti"], ctx["dag_run"]
        repro = (ctx["params"].get("run_id_reproceso") or "").strip()
        if repro:
            if not RUN_ID_RE.match(repro):
                raise AirflowFailException(f"run_id_reproceso invalido: {repro}")
            if repro not in _lotes():
                raise AirflowFailException(f"el lote {repro} no existe en Bronze")
            lote = repro
            log.info("%s REPROCESO desde Bronze: no se consultan los RSS",
                     _tag(ctx, lote))
        else:
            previos = set(_lotes())
            args = []
            if ti.try_number > 1:   # reintento: retomar el lote FALLIDO de esta corrida
                desde = pendulum.instance(dr.start_date).in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ")
                fallidos = [r for r in sorted(previos) if r >= desde
                            and str(_resumen(r).get("estado", "")).upper() == "FAILED"]
                if fallidos:
                    args = ["--run-id", fallidos[-1]]
            _ejecutar("ingest_bronze.py", args, _tag(ctx))
            if args:
                lote = args[1]
            else:
                nuevos = sorted(set(_lotes()) - previos)
                if not nuevos:
                    raise AirflowException("ingest_bronze no registro ningun lote en control/")
                lote = nuevos[-1]

        tag = _tag(ctx, lote)
        resumen = _resumen(lote)
        estado = str(resumen.get("estado", "?")).upper()   # COMPLETE / PARTIAL / FAILED
        log.info("%s estado de la ingesta: %s", tag, estado)
        fuentes = resumen.get("fuentes") or resumen.get("sources") or {}
        for k, v in (fuentes.items() if isinstance(fuentes, dict) else enumerate(fuentes)):
            log.info("[fuente=%s lote=%s tarea=%s] %s", k, lote, ti.task_id, v)
        log.info("%s resumen: %s", tag, json.dumps(resumen, ensure_ascii=False, default=str)[:3000])
        ti.xcom_push(key="estado_ingesta", value=estado)
        if estado == "FAILED":
            raise AirflowException(f"{tag} ingesta FALLIDA: una fuente caida no es 'sin noticias'")
        if estado == "PARTIAL":
            log.warning("%s ingesta PARCIAL: hay fuentes con error; se continua y queda registrado", tag)
        return lote

    # 3. Limpieza, validacion y deduplicacion (clasificacion en seco, solo lectura)
    @task
    def limpieza_dedup_silver(lote, **ctx):
        _ejecutar("silver_classify.py", [], _tag(ctx, lote))

    # 4. Publicacion de Silver
    @task(pool=POOL)
    def publicar_silver(lote, **ctx):
        _ejecutar("silver_publish.py", ["--run-id", lote], _tag(ctx, lote))

    # 5. Enriquecimiento con Comprehend (solo noticias nuevas o modificadas)
    @task(pool=POOL)
    def enriquecer_comprehend(lote, **ctx):
        _ejecutar("silver_enrich.py", ["--yes"], _tag(ctx, lote))

    # 6. Construccion de Gold
    @task(pool=POOL)
    def construir_gold(lote, **ctx):
        _ejecutar("gold_build.py", [], _tag(ctx, lote))

    # 7. Actualizacion de particiones en Glue Data Catalog
    @task
    def actualizar_glue(lote, **ctx):
        _ejecutar("athena_run.py", ["--repair"], _tag(ctx, lote))

    # 8. Validaciones finales con Athena
    @task
    def validaciones_athena(lote, **ctx):
        tag = _tag(ctx, lote)
        glue = boto3.client("glue")
        for tabla, cols in REQUERIDAS.items():
            t = glue.get_table(DatabaseName=GLUE_DB, Name=tabla)["Table"]
            existentes = [c["Name"] for c in t["StorageDescriptor"]["Columns"]]
            faltan = [c for c in cols if c not in existentes]
            if faltan:
                raise AirflowFailException(
                    f"{tag} {tabla} no tiene {faltan}. Columnas reales: {existentes}")
        r = {}
        for nombre, sql in CHECKS.items():
            r[nombre] = _athena(sql)
            log.info("%s %s = %s", tag, nombre, r[nombre])
        errores = []
        if r["duplicados_news_id"] != 0:
            errores.append("news_id duplicados en Silver")
        if r["entidades_huerfanas"] != 0 or r["frases_huerfanas"] != 0:
            errores.append("entidades o frases sin noticia asociada")
        if r["silver_total"] != r["gold_total"]:
            errores.append(f"Silver ({r['silver_total']}) no concilia con Gold ({r['gold_total']})")
        if errores:
            raise AirflowFailException(f"{tag} validaciones fallidas: {errores}")
        log.info("%s todas las validaciones OK", tag)
        return r

    # 9. Registro del estado (corre siempre, aunque algo haya fallado)
    @task(trigger_rule="all_done", retries=0)
    def registrar_estado(**ctx):
        ti, dr = ctx["ti"], ctx["dag_run"]
        tareas = {t.task_id: t.state for t in dr.get_task_instances()
                  if t.task_id != ti.task_id}
        lote = ti.xcom_pull(task_ids="ingesta_bronze")
        estado_ing = ti.xcom_pull(task_ids="ingesta_bronze", key="estado_ingesta")
        valid = ti.xcom_pull(task_ids="validaciones_athena")
        fallidas = [k for k, v in tareas.items() if v in ("failed", "upstream_failed")]
        if fallidas:
            resultado = "FALLIDA"
        elif estado_ing == "PARTIAL":
            resultado = "PARCIAL"
        else:
            resultado = "COMPLETA"
        registro = {
            "dag_id": dr.dag_id,
            "dag_run_id": dr.run_id,
            "run_type": str(dr.run_type),
            "logical_date": str(ctx.get("logical_date")),
            "lote": lote,
            "reproceso": bool((ctx["params"].get("run_id_reproceso") or "").strip()),
            "estado_ingesta": estado_ing,
            "resultado": resultado,
            "tareas": tareas,
            "validaciones": valid,
            "registrado_utc": pendulum.now("UTC").to_iso8601_string(),
        }
        seguro = re.sub(r"[^A-Za-z0-9_.=-]", "_", dr.run_id)
        key = f"control/dag_runs/dag_run_id={seguro}.json"
        boto3.client("s3").put_object(
            Bucket=BUCKET, Key=key,
            Body=json.dumps(registro, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json")
        tag = _tag(ctx, lote or "-")
        log.info("%s resultado=%s registrado en s3://%s/%s", tag, resultado, BUCKET, key)
        if fallidas:
            raise AirflowFailException(f"{tag} tareas fallidas: {fallidas}")

    t1 = validar_config_accesos()
    lote = ingesta_bronze()
    t3 = limpieza_dedup_silver(lote)
    t4 = publicar_silver(lote)
    t5 = enriquecer_comprehend(lote)
    t6 = construir_gold(lote)
    t7 = actualizar_glue(lote)
    t8 = validaciones_athena(lote)
    t9 = registrar_estado()

    t1 >> lote >> t3 >> t4 >> t5 >> t6 >> t7 >> t8 >> t9


datalake_news()
