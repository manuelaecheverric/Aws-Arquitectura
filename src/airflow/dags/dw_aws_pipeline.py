"""Pipeline de punta a punta en AWS (requisito 3.4).

validar -> registrar inicio -> extraer (RDS/DocumentDB -> S3) -> cargar (COPY a "raw")
-> dbt seed -> intermediate -> quarantine -> snapshot (SCD2) -> marts -> pruebas
-> registrar resultado (siempre).

Parámetros al disparar el DAG ("Trigger DAG w/ config"):
  batch_id  : -1 = siguiente lote libre (0 si no hay ninguno). Un número fija el lote.
  mode      : initial | incremental (se ignora si batch_id = -1).
  reprocess : true = no consulta las fuentes; recarga el lote desde S3 (recuperación).
  fail_task : provoca un fallo controlado en esa tarea (demostración de recuperación).
"""

from datetime import datetime, timedelta, timezone

from airflow.decorators import task
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG
from ecommerce_dw.aws import pipeline_tasks as pt

DEFAULT_ARGS = {
    "retries": 2,                                  # reintentos acotados
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_aws_pipeline",
    description="RDS/DocumentDB -> S3 -> Redshift (COPY) -> dbt -> pruebas, con auditoría por lote",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,  # un solo lote a la vez: evita escrituras concurrentes en raw y marts
    default_args=DEFAULT_ARGS,
    params={
        "batch_id": Param(-1, type="integer", minimum=-1),
        "mode": Param("incremental", enum=["initial", "incremental"]),
        "reprocess": Param(False, type="boolean"),
        # Escenario de fallo controlado: "load_to_redshift", "dbt_run" o "dbt_test". Vacío = normal.
        "fail_task": Param("", type="string"),
    },
    tags=["aws", "taller-dw"],
) as dag:

    @task(retries=0)
    def resolve_batch(**context):
        return pt.resolve_batch(context["params"], context["run_id"])

    @task
    def validate(batch):
        pt.validate_connections(batch)
        return batch

    @task
    def register_start(batch, **context):
        pt.register_start(batch, context["run_id"])
        return batch

    @task
    def extract_to_s3(batch, **context):
        pt.extract(batch, context["run_id"])
        return batch

    @task
    def load_to_redshift(batch, **context):
        pt.load(batch, context["run_id"])
        return batch

    def dbt_step(name, command, selector=None):
        @task(task_id=name)
        def _run(batch):
            pt.run_dbt(batch, command, selector)
            return batch
        return _run

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0)
    def register_result(batch, **context):
        dag_run = context["dag_run"]
        states = {ti.task_id: ti.state for ti in dag_run.get_task_instances()
                  if ti.task_id != "register_result"}
        pt.register_result(batch, context["run_id"], states)

    batch = resolve_batch()
    ready = register_start(validate(batch))
    loaded = load_to_redshift(extract_to_s3(ready))
    seeded = dbt_step("dbt_seed", "seed")(loaded)
    intermediate = dbt_step("dbt_run_intermediate", "run", "path:models/intermediate")(seeded)
    quarantine = dbt_step("dbt_run_quarantine", "run", "path:models/quarantine")(intermediate)
    snapshots = dbt_step("dbt_snapshot", "snapshot")(quarantine)
    marts = dbt_step("dbt_run_marts", "run", "path:models/marts")(snapshots)
    tested = dbt_step("dbt_test", "test")(marts)
    register_result(batch).set_upstream(tested)
