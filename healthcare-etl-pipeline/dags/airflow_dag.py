import logging
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task

import who_data_extractor as extractor
import data_transformation as transformer
import data_loader as loader


logger = logging.getLogger(__name__)


default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    dag_id="healthcare_gho_etl",
    default_args=default_args,
    schedule="@monthly",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    description="Monthly ETL for WHO GHO healthcare indicators",
    tags=["healthcare", "etl", "gho"],
)
def healthcare_gho_etl():
    @task
    def extract():
        logger.info("Starting extraction step for WHO healthcare GHO ETL pipeline")
        raw_data = extractor.run_extraction()
        try:
            record_count = len(raw_data)
        except TypeError:
            record_count = "unknown"
        logger.info("Extraction completed, records fetched: %s", record_count)
        return raw_data

    @task
    def transform(raw_data):
        logger.info("Starting transformation step")
        clean_data, metrics_data = transformer.run_transformations(raw_data)
        try:
            clean_count = len(clean_data)
        except TypeError:
            clean_count = "unknown"
        try:
            metrics_count = len(metrics_data)
        except TypeError:
            metrics_count = "unknown"
        logger.info(
            "Transformation completed, clean: %s, metrics: %s",
            clean_count,
            metrics_count,
        )
        return {"clean_data": clean_data, "metrics_data": metrics_data}

    @task
    def load(clean_data, metrics_data):
        logger.info("Starting load step")
        loader.load_data(clean_data, "fact_healthcare_indicators")
        loader.load_data(metrics_data, "summary_regional_metrics")
        logger.info("Load step completed successfully")

    raw = extract()
    transformed = transform(raw)
    load(
        clean_data=transformed["clean_data"],
        metrics_data=transformed["metrics_data"],
    )


dag = healthcare_gho_etl()