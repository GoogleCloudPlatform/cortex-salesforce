# Copyright 2022 Google LLC

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Disable pylance / pylint as errors
# type: ignore

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

default_args = {
   "depends_on_past": False,
   "start_date": datetime(${year}, ${month}, ${day}),
   "catchup": False,
   "retries": 1,
   "retry_delay": timedelta(minutes=10),
}

with DAG(
        dag_id="SFDC_RAW_TO_CDC_${base_table}",
        template_searchpath=["/home/airflow/gcs/data/bq_data_replication"],
        description=(
            "Merge from Salesforce RAW BQ dataset to CDC BQ dataset for "
            "'${base_table}' table"),
        default_args=default_args,
        schedule_interval="${load_frequency}",
        catchup = False,
        max_active_runs=1
) as dag:
    start_task = DummyOperator(task_id="start")
    copy_raw_to_cdc = BigQueryOperator(
        task_id="copy_raw_to_cdc_${base_table}",
        sql="sfdc_raw_to_cdc_${base_table}.sql",
        # TODO:Airflow 1.x uses bigquery_conn_id, Airflow 2.x uses gcp_conn_id.
        # We need to figure out how to support both.
        bigquery_conn_id="sfdc_cdc_bq",
        use_legacy_sql=False)
    stop_task = DummyOperator(task_id="stop")

start_task >> copy_raw_to_cdc >> stop_task # pylint: disable=pointless-statement
