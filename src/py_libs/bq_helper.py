# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Library for BigQuery related functions."""

import logging

from google.cloud.exceptions import NotFound
from google.cloud import bigquery

client = bigquery.Client()


def execute_sql_file(sql_file, log_sql=False):
    """Executes a Bigquery sql file."""
    with open(sql_file, mode="r", encoding="utf-8") as sqlf:
        sql_str = sqlf.read()
        if log_sql:
            logging.info("Executing SQL: %s", sql_str)
        query_job = client.query(sql_str)
        # Let's wait for query to complete.
        _ = query_job.result()


def table_exists(full_table_name) -> bool:
    """Checks if a BigQuery table exists."""
    try:
        _ = client.get_table(full_table_name)
        return True
    except NotFound:
        return False


def create_table(full_name, schema_tuples_list, exists_ok=False):
    """Create a BigQuery table"""
    project, dataset_id, table_id = full_name.split(".")
    table_ref = bigquery.TableReference(
        bigquery.DatasetReference(project, dataset_id), table_id)
    table = bigquery.Table(
        table_ref,
        schema=[bigquery.SchemaField(t[0], t[1]) for t in schema_tuples_list])
    client.create_table(table, exists_ok=exists_ok)
