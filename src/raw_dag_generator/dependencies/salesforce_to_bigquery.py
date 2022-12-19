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
""" This module provides SFDC -> BigQuery extraction code / logic """

import csv
from datetime import datetime, timezone, timedelta
import json
import logging
import typing
import tempfile
import time
from pathlib import Path

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.salesforce.hooks.salesforce import SalesforceHook
from simple_salesforce import Salesforce
from simple_salesforce.util import exception_handler

from google.cloud.exceptions import NotFound
from google.cloud import bigquery


class SalesforceToBigquery:
    """Class that handles extracting SFDC data to BigQuery"""

    _MAX_RECORDS_PER_BULK_BATCH_ = 100000
    _MAX_LINES_IN_CVS_WRITE_BATCH_ = 512
    _RECORD_STAMP_NAME_ = "Recordstamp"
    _OPERATIONAL_FLAG_NAME_ = "OperationalFlag"

    @staticmethod
    def extract_data_from_sfdc(
        sfdc_connection_id: str,
        api_name: str,
        bq_connection_id: str,
        project_id: str,
        dataset_name: str,
        output_table_name: str,
        schema_file_path: str,
    ) -> None:
        """Method to extract data from salesforce to BQ

        Args:
            sfdc_connection_id (str): Airflow Salesforce connection id
            api_name (str): Salesforce object name to replicate
            bq_connection_id (str): Airflow BigQuery connection name
            project_id (str): destination GCP project id
            dataset_name (str): destination dataset name
            output_table_name (str): destination table
            schema_file_path (str): mapping schema CVS file path
        """

        logging.info("Preparing Salesforce to BigQuery Replication for %s...",
                     api_name)
        start_time = time.time()

        if bq_connection_id:
            bq_hook = BigQueryHook(bq_connection_id)
            client = bq_hook.get_client()
        else:
            client = bigquery.Client()

        # Salesforce hook made with a connection or a secret
        sfdc_connection = SalesforceHook(sfdc_connection_id)
        # Simple-Salesforce connection
        simple_sf_connection: Salesforce = sfdc_connection.get_conn()

        # Recordstamp is the start date/time of replication job.
        # Preliminary Recordstamp is used for initializing
        # new BigQuery rows' Recordstamp until
        # the entire replication job is done.
        # After that, all preliminary values are replaced by "real" recordstamp.
        # Preliminary Recordstamp is calculated from the current UTC datetime
        # by subtracting 365*500 days (roughly 500 years).
        recordstamp = datetime.now(timezone.utc)
        preliminary_recordstamp = recordstamp - timedelta(days=365 * 500)

        # Load column config from CSV schema file
        logging.info("Reading schema from %s", schema_file_path)

        # SFDC-to-BQ schema
        sfdc_to_bq_field_map = {}
        # Field list for SELECT query
        source_fields = []

        with open(
                schema_file_path,
                encoding="utf-8",
                newline="",
        ) as csv_file:
            for row in csv.DictReader(csv_file, delimiter=","):
                source_fields.append(row["SourceField"])
                sfdc_to_bq_field_map[row["SourceField"]] = (row["TargetField"],
                                                            row["DataType"])

        # ensure SystemModStamp field is selected even if not in the field map
        if "systemmodstamp" not in set(k.lower() for k in source_fields):
            sfdc_to_bq_field_map["SystemModStamp"] = ("SystemModstamp",
                                                      "TIMESTAMP")
            source_fields.append("SystemModStamp")

        # Destination table
        table = bigquery.TableReference(
            bigquery.DatasetReference(project_id, dataset_name),
            output_table_name,
        )

        # When was our last replication for this table?
        last_load_timestamp = SalesforceToBigquery._find_last_load_datetime_str(
            client, project_id, dataset_name, output_table_name)
        if last_load_timestamp is None:
            logging.info("Running Full Replication on %s.", api_name)
            ops = ["I"]
        else:
            logging.info("Running Incremental Replication on %s.", api_name)
            if api_name.lower() in ["user", "recordtype"]:
                # Objects without IsDeleted field.
                ops = ["I", "U"]
            else:
                ops = ["I", "U", "D"]

        # Running replication jobs.
        logging.info("%i replication job(s) to run for %s", len(ops), api_name)
        job_counter = 0

        try:
            for op in ops:
                job_start_time = time.time()
                include_deleted = (last_load_timestamp is not None and
                                   op == "D")

                extra_fields = {
                    SalesforceToBigquery._RECORD_STAMP_NAME_:
                        (preliminary_recordstamp.strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"), "TIMESTAMP"),
                    SalesforceToBigquery._OPERATIONAL_FLAG_NAME_:
                        (op, "STRING"),
                }

                query = SalesforceToBigquery._create_sfdc_query(
                    api_name, ",".join(source_fields), last_load_timestamp, op)
                job_counter += 1

                logging.info(
                    "Initializing SFDC Bulk API job (%i of %i) for %s with"
                    " query: %s",
                    job_counter,
                    len(ops),
                    api_name,
                    query,
                )

                job_id = SalesforceToBigquery._bulk_start_job(
                    simple_sf_connection, query, include_deleted)

                logging.info(
                    "Running SFDC job and loading results to BigQuery.")

                # Starting a Bulk API 2.0 job.
                batches = SalesforceToBigquery._bulk_get_records(
                    simple_sf_connection, job_id)

                SalesforceToBigquery._upload_batches_to_bq(
                    client, batches, table, sfdc_to_bq_field_map, extra_fields)

                job_end_time = time.time()
                logging.info(
                    "Job has been completed in %f seconds.",
                    job_end_time - job_start_time,
                )

                # Deleting SFDC job.
                # We can only do it now because
                # _upload_batches_to_bq dynamically retrieves results
                # from the generator returned by _bulk_get_records
                SalesforceToBigquery._bulk_delete_job(simple_sf_connection,
                                                      job_id)

            # Updating Recordstamp of new records to the actual value.
            # This is our way for "committing the transaction" for now.
            logging.info("Finalizing BigQuery records.")
            SalesforceToBigquery._bigquery_commit(client, table,
                                                  preliminary_recordstamp,
                                                  recordstamp)
        except Exception:
            logging.error(
                "⛔️ Failed to run Salesforce to BigQuery Replication\n",
                exc_info=True,
            )
            raise

        end_time = time.time()
        logging.info(
            "Salesforce to BigQuery Replication jobs have been completed in"
            " %f seconds",
            end_time - start_time,
        )

    @staticmethod
    def _bulk_start_job(sfdc_connection: Salesforce,
                        query: str,
                        include_deleted: bool = False) -> str:
        """Starts Salesforce Bulk API 2.0 query job.

        Args:
            sfdc_connection (Salesforce): Salesforce connection
            query (str): Salesforce query for Bulk API 2.0
            include_deleted (bool, optional): Whether to include
                deleted records. Defaults to False.

        Returns:
            str: Job Id
        """
        operation = "queryAll" if include_deleted else "query"
        request_body = {
            "operation": operation,
            "query": query,
            "contentType": "CSV",
            "columnDelimiter": "COMMA",
            "lineEnding": "LF",
        }

        # Start a job
        job = sfdc_connection.restful(path="jobs/query",
                                      method="POST",
                                      data=json.dumps(request_body))
        return job["id"]

    @staticmethod
    def _bulk_get_records(
        sfdc_connection: Salesforce,
        job_id: str,
        job_status_interval: float = 10.0,
    ) -> typing.Iterable[typing.Iterable[str]]:
        """Retrieves CSV content of Salesforce Build API 2.0 query results
            as batches of CVS lines.

        Args:
            sfdc_connection (Salesforce): Salesforce connection
            job_id (str): Salesforce Bulk API 2.0 job to retrieve results from
            include_deleted (bool, optional): Whether to include
                deleted records. Defaults to False.
            job_status_interval (float, optional): Job status polling interval
                in seconds. Defaults to 10.0.

        Raises:
            RuntimeError: Job failed.

        Yields:
            Iterator[typing.Iterable[typing.Iterable[str]]]:
                iterable batches of iterables with result CSV lines.
        """

        # Checking for job status every job_status_interval seconds.
        job_status_path = f"jobs/query/{job_id}"
        job_running = True
        while job_running:
            time.sleep(job_status_interval)
            status = sfdc_connection.restful(path=job_status_path, method="GET")
            state = status["state"]
            if state in ["Failed", "Aborted"]:
                raise RuntimeError(f"Operation {job_id} {state}")
            elif state == "JobComplete":
                break

        locator = None

        # Retrieve job results.
        while locator != "null":
            headers = sfdc_connection.headers.copy()
            result_path = f"jobs/query/{job_id}/results?maxRecords={SalesforceToBigquery._MAX_RECORDS_PER_BULK_BATCH_}"
            if locator:
                result_path += f"&locator={locator}"
            with sfdc_connection.session.request(
                    "GET",
                    f"{sfdc_connection.base_url}{result_path}",
                    headers=headers,
                    stream=True,
            ) as result_response:
                if result_response.status_code == 401:
                    # Auth token might have expired,
                    # Let simple-salesforce renew it
                    # by performing a restful call on the job status
                    sfdc_connection.restful(path=job_status_path, method="GET")
                    continue
                elif result_response.status_code >= 300:
                    # Error codes >= 300 mean an error,
                    # except when it's 404 and the locator is not None.
                    # It such cases, job has been deleted,
                    # and there is nothing more to get.
                    if result_response.status_code == 404 and locator is not None:
                        logging.warning("SFDC Bulk API job %s was deleted",
                                        job_id)
                        locator = "null"
                    else:
                        # Let simple-salesforce handle it.
                        exception_handler(result_response, name=result_path)
                else:
                    if "Sforce-Locator" in result_response.headers:
                        locator = result_response.headers["Sforce-Locator"]
                    else:
                        # No locator means there is only one set of results,
                        # but we explicitly assign if to a special "null" value
                        # because this is what's returned when the last set
                        # was retrieved in the multiple-batch situation.
                        locator = "null"
                    yield result_response.iter_lines(decode_unicode=True)

    @staticmethod
    def _bulk_delete_job(sfdc_connection: Salesforce, job_id):
        # Delete job to free up Salesforce job storage.
        job_status_path = f"jobs/query/{job_id}"
        sfdc_connection.session.request(
            "DELETE",
            f"{sfdc_connection.base_url}{job_status_path}",
            headers=sfdc_connection.headers.copy(),
        ).close()

    @staticmethod
    def _run_bq_load_job(
        client: bigquery.Client,
        csv_batch_file: str,
        destination: bigquery.TableReference,
        job_config: bigquery.LoadJobConfig,
    ) -> int:
        """Loads CVS file into BigQuery

        Args:
            client (bigquery.Client): BigQuery client to use.
            csv_batch_file (str): CVS file path.
            destination (bigquery.TableReference): Destination table.
            job_config (bigquery.LoadJobConfig): BigQuery job config.

        Returns:
            int: Number of inserted rows.
        """
        logging.info(
            "Loading a data batch from %s (%i bytes) to BigQuery table %s.",
            csv_batch_file,
            Path(csv_batch_file).stat().st_size,
            destination,
        )
        with open(csv_batch_file, "rb") as file:
            job = client.load_table_from_file(
                file,
                destination,
                job_config=job_config,
                project=destination.project,
            )
            job.result()
            logging.info("Done. %i rows were added.", job.output_rows)
            return job.output_rows

    @staticmethod
    def _find_last_load_datetime_str(client: bigquery.Client, project_name,
                                     dataset_name,
                                     table_name) -> typing.Union[str, None]:
        """Method to get latest Recordstamp so the DAG can determine whether to
            do full or incremental load.

        Args:
            project_name: BQ project
            dataset_name: BQ Dataset
            table_name: BQ table that is currently being worked on

        Returns:
            None if the current table doesn't exist, or the Recordstamp cannot
            be retrieved. Otherwise, returns a formatted string containing
            latest record update datetime.
        """

        full_table_name = project_name + "." + dataset_name + "." + table_name
        try:
            client.get_table(full_table_name)
            query_job = client.query(
                f"SELECT MAX({SalesforceToBigquery._RECORD_STAMP_NAME_})"
                f" FROM `{full_table_name}`")

            # This query is guaranteed to return one column and one row.
            last_update_time_value = list(query_job)[0][0]
            last_update_time = last_update_time_value.strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ") if last_update_time_value else None

            return last_update_time

        except NotFound:
            logging.info(
                "Table '%s' does not exist.",
                full_table_name,
            )
            return None

    @staticmethod
    def _bigquery_commit(client: bigquery.Client,
                         table: bigquery.TableReference,
                         preliminary_recordstamp: datetime,
                         new_recordstamp: datetime):
        """Performs replication job data "commit" by updating
            preliminary value of Recordstamp field to
            the biggest value of SystemModStamp of inserted rows.

        Args:
            client (bigquery.Client): BigQuery client to use.
            table (bigquery.TableReference): Destination table.
            preliminary_recordstamp (datetime): Recordstamp value to update.
            new_recordstamp (datetime): Recordstamp value to set.
        """
        old_ts_value = preliminary_recordstamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        new_ts_value = new_recordstamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        query = (
            f"UPDATE `{table}` SET"
            f" {SalesforceToBigquery._RECORD_STAMP_NAME_}='{new_ts_value}'"
            " WHERE"
            f" {SalesforceToBigquery._RECORD_STAMP_NAME_}='{old_ts_value}'")
        job_config = bigquery.QueryJobConfig(use_legacy_sql=False)
        query_job = client.query(query, job_config=job_config)
        query_job.result()
        row_num = query_job.num_dml_affected_rows
        logging.info("Finalized %i rows in %s.", row_num, table)

    @staticmethod
    def _create_sfdc_query(api_name: str, column_list: str,
                           last_load_timestamp_string: str,
                           op_flag: str) -> str:
        """Building SFDC query depending on the incremental logic."""

        query = f"SELECT {column_list} FROM {api_name}"
        if last_load_timestamp_string is not None:
            if op_flag == "I":
                query += f" WHERE CreatedDate>{last_load_timestamp_string}"
            elif op_flag in ["U", "D"]:
                query += f" WHERE SystemModStamp>{last_load_timestamp_string} AND CreatedDate<={last_load_timestamp_string}"
                if op_flag == "D":
                    query += " AND IsDeleted = true"
            else:
                raise ValueError(f"{op_flag} is an invalid value of op_flag")

        return query

    @staticmethod
    def _upload_batches_to_bq(
        client: bigquery.Client,
        batches: typing.Iterable[typing.Iterable[str]],
        table: bigquery.TableReference,
        sfdc_to_bq_field_map: typing.Dict[str, typing.Tuple[str, str]],
        extra_fields: typing.Dict[str, typing.Tuple[str, str]],
    ) -> None:
        """Processes batches of Salesforce Bulk API 2.0 query.
        It retrieves CSV lines from the Bulk API batches,
        renames the header with the target names,
        appends extra fields,
        saves every batch to a CSV file,
        and calls _run_bq_load_job to load the CSV to BigQuery.

        Args:
            client (bigquery.Client): BigQuery client to use.
            batches (typing.Iterable[typing.Iterable[str]]):
                generator returned by _bulk_get_records call.
            table (bigquery.TableReference): Destination table.
            sfdc_to_bq_field_map (typing.Dict[str, typing.Tuple[str, str]]):
                Salesforce-to-BigQuery field name mapping dictionary.
            extra_fields (typing.Dict[str, typing.Tuple[str, str]]):
                Extra fields and their values to add to every row.
                Dict[target_name, (value, target_type)]
        """
        batch_count = 0
        record_count = 0
        renamed_header = None

        schema = []
        for f in list(sfdc_to_bq_field_map.items()):
            source_name = f[0]
            schema.append(bigquery.SchemaField(name=f[1][0],
                                               field_type=f[1][1]))
            # Renaming source field to lowercase,
            # so we can ignore the case when renaming
            sfdc_to_bq_field_map[
                source_name.lower()] = sfdc_to_bq_field_map.pop(source_name)

        source_fields = list(sfdc_to_bq_field_map.keys())

        for f in extra_fields.items():
            schema.append(bigquery.SchemaField(name=f[0], field_type=f[1][1]))

        # Create table with the target schema.
        # It will force types interpretation when loading data.
        # We keep the schema of the original table,
        # assuming it's correct.
        table_obj = client.create_table(bigquery.Table(table, schema),
                                        exists_ok=True)
        table = table_obj.reference

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            skip_leading_rows=1,
            schema=table_obj.schema,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
            source_format=bigquery.SourceFormat.CSV,
            allow_quoted_newlines=True,
            null_marker="",
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        for batch in batches:
            first_line = True
            with tempfile.NamedTemporaryFile(
                    "w",
                    encoding="utf-8",
                    prefix=f"{table.table_id}_",
                    suffix=".csv",
            ) as file:
                lines = []
                new_line_processing = False
                has_valid_lines = False

                # Processing lines from the returned CSV.
                # We need that for 2 reasons:
                #   1. To rename fields in the header.
                #   2. To add Recordstamp and OperationalFlag extra fields.
                #
                # We cannot use Python's CSV because it removes new lines
                # inside string values.

                for line_ in batch:
                    line = str(line_)
                    if first_line:
                        # It's the first line in CVS file.
                        # We need to replace the original names in the header
                        # with the target field names from the schema map.
                        first_line = False
                        if not renamed_header:
                            # No renamed header yet, so make one
                            columns = line.split(",")
                            for i in range(0, len(columns)):
                                column = columns[i].strip('"')
                                if column.lower() in source_fields:
                                    columns[i] = \
                                        f'"{sfdc_to_bq_field_map[column.lower()][0]}"'
                                else:
                                    logging.error(
                                        "Unknown column %s."
                                        " This job will most likely fail.",
                                        column)
                            columns.extend(
                                [f'"{v}"' for v in extra_fields.keys()])
                            renamed_header = ",".join(columns)
                        # Replace the header with the renamed one.
                        line = renamed_header
                    else:
                        # Adding extra fields Recordstamp and OperationalFlag.

                        quote_number = line.count('"')
                        # If not even number of quotes,
                        # the row has a new line inside a string.
                        # To handle that,
                        # we have to write all lines with
                        # even number of quotes
                        # as they are, until there is another line
                        # with an odd number of quotes.

                        if quote_number == 0 and not new_line_processing:
                            # empty line, just skip it
                            continue
                        if quote_number % 2 == 1:
                            new_line_processing = not new_line_processing
                        if not new_line_processing:
                            for v in extra_fields.values():
                                line += f',"{v[0]}"'
                                has_valid_lines = True

                    lines.append(f"{line}\n")

                    # Dumping every 512 lines to the CSV file.
                    if (len(lines) >=
                            SalesforceToBigquery._MAX_LINES_IN_CVS_WRITE_BATCH_
                       ):
                        file.writelines(lines)
                        lines.clear()

                # Making sure all lines are in the CSV file.
                if len(lines) > 0:
                    file.writelines(lines)
                # Flushing before _run_bq_load_job re-opens it in binary mode.
                file.flush()

                batch_count += 1
                if has_valid_lines:
                    record_count += SalesforceToBigquery._run_bq_load_job(
                        client, file.name, table, job_config)
                else:
                    logging.info("No BigQuery records in this batch.")
