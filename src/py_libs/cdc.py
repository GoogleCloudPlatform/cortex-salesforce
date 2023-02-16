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
"""Library for CDC related functions."""

import logging
from google.cloud.exceptions import NotFound
from google.cloud import bigquery

# Supported partition types.
_PARTITION_TYPES = ["time", "integer_range"]

# Column data types supported for time based partitioning.
_TIME_PARTITION_DATA_TYPES = ["DATE", "TIMESTAMP", "DATETIME"]

# Supported grains for time based partitioning.
_TIME_PARTITION_GRAIN_LIST = ["hour", "day", "month", "year"]

# Dict to convert string values to correct partitioning type.
_TIME_PARTITION_GRAIN_DICT = {
    "hour": bigquery.TimePartitioningType.HOUR,
    "day": bigquery.TimePartitioningType.DAY,
    "month": bigquery.TimePartitioningType.MONTH,
    "year": bigquery.TimePartitioningType.YEAR
}

# Frequency to refresh CDC table. These values corresponds to
# Apache Airflow / Cloud Composer DAG schedule interval values.
_LOAD_FREQUENCIES = [
    "None", "@once", "@hourly", "@daily", "@weekly", "@monthly", "@yearly"
]


def validate_partition_details(partition_details):

    partition_column = partition_details.get("column")
    if not partition_column:
        e_msg = ("Partition `column` property missing from "
                 "`partition_details` property.")
        return e_msg

    partition_type = partition_details.get("partition_type")
    if not partition_type:
        e_msg = ("`partition_type` property missing from "
                 "`partition_details` property.")
        return e_msg

    if partition_type not in _PARTITION_TYPES:
        e_msg = ("`partition_type` has to be one of the following:"
                 f"{_PARTITION_TYPES}.\n"
                 f"Specified `partition_type` is '{partition_type}'.")
        return e_msg

    if partition_type == "time":
        time_partition_grain = partition_details.get("time_grain")
        if not time_partition_grain:
            e_msg = ("`time_grain` property missing for "
                     "`time` based partition.")
            return e_msg

        if time_partition_grain not in _TIME_PARTITION_GRAIN_LIST:
            e_msg = ("`time_grain` property has to be one of the following:"
                     f"{_TIME_PARTITION_GRAIN_LIST}.\n"
                     f"Specified `time_grain` is '{time_partition_grain}'.")
            return e_msg

    if partition_type == "integer_range":
        integer_range_bucket = partition_details.get("integer_range_bucket")
        if not integer_range_bucket:
            e_msg = ("`integer_range_bucket` property missing for "
                     "`integer_range` based partition.")
            return e_msg

        bucket_start = integer_range_bucket.get("start")
        bucket_end = integer_range_bucket.get("end")
        bucket_interval = integer_range_bucket.get("interval")

        if (bucket_start is None or bucket_end is None or
                bucket_interval is None):
            e_msg = ("Error: `start`, `end` or `interval` property missing for "
                     "the `integer_range_bucket` property.")
            return e_msg

    return None


def validate_cluster_details(cluster_details):

    cluster_columns = cluster_details.get("columns")

    if not cluster_columns or len(cluster_columns) == 0:
        e_msg = "`columns` property missing from `cluster_details` property."
        return e_msg

    if not isinstance(cluster_columns, list):
        e_msg = "`columns` property in `cluster_details` has to be a List."
        return e_msg

    if len(cluster_columns) > 4:
        e_msg = ("More than 4 columns specified in `cluster_details` property. "
                 "BigQuery supports maximum of 4 columns for table cluster.")
        return e_msg

    return None


def validate_table_config(table_setting):
    """Makes sure the config for a table in settings file is valid."""
    load_frequency = table_setting.get("load_frequency")
    if not load_frequency:
        e_msg = "Missing `load_frequency` property."
        return e_msg

    if load_frequency not in _LOAD_FREQUENCIES:
        e_msg = ("`load_frequency` has to be one of the following:"
                 f"{_LOAD_FREQUENCIES}.\n"
                 f"Specified `load_frequency` is '{load_frequency}'.")
        return e_msg

    partition_details = table_setting.get("partition_details")
    cluster_details = table_setting.get("cluster_details")

    # Validate partition details.
    if partition_details:
        e_msg = validate_partition_details(partition_details)
        if e_msg:
            return e_msg

    if cluster_details:
        e_msg = validate_cluster_details(cluster_details)
        if e_msg:
            return e_msg

    return None


def validate_table_configs(table_configs):
    """Makes sure all the configs provided in settings file is valid."""
    tables_processed = set()
    for config in table_configs:
        table_name = config.get("base_table")
        if not table_name:
            e_msg = "`base_table` property missing from an entry."
            return e_msg
        error_message = validate_table_config(config)

        logging.info(".... Checking configs for table '%s' ....", table_name)

        if error_message:
            e_msg = (f"Invalid settings for table '{table_name}'.\n"
                     f"{error_message}")
            return e_msg

        # Check for duplicate entries.
        if table_name in tables_processed:
            e_msg = f"Table '{table_name}' is present multiple times."
            return e_msg
        else:
            tables_processed.add(table_name)

        logging.info(".... Check for configs for table '%s' success",
                     table_name)


def validate_cluster_columns(cluster_details, target_schema):
    """Checks schema to make sure columns are appropriate for clustering."""
    cluster_columns = cluster_details["columns"]
    for column in cluster_columns:
        cluster_column_details = [
            field for field in target_schema if field.name == column
        ]
        if not cluster_column_details:
            e_msg = (f"Column '{column}' specified for clustering does "
                     "not exist in the table.")
            raise Exception(e_msg) from None


def validate_partition_columns(partition_details, target_schema):
    """Checks schema to make sure columns are appropriate for partitioning."""

    column = partition_details["column"]

    partition_column_details = [
        field for field in target_schema if field.name == column
    ]
    if not partition_column_details:
        e_msg = (f"Column '{column}' specified for partitioning does not "
                 "exist in the table.")
        raise Exception(e_msg) from None

    # Since there will be only value in the list (a column exists only once
    # in a table), let's just use the first value from the list.
    partition_column_type = partition_column_details[0].field_type

    partition_type = partition_details["partition_type"]

    if (partition_type == "time" and
            partition_column_type not in _TIME_PARTITION_DATA_TYPES):
        e_msg = ("For `partition_type` = 'time', partitioning column has to be "
                 "one of the following data types:"
                 f"{_TIME_PARTITION_DATA_TYPES}.\n"
                 f"But column '{column}' is of '{partition_column_type}' type.")
        raise Exception(e_msg) from None

    if (partition_type == "integer_range" and
            partition_column_type != "INTEGER"):
        e_msg = ("Error: For `partition_type` = 'integer_range', "
                 "partitioning column has to be of INTEGER data type.\n"
                 f"But column '{column}' is of '{partition_column_type}'.")
        raise Exception(e_msg) from None


def create_cdc_table(table_setting, cdc_project, cdc_dataset, schema):
    """Creates CDC table based on source RAW table schema.

    Retrieves schema details from source table in RAW dataset and creates a
    table in CDC dataset based on that schema if it does not exist.

    Args:
        table_setting: Table config as defined in the settings file.
        cdc_project: BQ CDC project.
        cdc_dataset: BQ CDC dataset name.
        schema: CDC table schema as a list of tuples (column name, column type)
    """

    client = bigquery.Client()

    base_table: str = table_setting["base_table"].lower()
    cdc_table_name = cdc_project + "." + cdc_dataset + "." + base_table
    partition_details = table_setting.get("partition_details")
    cluster_details = table_setting.get("cluster_details")

    try:
        _ = client.get_table(cdc_table_name)
        logging.warning("Table '%s' already exists. Not creating it again.",
                        cdc_table_name)
    except NotFound:
        # Let's create CDC table.
        logging.info("Table '%s' does not exists. Creating it.", cdc_table_name)

        target_schema = [
            bigquery.SchemaField(name=f[0], field_type=f[1]) for f in schema
        ]

        cdc_table = bigquery.Table(cdc_table_name, schema=target_schema)

        # Add clustering and partitioning properties if specified.
        if partition_details:
            validate_partition_columns(partition_details, target_schema)
            # Add relevant partitioning clause
            if partition_details["partition_type"] == "time":
                time_partition_grain = partition_details["time_grain"]
                cdc_table.time_partitioning = bigquery.TimePartitioning(
                    field=partition_details["column"],
                    type_=_TIME_PARTITION_GRAIN_DICT[time_partition_grain])
            else:
                integer_range_bucket = partition_details["integer_range_bucket"]
                bucket_start = integer_range_bucket["start"]
                bucket_end = integer_range_bucket["end"]
                bucket_interval = integer_range_bucket["interval"]
                cdc_table.range_partitioning = bigquery.RangePartitioning(
                    field=partition_details["column"],
                    range_=bigquery.PartitionRange(start=bucket_start,
                                                   end=bucket_end,
                                                   interval=bucket_interval))

        if cluster_details:
            validate_cluster_columns(cluster_details, target_schema)
            cdc_table.clustering_fields = cluster_details["columns"]

        _ = client.create_table(cdc_table)

        logging.info("Created table '%s'.", cdc_table_name)
