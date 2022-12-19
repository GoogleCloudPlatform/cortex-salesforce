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
"""
Generates DAG and related files needed to copy/move Salesforce data from
RAW dataset to CDC dataset.
"""

import datetime
import json
import logging
import sys
import yaml
from pathlib import Path

from py_libs.bq_helper import execute_sql_file
from py_libs.cdc import create_cdc_table
from py_libs.configs import load_config_file
from py_libs.dag_generator import generate_file_from_template

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

_THIS_DIR = Path(__file__).resolve().parent

# Config file containing various parameters.
_CONFIG_FILE = Path(_THIS_DIR, "../../config/sfdc_config.json")

# Settings file containing tables to be copied from SFDC.
_SETTINGS_FILE = Path(_THIS_DIR, "../../config/setting.yaml")

_GENERATED_FILE_PREFIX = "sfdc_raw_to_cdc_"

# Directory under which all the generated dag files and related files
# will be created.
_GENERATED_DAG_DIR = "generated_dag"
# Directory under which all the generated sql files will be created.
_GENERATED_DAG_SQL_DIR = "generated_sql"

# Directory containing various template files.
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")
# Directory containing various template files.
_SQL_TEMPLATE_DIR = Path(_TEMPLATE_DIR, "sql")


def process_table(table_setting, raw_project, raw_dataset, cdc_project,
                  cdc_dataset, load_test_data):
    """For a given table config, creates required tables as well as
    dag and related files. """

    base_table = table_setting["base_table"].lower()

    logging.info("__ Processing table '%s' __", base_table)

    # Create CDC table if needed.
    #############################
    try:
        create_cdc_table(table_setting, raw_project, raw_dataset, cdc_project,
                         cdc_dataset)
    except Exception as e:
        logging.error("Failed while processing table '%s'.\n"
                      "ERROR: %s", base_table, str(e))
        raise SystemExit(
            "⛔️ Failed to deploy CDC. Please check the logs.") from e

    # Python file generation
    #########################
    python_template_file = Path(_TEMPLATE_DIR, "airflow_dag_raw_to_cdc.py")
    output_py_file_name = (_GENERATED_FILE_PREFIX +
                           base_table.replace(".", "_") + ".py")
    output_py_file = Path(_GENERATED_DAG_DIR, output_py_file_name)

    today = datetime.datetime.now()
    load_frequency = table_setting["load_frequency"]
    py_subs = {
        "base_table": base_table,
        "load_frequency": load_frequency,
        "year": today.year,
        "month": today.month,
        "day": today.day
    }

    generate_file_from_template(python_template_file, output_py_file, **py_subs)

    logging.info("Generated dag python files")

    # SQL file generation
    #########################
    sql_file_name = (_GENERATED_FILE_PREFIX + base_table.replace(".", "_") +
                     ".sql")
    sql_template_file = Path(_SQL_TEMPLATE_DIR, sql_file_name)
    output_sql_file = Path(_GENERATED_DAG_SQL_DIR, sql_file_name)

    sql_subs = {
        "source_table": raw_project + "." + raw_dataset + "." + base_table,
        "target_table": cdc_project + "." + cdc_dataset + "." + base_table
    }

    # TODO:Generate sql files from a common template, instead of individual
    # templates.
    generate_file_from_template(sql_template_file, output_sql_file, **sql_subs)
    logging.info("Generated dag sql files")

    # If test data is needed, we want to populate the CDC table from data in
    # the RAW tables. Let's use the DAG SQL file to do that.
    if load_test_data:
        try:
            execute_sql_file(output_sql_file)
            logging.info("Populated CDC table with test data.")
        except Exception as e:
            logging.error("Failed to populate CDC table '%s'.\n"
                          "ERROR: %s", (cdc_dataset + "." + base_table), str(e))
            raise SystemExit(
                "⛔️ Failed to deploy CDC. Please check the logs.") from e

    logging.info("__ Table '%s' processed.__", base_table)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Generating CDC tables and DAGS...")

    # Lets load configs to get various parameters needed for the dag generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    # Read params from the config
    raw_project = config_dict.get("projectIdSource")
    raw_dataset = config_dict.get("SFDC").get("datasets").get("raw")
    cdc_project = config_dict.get("projectIdSource")
    cdc_dataset = config_dict.get("SFDC").get("datasets").get("cdc")
    load_test_data = config_dict.get("testData")

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  raw_project = %s \n"
        "  raw_dataset = %s \n"
        "  cdc_project = %s \n"
        "  cdc_dataset = %s \n"
        "  load_test_data = %s \n"
        "---------------------------------------\n", raw_project, raw_dataset,
        cdc_project, cdc_dataset, load_test_data)

    Path(_GENERATED_DAG_DIR).mkdir(exist_ok=True)
    Path(_GENERATED_DAG_SQL_DIR).mkdir(exist_ok=True)

    # Process tables based on table settings from settings file
    logging.info("Reading table settings...")

    if not Path(_SETTINGS_FILE).is_file():
        logging.warning(
            "️File '%s' does not exist. Skipping CDC DAG generation.",
            _SETTINGS_FILE)
        sys.exit()

    with open(_SETTINGS_FILE, encoding="utf-8") as settings_file:
        settings = yaml.load(settings_file, Loader=yaml.SafeLoader)

    if not settings:
        logging.warning("File '%s' is empty. Skipping CDC DAG generation.",
                        _SETTINGS_FILE)
        sys.exit()

    # TODO: Check settings file schema.

    if not "raw_to_cdc_tables" in settings:
        logging.warning(
            "File '%s' is missing property `raw_to_cdc_tables`. "
            "Skipping CDC DAG generation.", _SETTINGS_FILE)
        sys.exit()

    logging.info("Processing tables...")

    table_settings = settings["raw_to_cdc_tables"]
    for table_setting in table_settings:
        process_table(table_setting, raw_project, raw_dataset, cdc_project,
                      cdc_dataset, load_test_data)

    logging.info("Done generating CDC tables and DAGs.")


if __name__ == "__main__":
    main()
