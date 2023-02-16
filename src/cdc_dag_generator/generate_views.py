# Copyright 2023 Google LLC

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
Generates views to project Salesforce RAW dataset table fields
to CDC according to the defined CDC schema.
"""

import csv
import json
import logging
import sys
import yaml
from pathlib import Path
import typing

from py_libs.bq_helper import execute_sql_file, table_exists
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
_TEMPLATE_SQL_NAME = "view_template"

# Directory under which all the generated sql files will be created.
_GENERATED_VIEW_SQL_DIR = "generated_view_sql"

# Directory containing various template files.
_TEMPLATE_DIR = Path(_THIS_DIR, "templates")
# Directory containing various template files.
_SQL_TEMPLATE_DIR = Path(_TEMPLATE_DIR, "sql")


def process_table(table_setting, raw_project, raw_dataset, cdc_project,
                  cdc_dataset):
    """For a given table config, creates required view SQL,
    and if the raw table exists, executes the SQL.
    """

    base_table = table_setting["base_table"].lower()
    raw_table = table_setting["raw_table"]

    source_table = raw_project + "." + raw_dataset + "." + raw_table
    target_view = cdc_project + "." + cdc_dataset + "." + base_table
    raw_exists = table_exists(source_table)

    if not raw_exists:
        logging.error(("Source raw table `%s` doesn't exist! \n"
                       "CDC view cannot be created."), source_table)
        raise SystemExit("⛔️ Failed to deploy CDC views.")

    schema_file = Path(_THIS_DIR,
                       f"../table_schema/{base_table}.csv").absolute()

    logging.info("__ Processing view '%s' __", base_table)

    sfdc_to_bq_field_map: typing.Dict[str, typing.Tuple[str, str]] = {}

    # TODO: Check Config File schema.
    with open(
            schema_file,
            encoding="utf-8",
            newline="",
    ) as csv_file:
        for row in csv.DictReader(csv_file, delimiter=","):
            sfdc_to_bq_field_map[row["SourceField"]] = (row["TargetField"],
                                                        row["DataType"])

    # SQL file generation
    #########################
    sql_template_file_name = (_GENERATED_FILE_PREFIX + _TEMPLATE_SQL_NAME +
                              ".sql")
    sql_file_name = (_GENERATED_FILE_PREFIX + base_table.replace(".", "_") +
                     "_view.sql")
    sql_template_file = Path(_SQL_TEMPLATE_DIR, sql_template_file_name)
    output_sql_file = Path(_GENERATED_VIEW_SQL_DIR, sql_file_name)

    field_assignments = [
        f"`{f[0]}` AS `{f[1][0]}`" for f in sfdc_to_bq_field_map.items()
    ]

    sql_subs = {
        "source_table": source_table,
        "target_view": target_view,
        "field_assignments": ",".join(field_assignments)
    }

    generate_file_from_template(sql_template_file, output_sql_file, **sql_subs)
    logging.info("Generated CDC view SQL file.")

    try:
        if table_exists(target_view):
            logging.warning(("⚠️ View or table %s already exists. "
                             "Skipping it."), target_view)
        else:
            logging.info("Creating view %s", target_view)
            execute_sql_file(output_sql_file, True)
            logging.info("✅ Created CDC view %s.", target_view)
            # deleting SQL file as we are not going to need it.
            output_sql_file.unlink()
    except Exception as e:
        logging.error("Failed to create CDC view '%s'.\n"
                      "ERROR: %s", target_view, str(e))
        raise SystemExit(
            "⛔️ Failed to deploy CDC views. Please check the logs.") from e

    logging.info("__ View '%s' processed.__", base_table)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Generating CDC views...")

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

    logging.info(
        "\n---------------------------------------\n"
        "Using the following parameters from config:\n"
        "  raw_project = %s \n"
        "  raw_dataset = %s \n"
        "  cdc_project = %s \n"
        "  cdc_dataset = %s \n"
        "---------------------------------------\n", raw_project, raw_dataset,
        cdc_project, cdc_dataset)

    Path(_GENERATED_VIEW_SQL_DIR).mkdir(exist_ok=True)

    # Process tables based on table settings from settings file
    logging.info("Reading table settings...")

    if not Path(_SETTINGS_FILE).is_file():
        logging.warning(
            "File '%s' does not exist. Skipping CDC view generation.",
            _SETTINGS_FILE)
        sys.exit()

    with open(_SETTINGS_FILE, encoding="utf-8") as settings_file:
        settings = yaml.load(settings_file, Loader=yaml.SafeLoader)

    if not settings:
        logging.warning("File '%s' is empty. Skipping CDC view generation.",
                        _SETTINGS_FILE)
        sys.exit()

    # TODO: Check settings file schema.

    if not "raw_to_cdc_tables" in settings:
        logging.warning(
            "File '%s' is missing property `raw_to_cdc_tables`. "
            "Skipping CDC view generation.", _SETTINGS_FILE)
        sys.exit()

    logging.info("Processing tables...")

    table_settings = settings["raw_to_cdc_tables"]
    for table_setting in table_settings:
        process_table(table_setting, raw_project, raw_dataset, cdc_project,
                      cdc_dataset)

    logging.info("Done generating CDC views.")


if __name__ == "__main__":
    main()
