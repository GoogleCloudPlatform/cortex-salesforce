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
""" Generates Cloud Build files that will create reporting bq objects.  """

import json
import logging
import shutil

from jinja2 import Environment, FileSystemLoader
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from py_libs.configs import load_config_file

# NOTE: All paths here are relative to the root directory, unless specified
# otherwise.

_THIS_DIR = Path(__file__).resolve().parent

# Config file containing various parameters.
_CONFIG_FILE = Path(_THIS_DIR, "../../config/sfdc_config.json")

# File providing order in which bq objects should be created.
_DEPENDENCIES_FILE = Path(_THIS_DIR, "bq_dependencies.txt")

# Directory where all generated files will be created.
_GENERATED_FILES_DIR = Path(_THIS_DIR, "generated_reporting_files")

# Jinja data file containing substitution values for bq object creating step.
_JINJA_DATA_FILE = Path(_GENERATED_FILES_DIR, "bq_sql_jinja_data.json")

# Template containing a build step that will create one bq object from a file.
_CLOUDBUILD_TEMPLATE_DIR = Path(_THIS_DIR, "templates")
_CLOUDBUILD_TEMPLATE_FILE = "cloudbuild_create_bq_objects.yaml.jinja"


def _create_reporting_dataset(config_dict):
    """Creates BQ reporting dataset if needed."""

    gcp_project = config_dict["projectIdTarget"]
    reporting_dataset = config_dict["SFDC"]["datasets"]["reporting"]
    reporting_dataset_full_name = gcp_project + "." + reporting_dataset

    client = bigquery.Client()

    logging.info("Creating '%s' dataset if needed.", reporting_dataset)
    ds = bigquery.Dataset(reporting_dataset_full_name)
    ds.location = config_dict["location"]
    ds = client.create_dataset(ds, exists_ok=True, timeout=30)


def _generate_jinja_data_file(config_dict):
    """Generates jinja data file that will be used to create BQ objects."""
    with open(_JINJA_DATA_FILE, "w", encoding="utf-8") as f:
        jinja_data_file_dict = {
            "project_id_src":
                config_dict["projectIdSource"],
            "project_id_tgt":
                config_dict["projectIdTarget"],
            "currencies":
                ",".join(
                    f"'{currency}'" for currency in config_dict["currencies"]),
            "languages":
                ",".join(
                    f"'{language}'" for language in config_dict["languages"]),
            "dataset_cdc_processed_sfdc":
                config_dict["SFDC"]["datasets"]["cdc"],
            "dataset_raw_landing_sfdc":
                config_dict["SFDC"]["datasets"]["raw"],
            "dataset_reporting_tgt_sfdc":
                config_dict["SFDC"]["datasets"]["reporting"]
        }
        f.write(json.dumps(jinja_data_file_dict, indent=4))

    logging.info("Jinja template data file '%s' created successfully.",
                 _JINJA_DATA_FILE)


def _create_build_files(config_dict):
    """Generates build files that will create reporting bq objects."""
    with open(_DEPENDENCIES_FILE, encoding="utf-8") as df:
        file_entries = [line.strip() for line in df]

    location = config_dict["location"]
    wait_for_prev_step = False

    # Create a list containing entry for each sql file and it's parameters
    # that can be used with Jinja to create needed build files.
    build_file_master_list = []
    for file_entry in file_entries:
        if file_entry.startswith("-"):
            # Once we hit a line starting with "-", rest of the entries should
            # wait for previous builds to complete.
            wait_for_prev_step = True
            logging.debug("Skipping to the next entry")
            continue
        # Although not expected, empty lines in the file are to be skipped.
        if file_entry == "":
            continue

        build_file_master_list.append({
            "sql_file_name": file_entry,
            "wait_for_prev_step": wait_for_prev_step
        })

    # Since cloud build limits 100 steps in one build file, let's split
    # our list such that each list contains at the most 100 entries.
    # Each of these lists will be used to generate one "big" build
    # file that will create Reporting BQ objects one object at a time.
    build_files_lists = [
        build_file_master_list[x:x + 100]
        for x in range(0, len(build_file_master_list), 100)
    ]

    # Generate build file for each list, using Jinja.
    environment = Environment(loader=FileSystemLoader(_CLOUDBUILD_TEMPLATE_DIR))
    build_file_template = environment.get_template(_CLOUDBUILD_TEMPLATE_FILE)
    build_file_counter = 0
    for build_files_list in build_files_lists:
        build_file_counter += 1
        build_file_text = build_file_template.render({
            "location": location,
            "build_files": build_files_list
        })

        build_file_num = f"{build_file_counter:02d}"
        build_file = Path(
            _GENERATED_FILES_DIR,
            f"cloudbuild.reporting.create_bq_objects.{build_file_num}.yaml")
        with open(build_file, "a", encoding="utf-8") as bf:
            logging.debug("Opened build file '%s'", build_file)
            bf.write(build_file_text)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info(
        "Generating individual Cloudbuild files for reporting objects...")

    # Create output directory, but remove first if already exists.
    # This is important, because otherwise, we may keep on appending to files
    # created by an earlier run.
    if _GENERATED_FILES_DIR.exists():
        shutil.rmtree(_GENERATED_FILES_DIR)
    Path(_GENERATED_FILES_DIR).mkdir(exist_ok=True)

    # Lets load configs to get various parameters needed for the dag generation.
    config_dict = load_config_file(_CONFIG_FILE)
    logging.info(
        "\n---------------------------------------\n"
        "Using the following config:\n %s"
        "\n---------------------------------------\n",
        json.dumps(config_dict, indent=4))

    # Let's make sure the reporting dataset exists. If not, create it.
    logging.info("Creating reporting dataset if needed...")
    _create_reporting_dataset(config_dict)

    # Let's create jinja template substitution file needed when running
    # individual bq object build file.
    logging.info("Creating jinja data file for creating bq objects later...")
    _generate_jinja_data_file(config_dict)

    logging.info("Creating build files that will create bq objects...")
    _create_build_files(config_dict)

    logging.info(
        "Finished generating individual Cloudbuild files for reporting.")


if __name__ == "__main__":
    main()
