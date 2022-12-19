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
"""Library for Cortex Config related functions."""

import json
import logging
from pathlib import Path


def _validate_config(config):
    """Validates configurations.

    Args:
        config: Dictionary of config as read from a config file.

    Raises:
      ValueError: If config is missing a required value.
    """

    # TODO: This only validates for SFDC for now. This can and should be
    # expanded to SAP as well when this moves to common libs.

    logging.debug("Validating config")

    if config is None:
        raise ValueError("Config is empty.")

    # Make sure all the other needed top level configs are in place.
    missing_attr = []
    for attr in ("testData", "generateExtraData", "deployCDC", "deploySAP",
                 "deploySFDC", "turboMode", "projectIdSource",
                 "projectIdTarget", "location", "languages", "currencies",
                 "SFDC"):
        if config.get(attr) is None or config.get(attr) == "":
            missing_attr.append(attr)

    if missing_attr:
        raise ValueError("Config file is missing needed attributes or "
                         f"has empty value: {missing_attr}")

    sfdc_attr = config.get("SFDC")

    # Let's validate SFDC specific attributes.
    sfdc_datasets = sfdc_attr.get("datasets")
    if sfdc_datasets is None:
        raise ValueError("Config file is missing 'SFDC.datasets' attribute.")

    missing_datasets_attr = []
    for attr in ("cdc", "raw", "reporting"):
        if sfdc_datasets.get(attr) is None or sfdc_datasets.get(attr) == "":
            missing_datasets_attr.append(attr)

    if missing_datasets_attr:
        raise ValueError(
            "Config file is missing or has empty value for one or more "
            f"SFDC datasets attributes: {missing_datasets_attr} ")

    logging.debug("Config file validated and is looking good.")


def load_config_file(config_file):
    """Loads a json config file to a dictionary and validates it.

    Args:
        config_file: Path of config json file.

    Returns:
        Config as a dictionary.
    """
    logging.debug("Input file = %s", config_file)

    if not Path(config_file).is_file():
        raise FileNotFoundError(f"Config file '{config_file}' does not exist.")

    with open(config_file, mode="r", encoding="utf-8") as f:
        try:
            config = json.load(f)
        except json.JSONDecodeError:
            e_msg = f"Config file '{config_file}' is malformed or empty."
            raise Exception(e_msg) from None

        logging.debug("config = %s", config)

        _validate_config(config)

    return config
