#!/bin/bash

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

# This script deploys a single SQL file and BigQuery location provided as
# parameters.
# It also expects jinja tempate data file as parameter for template substituion.

sql_file="$1"
bq_location="$2"
jinja_data_file="$3"

if [[ "${sql_file}" == "" ]]; then
  echo -e "🛑 ERROR: no SQL file provided."
  exit 1
fi

if [[ "${bq_location}" == "" ]]; then
  echo -e "🛑 ERROR: no BigQuery location provided."
  exit 1
fi

if [[ "${jinja_data_file}" == "" ]]; then
  echo -e "🛑 ERROR: no Jinja Template Data File."
  exit 1
fi

if [[ ! -f "${jinja_data_file}" ]]; then
  echo -e "🛑 ERROR: ${jinja_data_file} not found."
  exit 1
fi

#--------------------
# Main logic
#--------------------

# Useful for debugging
echo "== Generating SQL query from ${sql_file} using the following parameters: =="
cat "${jinja_data_file}"
# sql_file_full="$(realpath "${sql_file}")"

query=$(jinja -d "${jinja_data_file}" -f json "${sql_file}")
echo "${query}"

set +e
BQ_STR=$(bq query --batch --location="${bq_location}" --use_legacy_sql=false "${query}" 2>&1)
ERR_CODE=$?

if [[ ${ERR_CODE} -ne 0 && "${BQ_STR}" == *"Retrying may solve the problem"* ]]; then
  echo "⚠️ Error encountered during BigQuery job execution (${ERR_CODE}). Retrying..."
  sleep 5s
  bq query --batch --location="${bq_location}" --use_legacy_sql=false "${query}"
  ERR_CODE=$?
else
  echo "${BQ_STR}"
fi

exit $ERR_CODE
