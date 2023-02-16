#!/bin/bash
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

# This script executes SFDC reporting deployment
# using parameters in sfdc_config.config file.
#   1. Calls script to generate cloud build files for each bq views/functions.
#   2. Run those cloud builds.

echo "🦄🦄🦄 Deploying SFDC Reporting 🔪🔪🔪"

set -e

echo "Starting build file generation"

# Set various directories that help navigate to various things.
THIS_DIR=$(dirname "$0")
SRC_DIR="${THIS_DIR}"/../../src
REPORTING_DIR="${SRC_DIR}"/reporting

echo "THIS_DIR = ${THIS_DIR}"
echo "SRC_DIR = ${SRC_DIR}"
echo "REPORTING_DIR = ${REPORTING_DIR}"

export PYTHONPATH=$PYTHONPATH:"${SRC_DIR}"

# Generate Actual Build files that will create reporting BQ views.
python3 "$THIS_DIR"/generate_build_files.py
echo "Build files generated successfully."

# We may have one or more build files. Let's run all of them.
set +e
failure=0
for build_file_name in "${REPORTING_DIR}"/generated_reporting_files/*.yaml; do
  [[ -e "$build_file_name" ]] || break
  # It's important to make just REPORTING_DIR available to cloud build, and
  # make it the root directory for the build. Code executed by the build file
  # uses paths relative to the reporting directory.
  echo -e "gcloud builds submit ${REPORTING_DIR} --config=\"${build_file_name}\" --substitutions _GCS_LOGS_BUCKET=\"$1\""
  gcloud builds submit "${REPORTING_DIR}" --config="${build_file_name}" --substitutions _GCS_LOGS_BUCKET="$1"
  # shellcheck disable=SC2181
  if [ $? -ne 0 ]; then
    failure=1
  fi
done
set -e

if [ "${failure}" -eq "0" ]; then
  echo "✅ 🦄🦄🦄 SFDC Reporting - DONE! 😺😺😺"
else
  echo "🛑 SFDC Reporting Failed! Please check logs!"
  exit 1
fi
