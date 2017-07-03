#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e

if [ -z "$TRAVIS_JOB_NUMBER" ]; then
  echo "TRAVIS_JOB_NUMBER isn't defined."
  exit 1
fi

if [ -z "$LOG_AZURE_STORAGE_CONNECTION_STRING" ]; then
  echo "LOG_AZURE_STORAGE_CONNECTION_STRING isn't defined."
  exit 1
fi

export AZURE_STORAGE_CONNECTION_STRING="$LOG_AZURE_STORAGE_CONNECTION_STRING"

LOG_ZIP_NAME="$TRAVIS_JOB_NUMBER.zip"
LOG_ZIP_PATH="assembly/target/$LOG_ZIP_NAME"

find * -name "*.log" -o -name "stderr" -o -name "stdout" | zip -@ $LOG_ZIP_PATH

azure telemetry --disable
azure storage blob upload -q $LOG_ZIP_PATH buildlogs $LOG_ZIP_NAME && echo "===== Build log uploaded to https://livy.blob.core.windows.net/buildlogs/$LOG_ZIP_NAME ====="
