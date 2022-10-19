#!/usr/bin/env bash
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
# Builds Docker image for livy
set -e
if [[ -z "$SPARK_VERSION" ]]; then
  if [[ -z "$1" ]]; then
    echo "Please provide spark version as first argument"
  else
    SPARK_VERSION=$1
  fi
fi
if [[ "$SPARK_VERSION" =~ ^2.* ]]; then
  SPARK_VERSION="2.4.5"
  # option to overwrite scala version from command line
  if [[ -z "$SCALA_VERSION" ]]; then
    SCALA_VERSION="2.11"
  fi
  MAVEN_ARGS=""
  IMAGE_SPARK_SUFFIX="spark2"
else
  SPARK_VERSION="3.0.0"
  SCALA_VERSION="2.12"
  MAVEN_ARGS="-Pspark-3.0"
  IMAGE_SPARK_SUFFIX="spark3"
fi
echo "Using spark version $SPARK_VERSION. Scala version $SCALA_VERSION MAVEN_ARGS: $MAVEN_ARGS"
# todo add support for taking in LIVY_VERSION
if [[ -z "$LIVY_VERSION" ]]; then
  echo "livy version: $LIVY_VERSION"
  LIVY_VERSION=$(grep '<version>' pom.xml | head -n 1 | awk -F '>' '{print $2}' | awk -F '<' '{print $1}')
else
  echo "setting version in poms: $LIVY_VERSION"
  LIVY_VERSION_OLD=$(grep '<version>' pom.xml | head -n 1 | awk -F '>' '{print $2}' | awk -F '<' '{print $1}')
  mvn versions:set $MAVEN_ARGS -DnewVersion="$LIVY_VERSION"
fi
mvn clean package install -B -V -e $MAVEN_ARGS -Pthriftserver -DskipTests -Dmaven.javadoc.skip=true
rm -rf ./apache-livy*zip
cp "assembly/target/apache-livy-${LIVY_VERSION}-bin.zip" ./
IMAGE=133450206866.dkr.ecr.us-west-1.amazonaws.com/livy:v${LIVY_VERSION}-${IMAGE_SPARK_SUFFIX}
docker build -t "$IMAGE" . --build-arg LIVY_VERSION="$LIVY_VERSION" --build-arg SPARK_VERSION=$SPARK_VERSION
docker push "$IMAGE"
if [[ -n "$LIVY_VERSION_OLD" ]]; then
  echo "resetting version in poms: $LIVY_VERSION_OLD"
  mvn versions:set -DnewVersion="$LIVY_VERSION_OLD"
fi