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

FROM livy-dev-spark:latest

ARG LIVY_VERSION=0.8.0-incubating-SNAPSHOT
ARG ROOT_PATH=/opt

RUN apt-get update \
    && apt-get install -y unzip

ENV LIVY_HOME=${ROOT_PATH}/livy
ENV LIVY_PACKAGE=apache-livy-${LIVY_VERSION}-bin

COPY ${LIVY_PACKAGE}.zip ${LIVY_PACKAGE}.zip

RUN unzip ${LIVY_PACKAGE}.zip 1>/dev/null \
  && mv ${LIVY_PACKAGE} ${ROOT_PATH}/ \
  && ln -s ${ROOT_PATH}/${LIVY_PACKAGE} ${LIVY_HOME} \
  && chown -R root:root ${LIVY_HOME} \
  && rm ${LIVY_PACKAGE}.zip

# Uncomment following line or add more such lines to replace the default jars with private builds.
# COPY livy-core_2.12-0.8.0-incubating-SNAPSHOT.jar ${SPARK_HOME}/repl_2.12-jars/livy-core_2.12-0.8.0-incubating-SNAPSHOT.jar

WORKDIR ${LIVY_HOME}
