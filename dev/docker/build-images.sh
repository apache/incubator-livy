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

# Fail if there is an error
set -e
APACHE_ARCHIVE_ROOT=http://archive.apache.org/dist
HADOOP_VERSION=2.7.7
HADOOP_PACKAGE="hadoop-${HADOOP_VERSION}.tar.gz"
SPARK_VERSION=2.4.5
SPARK_PACKAGE="spark-2.4.5-bin-hadoop2.7"
LIVY_VERSION=0.8.0-incubating
LIVY_PACKAGE="apache-livy-${LIVY_VERSION}-bin.zip"

# Download hadoop if needed
if [ ! -f "livy-dev-spark/${HADOOP_PACKAGE}" ]; then
    curl -sL --retry 3 -o "livy-dev-spark/${HADOOP_PACKAGE}" \
      "${APACHE_ARCHIVE_ROOT}/hadoop/common/hadoop-${HADOOP_VERSION}/${HADOOP_PACKAGE}" 
fi

# Download spark if needed
if [ ! -f "livy-dev-spark/${SPARK_PACKAGE}" ]; then
    curl -sL --retry 3 -o "livy-dev-spark/${SPARK_PACKAGE}" \
      "${APACHE_ARCHIVE_ROOT}/spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}"
fi

# Download livy if needed
if [ ! -f "livy-dev-server/${LIVY_PACKAGE}" ]; then
    curl -sL --retry 3 -o "livy-dev-server/${LIVY_PACKAGE}" \
      "${APACHE_ARCHIVE_ROOT}/incubator/livy/${LIVY_VERSION}/${LIVY_PACKAGE}"
fi 


# podman build -t livy-dev-base livy-dev-base/
# podman build -t livy-dev-spark livy-dev-spark/ --build-arg HADOOP_VERSION=${HADOOP_VERSION} --build-arg SPARK_VERSION=${SPARK_VERSION}
# podman build -t livy-dev-server livy-dev-server/ --build-arg LIVY_VERSION=${LIVY_VERSION}
