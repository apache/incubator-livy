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
FROM livy-dev-base:latest

ARG HADOOP_VERSION=3.3.1
ARG SPARK_VERSION=3.2.3
ARG ROOT_PATH=/opt

RUN mkdir -p ${ROOT_PATH}

ENV HADOOP_HOME=${ROOT_PATH}/hadoop
ENV HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
ENV PATH=${PATH}:${HADOOP_HOME}/bin
ENV HADOOP_PACKAGE=hadoop-${HADOOP_VERSION}

COPY ${HADOOP_PACKAGE}.tar.gz ${HADOOP_PACKAGE}.tar.gz

RUN gunzip ${HADOOP_PACKAGE}.tar.gz \
 && tar -xf ${HADOOP_PACKAGE}.tar -C ${ROOT_PATH}/ \
 && ln -s ${ROOT_PATH}/${HADOOP_PACKAGE} ${HADOOP_HOME} \
 && rm -rf ${HADOOP_HOME}/share/doc \
 && chown -R root:root ${HADOOP_HOME} \
 && rm ${HADOOP_PACKAGE}.tar

ENV SPARK_HOME=${ROOT_PATH}/spark
ENV SPARK_DIST_CLASSPATH="${HADOOP_HOME}/etc/hadoop/*:${HADOOP_HOME}/share/hadoop/common/lib/*:${HADOOP_HOME}/share/hadoop/common/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/hdfs/lib/*:${HADOOP_HOME}/share/hadoop/hdfs/*:${HADOOP_HOME}/share/hadoop/yarn/lib/*:${HADOOP_HOME}/share/hadoop/yarn/*:${HADOOP_HOME}/share/hadoop/mapreduce/lib/*:${HADOOP_HOME}/share/hadoop/mapreduce/*:${HADOOP_HOME}/share/hadoop/tools/lib/*"
ENV PATH=${PATH}:${SPARK_HOME}/bin
ENV SPARK_PACKAGE=spark-${SPARK_VERSION}-bin-without-hadoop

COPY ${SPARK_PACKAGE}.tgz ${SPARK_PACKAGE}.tgz

RUN gunzip ${SPARK_PACKAGE}.tgz \
 && tar -xf ${SPARK_PACKAGE}.tar -C ${ROOT_PATH}/ \
 && ln -s ${ROOT_PATH}/${SPARK_PACKAGE} ${SPARK_HOME} \
 && chown -R root:root ${SPARK_HOME} \
 && rm ${SPARK_PACKAGE}.tar

# Uncomment following line or add more such lines to replace the default jars with private builds.
# COPY hadoop-streaming-3.3.1.jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-3.3.1.jar

# Uncomment following line or add more such lines to replace the default jars with private builds.
# COPY spark-repl_2.12-3.2.3.jar ${SPARK_HOME}/jars/spark-repl_2.12-3.2.3.jar

WORKDIR ${SPARK_HOME}
