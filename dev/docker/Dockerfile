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

FROM openjdk:11-jdk-bullseye

ARG SPARK_VERSION=3.3.2
ARG HADOOP_VERSION=3
ARG APACHE_LIVY=0.7.1

# configure locale
RUN apt-get update -qq > /dev/null && apt-get install -qq --yes --no-install-recommends \
    locales && \
    locale-gen en_US.UTF-8
ENV LANG="en_US.UTF-8" \
    LANGUAGE="en_US.UTF-8" \
    LC_ALL="en_US.UTF-8"

# Install necessary dependencies for build/test
RUN apt-get install -qq \
    apt-transport-https \
    curl \
    git \
    libkrb5-dev \
    maven \
    python3-dev \
    python3-setuptools \
    python3-pip \
    software-properties-common \
    wget \
    unzip

# R 3.x install - ensure to add the signing key per https://cran.r-project.org/bin/linux/ubuntu/olderreleasesREADME.html
RUN apt-get update && \
    apt-get -qq install -y r-base

# Now do the same for python3
RUN python3 -m pip install -U pip
RUN python3 -m pip install \
        cloudpickle \
        codecov \
        flake8 \
        flaky \
        "future>=0.15.2" \
        "futures>=3.0.5" \
        pytest \
        pytest-runner \
        requests-kerberos \
        "requests >= 2.10.0" \
        "responses >= 0.5.1"
# Install spark
RUN mkdir -p /opt/spark &&  mkdir -p /opt/livy
RUN curl -L -o /opt/spark.tgz http://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xvzf /opt/spark.tgz -C /opt/spark --strip-components 1
RUN export SPARK_HOME=/opt/spark

RUN curl -L -o /opt/livy.zip https://dlcdn.apache.org/incubator/livy/${APACHE_LIVY}-incubating/apache-livy-${APACHE_LIVY}-incubating-bin.zip && \
    unzip  -d /opt/livy /opt/livy.zip \
    mv /opt/livy/apache-livy-${APACHE_LIVY}-incubating-bin /opt/livy/${APACHE_LIVY}

COPY /opt/livy/${APACHE_LIVY}/conf/livy.conf.template /opt/livy/${APACHE_LIVY}/conf/livy.conf
COPY /opt/livy/${APACHE_LIVY}/conf/livy-env.conf.template /opt/livy/${APACHE_LIVY}/conf/livy-env.conf
COPY /opt/livy/${APACHE_LIVY}/conf/spark-blacklist.conf.template /opt/livy/${APACHE_LIVY}/conf/spark-blacklist.conf
COPY /opt/livy/${APACHE_LIVY}/conf/livy-client.conf.template /opt/livy/${APACHE_LIVY}/conf/livy-client.conf
COPY /opt/livy/${APACHE_LIVY}/conf/log4j.properties.template /opt/livy/${APACHE_LIVY}/conf/log4j.properties
# Install Livy
WORKDIR /workspace
# https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz
ENTRYPOINT ['/opt/livy/${APACHE_LIVY}/bin/livy-server', 'start']