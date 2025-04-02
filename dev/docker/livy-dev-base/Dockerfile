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

FROM ubuntu:noble

# configure locale
RUN apt-get update -qq > /dev/null && apt-get install -qq --yes --no-install-recommends \
    locales && \
    locale-gen en_US.UTF-8
ENV LANG="en_US.UTF-8" \
    LANGUAGE="en_US.UTF-8" \
    LC_ALL="en_US.UTF-8" \
    TZ=US \
    DEBIAN_FRONTEND=noninteractive

# Install necessary dependencies for build/test/debug
# Use `lsof -i -P -n` to find open ports
RUN apt-get install -qq \
    apt-transport-https \
    curl \
    git \
    libkrb5-dev \
    openjdk-8-jdk-headless \
    r-base \
    maven \
    software-properties-common \
    vim \
    wget \
    telnet \
    lsof

# This fixes integration tests
# If setsid is available, signals are sent to containers in MiniYarnCluster using negative pids, however
# in the Docker container this results in a kill(0) system call which triggers an ExitCodeException in
# the kill function that breaks test execution. If setsid is removed, pids < 0 are not used.
# See https://github.com/apache/hadoop/blob/rel/release-2.7.3/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/util/Shell.java#L238
RUN rm /usr/bin/setsid

# python build
RUN apt-get install -y \
  build-essential \
  libbz2-dev \
  libffi-dev \
  liblzma-dev \
  libncurses-dev \
  libreadline-dev \
  libsqlite3-dev \
  libssl-dev \
  zlib1g-dev

ENV HOME=/root

RUN git clone https://github.com/pyenv/pyenv.git $HOME/pyenv

ENV PYENV_ROOT=$HOME/pyenv
ENV PATH="$HOME/pyenv/shims:$HOME/pyenv/bin:$HOME/bin:$PATH"

RUN pyenv install -v 2.7.18 && \
  pyenv install -v 3.9.21 && \
  pyenv global 2.7.18 3.9.21 && \
  pyenv rehash

# Add build dependencies for python2
# - First we upgrade pip because that makes a lot of things better
# - Then we remove the provided version of setuptools and install a different version
# - Then we install additional dependencies
RUN python2 -m pip install -U "pip < 21.0" && \
        apt-get remove -y python-setuptools && \
        python2 -m pip install "setuptools < 36" && \
        python2 -m pip install \
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

# Now do the same for python3
RUN python3 -m pip install -U pip && pip3 install \
        cloudpickle \
        codecov \
        flake8 \
        flaky \
        pytest \
        pytest-runner \
        requests-kerberos \
        requests \
        responses

RUN pyenv rehash

RUN apt remove -y openjdk-11-jre-headless

WORKDIR /workspace

