FROM ubuntu:xenial

# configure locale
RUN apt update -qq > /dev/null && apt install -qq --yes --no-install-recommends \
    locales && \
    locale-gen en_US.UTF-8
ENV LANG="en_US.UTF-8" \
    LANGUAGE="en_US.UTF-8" \
    LC_ALL="en_US.UTF-8"

# Install necessary dependencies for build/test
RUN apt update && \
    apt install -y \
    apt-transport-https \
    libkrb5-dev \
    maven \
    openjdk-8-jdk \
    python-dev \
    python-pip \
    python3-pip \
    software-properties-common

# R 3.x install - ensure to add the signing key per https://cran.r-project.org/bin/linux/ubuntu/olderreleasesREADME.html
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu xenial-cran35/' && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    apt-get update && \
    apt-get -y install r-base

# Add build dependencies for python2
# - First we upgrade pip because that makes a lot of things better
# - Then we remove the provided version of setuptools and install a different version
# - Then we install additional dependencies
RUN python -m pip install -U "pip < 21.0"
RUN apt-get remove -y python-setuptools
RUN python -m pip install "setuptools < 36"
RUN python -m pip install "requests >= 2.10.0" "responses >= 0.5.1" "futures>=3.0.5" "future>=0.15.2" pytest pytest-runner flaky flake8 requests-kerberos install codecov cloudpickle

# Now do the same for python3
RUN python3 -m pip install -U pip

WORKDIR /workspace
# https://archive.apache.org/dist/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz