FROM ubuntu:xenial as builder

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
    libkrb5-dev \
    maven \
    openjdk-8-jdk \
    python-dev \
    python-pip \
    python3-pip \
    software-properties-common \
    wget

# R 3.x install - ensure to add the signing key per https://cran.r-project.org/bin/linux/ubuntu/olderreleasesREADME.html
RUN add-apt-repository 'deb https://cloud.r-project.org/bin/linux/ubuntu xenial-cran35/' && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys E298A3A825C0D65DFD57CBB651716619E084DAB9 && \
    apt-get update && \
    apt-get -qq install r-base

# Add build dependencies for python2
# - First we upgrade pip because that makes a lot of things better
# - Then we remove the provided version of setuptools and install a different version
# - Then we install additional dependencies
RUN python -m pip install -U "pip < 21.0" && \
	apt-get remove -y python-setuptools && \
	python -m pip install "setuptools < 36" && \
	python -m pip install \
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
RUN python3 -m pip install -U pip

ADD . /workspace
WORKDIR /workspace

RUN mvn clean package  -Pspark-3.0 -DskipTests -DskipITs -Dmaven.javadoc.skip=true

RUN mkdir livy/jars/ && \
    mv /workspace/server/target/jars/* livy/jars/  && \
    mkdir livy/repl_2.12-jars/ && \
    mv /workspace/repl/scala-2.12/target/jars/* livy/repl_2.12-jars/  && \
    mkdir livy/rsc-jars/ && \
    mv /workspace/rsc/target/jars/* livy/rsc-jars/  && \
    mkdir livy/logs/

RUN tar -czvf /tmp/livy33-k8s.tar.gz livy

#
FROM ubuntu:focal-20220426

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-8-jre-headless \
    openjdk-17-jre-headless ca-certificates-java tini wget curl && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash livy

ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV PATH=/opt/spark/bin:$PATH
ENV SPARK_CONF_DIR=/opt/spark/conf
ENV SPARK_HOME=/opt/spark

ENV LIVY_HOME=/opt/livy
ENV PORT 8998

WORKDIR /opt/
COPY --from=builder /tmp/livy33-k8s.tar.gz /opt/
RUN tar -xzf /opt/livy33-k8s.tar.gz; rm /opt/livy33-k8s.tar.gz

RUN wget https://archive.apache.org/dist/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
RUN tar -xzf /opt/spark-3.3.0-bin-hadoop3.tgz;ln -s /opt/spark-3.3.0-bin-hadoop3 /opt/spark; rm /opt/spark-3.3.0-bin-hadoop3.tgz

ADD entrypoint.sh /entrypoint.sh
RUN chmod a+x /entrypoint.sh  && \
    chmod +x /opt/livy/bin/livy-server
RUN chown -R livy:root /opt/

EXPOSE ${PORT}

ENTRYPOINT ["/entrypoint.sh"]