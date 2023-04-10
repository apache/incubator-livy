# Set the base image to Ubuntu
FROM ubuntu:latest

# Install dependencies
RUN apt-get update && \
    apt-get install -y openjdk-8-jdk wget curl unzip && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV SPARK_VERSION=2.4.5
ENV HADOOP_VERSION=2.7
ENV LIVY_VERSION=0.7.1-incubating

# Download and install Spark
RUN set -eux; \
    url="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"; \
    filename="$(basename "$url")"; \
    wget --progress=bar:force:noscroll -O "$filename" "$url"; \
    tar xzf "$filename" -C /opt/; \
    rm "$filename"; \
    mv "/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}" /opt/spark

# Download and install Livy
RUN set -eux; \
    url="https://archive.apache.org/dist/incubator/livy/${LIVY_VERSION}/apache-livy-${LIVY_VERSION}-bin.zip"; \
    filename="$(basename "$url")"; \
    wget --progress=bar:force:noscroll -O "$filename" "$url"; \
    unzip "$filename"; \
    rm "$filename"; \
    mv "apache-livy-${LIVY_VERSION}-bin" /opt/livy

# Configure Livy
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
RUN cp /opt/livy/conf/log4j.properties.template /opt/livy/conf/log4j.properties
RUN mkdir -p /opt/livy/logs
RUN apt-get update && \
    apt-get install -y scala
ENV PATH="/opt/scala/bin:${PATH}"

COPY log4j.properties /opt/livy/conf/
RUN chmod 644 /opt/livy/conf/log4j.properties

# Expose ports
EXPOSE 8998 5005
# Start Livy
CMD ["/opt/livy/bin/livy-server"]
