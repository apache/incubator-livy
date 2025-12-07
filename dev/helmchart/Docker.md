## Steps to Create Docker Image for Spark with Python Binding

### 1. Download Spark Binary

Use the following command to download and extract the Spark binary:

```sh
wget https://archive.apache.org/dist/spark/spark-3.2.3/spark-3.2.3-bin-hadoop3.2.tgz
tar -xzf spark-3.2.3-bin-hadoop3.2.tgz
```

### 2. Build and Push Docker Image

Use the following commands to build and push the Docker image:

```sh
./spark-3.2.3-bin-hadoop3.2/bin/docker-image-tool.sh -r <your_repository> -t v3.2.3 -p kubernetes/dockerfiles/spark/bindings/python/Dockerfile build
./spark-3.2.3-bin-hadoop3.2/bin/docker-image-tool.sh -r <your_repository> -t v3.2.3 -p kubernetes/dockerfiles/spark/bindings/python/Dockerfile push
```

Replace `<your_repository>` with your Docker repository name.

## Steps to Create Docker Image for Livy

### 1. Build Livy Code

Build the Livy code using the following commands:

```sh
cd incubator-livy
mvn -Pthriftserver -Pscala-2.12 -Pspark3 package
```

Copy the generated Livy binary into the `/tmp` directory:

```sh
cp assembly/target/apache-livy-0.9.0-incubating-SNAPSHOT_2.12-bin.zip /tmp
```

### 2. Create Dockerfile

Create a `Dockerfile` in the `/tmp` directory with the following content:

```Dockerfile
FROM <your_repository>/spark-py:v3.2.3
ENV LIVY_VERSION            0.9.0-incubating-SNAPSHOT
ENV LIVY_PACKAGE            apache-livy-${LIVY_VERSION}_2.12-bin
ENV LIVY_HOME               /opt/livy
ENV LIVY_CONF_DIR           /conf
ENV PATH                    $PATH:$LIVY_HOME/bin

USER root

COPY $LIVY_PACKAGE.zip /
RUN apt-get update && apt-get install -y unzip && \
    unzip /$LIVY_PACKAGE.zip -d / && \
    mv /$LIVY_PACKAGE /opt/ && \
    rm -rf $LIVY_HOME && \
    ln -s /opt/$LIVY_PACKAGE $LIVY_HOME && \
    rm -f /$LIVY_PACKAGE.zip

RUN mkdir /var/log/livy  && \
    ln -s /var/log/livy $LIVY_HOME/logs

WORKDIR $LIVY_HOME

ENTRYPOINT [ "livy-server" ]
```

Replace `<your_repository>` with your Docker repository name.

### 3. Build and Push Docker Image

Use the following commands to build and push the Docker image:

```sh
cd /tmp
docker build -t <your_repository>/livy:spark3.2.3 .
docker push <your_repository>/livy:spark3.2.3
rm -f apache-livy-0.9.0-incubating-SNAPSHOT_2.12-bin.zip
```
