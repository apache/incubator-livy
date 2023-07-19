# Livy with standalone Spark Cluster
## Pre-requisite
Following steps use Ubuntu as development environment but most of the instructions can be modified to fit another OS as well.
* Install wsl if on windows, instructions available [here](https://ubuntu.com/tutorials/install-ubuntu-on-wsl2-on-windows-11-with-gui-support)
* Install docker engine, instructions available [here](https://docs.docker.com/engine/install/ubuntu/)
* Install docker-compose, instructions available [here](https://docs.docker.com/compose/install/)

## Standalone cluster using docker-compose
### Build the current livy branch and copy it to appropriate folder
```
$ mvn clean package -Pscala-2.12 -Pspark3 -DskipITs -DskipTests
```

This generates a zip file for livy similar to `assembly/target/apache-livy-0.8.0-incubating_2.12-bin.zip`. It's useful to use the `clean` target to avoid mixing with previously built dependencies/versions.

### Build container images locally
* Build livy-dev-base, livy-dev-spark, livy-dev-server container images using provided script `build-images.sh`
```
livy/dev/docker$ ls
README.md  build-images.sh  livy-dev-base  livy-dev-cluster  livy-dev-server  livy-dev-spark
livy/dev/docker$ ./build-images.sh
```
#### Customizing container images
`build-images.sh` downloads built up artifacts from Apache's respository however, private builds can be copied to respective container directories to build a container image with private artifacts as well.
```
livy-dev-spark uses hadoop and spark tarballs
livy-dev-server uses livy zip file
```

For quicker iteration, copy the modified jars to specific container directories and update corresponding `Dockerfile` to replace those jars as additional steps inside the image. Provided `Dockerfile`s have example lines that can be uncommented/modified to achieve this.

`livy-dev-cluster` folder contains conf folder with customizable configurations (environment, .conf and log4j.properties files) that can be updated to suit specific needs. Restart the cluster after making changes (without rebuilding the images).
### Launching the cluster
```
livy/dev/docker/livy-dev-cluster$ docker-compose up
Starting spark-master   ... done
Starting spark-worker-1 ... done
Starting livy           ... done
Attaching to spark-worker-1, spark-master, livy
```
### UIs
* Livy UI at http://localhost:8998/
* Spark Master at spark://master:7077 (http://localhost:8080/).
* Spark Worker at spark://spark-worker-1:8881 (http://localhost:8081/)

### Run spark shell
* Login to spark-master or spark-worker or livy container using docker cli
```
$ docker exec -it spark-master /bin/bash
root@master:/opt/spark-3.2.3-bin-without-hadoop# spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
2023-01-27 19:32:37,469 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://localhost:4040
Spark context available as 'sc' (master = spark://master:7077, app id = app-20230127193238-0002).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.2.3
      /_/

Using Scala version 2.12.15 (OpenJDK 64-Bit Server VM, Java 1.8.0_292)
Type in expressions to have them evaluated.
Type :help for more information.

scala> println("Hello world!")
Hello world!
``` 
### Submit requests to livy using REST apis
Login to livy container directly and submit requests using REST endpoint
```
# Create a new session
curl -s -X POST -d '{"kind": "spark","driverMemory":"512M","executorMemory":"512M"}' -H "Content-Type: application/json" http://localhost:8998/sessions/ | jq

# Check session state
curl -s -X GET -H "Content-Type: application/json" http://localhost:8998/sessions/ | jq -r '.sessions[] | [ .id, .state ] | @tsv'

# Submit the simplest `1+1` statement
curl -s -X POST -d '{"code": "1 + 1"}' -H "Content-Type: application/json" http://localhost:8998/sessions/0/statements | jq

# Check for statement status
curl -s -X GET -H "Content-Type: application/json" http://localhost:8998/sessions/0/statements | jq -r '.statements[] | [ .id,.state,.progress,.output.status,.code ] | @tsv'

# Submit simple spark code
curl -s -X POST -d '{"code": "val data = Array(1,2,3); sc.parallelize(data).count"}' -H "Content-Type: application/json" http://localhost:8998/sessions/0/statements | jq

# Check for statement status
curl -s -X GET -H "Content-Type: application/json" http://localhost:8998/sessions/0/statements | jq -r '.statements[] | [ .id,.state,.progress,.output.status,.code ] | @tsv'

# Submit simple sql code (this setup still doesn't have hive metastore configured)
curl -X POST -d '{"kind": "sql", "code": "show databases"}, ' -H "Content-Type: application/json" http://localhost:8998/sessions/0/statements | jq

# Check for statement status
curl -s -X GET -H "Content-Type: application/json" http://localhost:8998/sessions/0/statements | jq -r '.statements[] | [ .id,.state,.progress,.output.status,.code ] | @tsv'
```
### Debugging Livy/Spark/Hadoop
`livy-dev-cluster` has conf directory for spark-master, spark-worker and livy. Configuration files in those directories can be modified before launching the cluster, for example:
1. `Setting log level` - log4j.properties file can be modified in `livy-dev-cluster` folder to change log level for root logger as well as for specific packages
2. `Testing private changes` - copy private jars into respective container folder, update corresponding Dockerfile to copy/replace those jars into respective paths on the container image and rebuild all the images (Note: livy-dev-server builds on top of livy-dev-spark which builds on top of livy-dev-base)
3. `Remote debugging` - livy-env.sh already has customization to start with remote debugging on 9010. Please follow IDE specific guidance on how to debug remotely connecting to specific JDWP port for the daemon. Instructions for IntelliJ are available [here](https://www.jetbrains.com/help/idea/tutorial-remote-debug.html) and for Eclipse, [here](https://help.eclipse.org/latest/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Ftasks%2Ftask-remotejava_launch_config.htm)
### Terminate the cluster
Press `CTRL-C` to terminate
```
spark-worker-1    | 2023-01-27 19:16:47,921 INFO shuffle.ExternalShuffleBlockResolver: Application app-20230127191546-0000 removed, cleanupLocalDirs = true
^CGracefully stopping... (press Ctrl+C again to force)
Stopping spark-worker-1 ... done
Stopping spark-master   ... done
```

## Common Gotchas
1. Use `docker-compose down` to clean up all the resources created for the cluster
2. Login to created images to check the state
```
docker run -it [imageId | imageName] /bin/bash
```
