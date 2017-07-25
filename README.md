# Apache Livy

[![Build Status](https://travis-ci.org/apache/incubator-livy.svg?branch=master)](https://travis-ci.org/apache/incubator-livy)

Apache Livy is an open source REST interface for interacting with
[Apache Spark](http://spark.apache.org) from anywhere. It supports executing snippets of code or
programs in a Spark context that runs locally or in
[Apache Hadoop YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html).

* Interactive Scala, Python and R shells
* Batch submissions in Scala, Java, Python
* Multiple users can share the same server (impersonation support)
* Can be used for submitting jobs from anywhere with REST
* Does not require any code change to your programs

[Pull requests](https://github.com/apache/incubator-livy/pulls) are welcomed! But before you begin,
please check out the [Contributing](http://livy.incubator.apache.org/community#Contributing)
section on the [Community](http://livy.incubator.apache.org/community) page of our website.

## Online Documentation

Guides and documentation on getting started using Livy, example code snippets, and Livy API
documentation can be found at [livy.incubator.apache.org](http://livy.incubator.apache.org).

## Before Building Livy

To build Livy, you will need:

Debian/Ubuntu:
  * mvn (from ``maven`` package or maven3 tarball)
  * openjdk-7-jdk (or Oracle Java7 jdk)
  * Python 2.6+
  * R 3.x

Redhat/CentOS:
  * mvn (from ``maven`` package or maven3 tarball)
  * java-1.7.0-openjdk (or Oracle Java7 jdk)
  * Python 2.6+
  * R 3.x

MacOS:
  * Xcode command line tools
  * Oracle's JDK 1.7+
  * Maven (Homebrew)
  * Python 2.6+
  * R 3.x

Required python packages for building Livy:
  * cloudpickle
  * requests
  * requests-kerberos
  * flake8
  * flaky
  * pytest


To run Livy, you will also need a Spark installation. You can get Spark releases at
https://spark.apache.org/downloads.html.

Livy requires at least Spark 1.6 and supports both Scala 2.10 and 2.11 builds of Spark, Livy
will automatically pick repl dependencies through detecting the Scala version of Spark.

Livy also supports Spark 2.0+ for both interactive and batch submission, you could seamlessly
switch to different versions of Spark through ``SPARK_HOME`` configuration, without needing to
rebuild Livy.


## Building Livy

Livy is built using [Apache Maven](http://maven.apache.org). To check out and build Livy, run:

```
git clone https://github.com/apache/incubator-livy.git
cd livy
mvn package
```

By default Livy is built against Apache Spark 1.6.2, but the version of Spark used when running
Livy does not need to match the version used to build Livy. Livy internally uses reflection to
mitigate the gaps between different Spark versions, also Livy package itself does not
contain a Spark distribution, so it will work with any supported version of Spark (Spark 1.6+)
without needing to rebuild against specific version of Spark.
