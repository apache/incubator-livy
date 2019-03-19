---
layout: page
title: Livy Docs - Programmatic API
tagline: Programmatic API
---
<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->

{% include JB/setup %}

## Using the Programmatic API

Livy provides a programmatic Java/Scala and Python API that allows applications to run code inside
Spark without having to maintain a local Spark context. Here shows how to use the Java API.

Add the Livy client dependency to your application's POM:

```xml
<dependency>
  <groupId>org.apache.livy</groupId>
  <artifactId>livy-client-http</artifactId>
  <version>0.6.0-incubating</version>
</dependency>
```

To be able to compile code that uses Spark APIs, also add the correspondent Spark dependencies.

To run Spark jobs within your applications, extend ``org.apache.livy.Job`` and implement
the functionality you need. Here's an example job that calculates an approximate value for Pi:

```java
import java.util.*;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import org.apache.livy.*;

public class PiJob implements Job<Double>, Function<Integer, Integer>,
  Function2<Integer, Integer, Integer> {

  private final int samples;

  public PiJob(int samples) {
    this.samples = samples;
  }

  @Override
  public Double call(JobContext ctx) throws Exception {
    List<Integer> sampleList = new ArrayList<Integer>();
    for (int i = 0; i < samples; i++) {
      sampleList.add(i + 1);
    }

    return 4.0d * ctx.sc().parallelize(sampleList).map(this).reduce(this) / samples;
  }

  @Override
  public Integer call(Integer v1) {
    double x = Math.random();
    double y = Math.random();
    return (x*x + y*y < 1) ? 1 : 0;
  }

  @Override
  public Integer call(Integer v1, Integer v2) {
    return v1 + v2;
  }

}
```

To submit this code using Livy, create a LivyClient instance and upload your application code to
the Spark context. Here's an example of code that submits the above job and prints the computed
value:

```java
LivyClient client = new LivyClientBuilder()
  .setURI(new URI(livyUrl))
  .build();

try {
  System.err.printf("Uploading %s to the Spark context...\n", piJar);
  client.uploadJar(new File(piJar)).get();

  System.err.printf("Running PiJob with %d samples...\n", samples);
  double pi = client.submit(new PiJob(samples)).get();

  System.out.println("Pi is roughly: " + pi);
} finally {
  client.stop(true);
}
```

To learn about all the functionality available to applications, read the javadoc documentation for
the classes under the ``api`` module.
