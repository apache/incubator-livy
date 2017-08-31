/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.rsc.driver;

import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkEntries {

  private static final Logger LOG = LoggerFactory.getLogger(SparkEntries.class);

  private volatile JavaSparkContext sc;
  private final SparkConf conf;
  private volatile SQLContext sqlctx;
  private volatile HiveContext hivectx;
  private volatile Object sparksession;

  public SparkEntries(SparkConf conf) {
    this.conf = conf;
  }

  public JavaSparkContext sc() {
    if (sc == null) {
      synchronized (this) {
        if (sc == null) {
          long t1 = System.nanoTime();
          LOG.info("Starting Spark context...");
          SparkContext scalaSc = SparkContext.getOrCreate(conf);
          sc = new JavaSparkContext(scalaSc);
          LOG.info("Spark context finished initialization in {}ms",
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t1));
        }
      }
    }
    return sc;
  }

  @SuppressWarnings("unchecked")
  public Object sparkSession() throws Exception {
    if (sparksession == null) {
      synchronized (this) {
        if (sparksession == null) {
          try {
            Class<?> clz = Class.forName("org.apache.spark.sql.SparkSession$");
            Object spark = clz.getField("MODULE$").get(null);
            Method m = clz.getMethod("builder");
            Object builder = m.invoke(spark);
            builder.getClass().getMethod("sparkContext", SparkContext.class)
              .invoke(builder, sc().sc());

            SparkConf conf = sc().getConf();
            if (conf.get("spark.sql.catalogImplementation", "in-memory").toLowerCase()
              .equals("hive")) {
              if ((boolean) clz.getMethod("hiveClassesArePresent").invoke(spark)) {
                ClassLoader loader = Thread.currentThread().getContextClassLoader() != null ?
                  Thread.currentThread().getContextClassLoader() : getClass().getClassLoader();
                if (loader.getResource("hive-site.xml") == null) {
                  LOG.warn("livy.repl.enable-hive-context is true but no hive-site.xml found on " +
                   "classpath");
                }

                builder.getClass().getMethod("enableHiveSupport").invoke(builder);
                sparksession = builder.getClass().getMethod("getOrCreate").invoke(builder);
                LOG.info("Created Spark session (with Hive support).");
              } else {
                builder.getClass().getMethod("config", String.class, String.class)
                  .invoke(builder, "spark.sql.catalogImplementation", "in-memory");
                sparksession = builder.getClass().getMethod("getOrCreate").invoke(builder);
                LOG.info("Created Spark session.");
              }
            } else {
              sparksession = builder.getClass().getMethod("getOrCreate").invoke(builder);
              LOG.info("Created Spark session.");
            }
          } catch (Exception e) {
            LOG.warn("SparkSession is not supported", e);
            throw e;
          }
        }
      }
    }

    return sparksession;
  }

  public SQLContext sqlctx() {
    if (sqlctx == null) {
      synchronized (this) {
        if (sqlctx == null) {
          sqlctx = new SQLContext(sc());
          LOG.info("Created SQLContext.");
        }
      }
    }
    return sqlctx;
  }

  public HiveContext hivectx() {
    if (hivectx == null) {
      synchronized (this) {
        if (hivectx == null) {
          SparkConf conf = sc.getConf();
          if (conf.getBoolean("spark.repl.enableHiveContext", false) ||
            conf.get("spark.sql.catalogImplementation", "in-memory").toLowerCase()
              .equals("hive")) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader() != null ?
              Thread.currentThread().getContextClassLoader() : getClass().getClassLoader();
            if (loader.getResource("hive-site.xml") == null) {
              LOG.warn("livy.repl.enable-hive-context is true but no hive-site.xml found on " +
               "classpath.");
            }
            hivectx = new HiveContext(sc().sc());
            LOG.info("Created HiveContext.");
          }
        }
      }
    }
    return hivectx;
  }

  public synchronized void stop() {
    if (sc != null) {
      sc.stop();
    }
  }
}
