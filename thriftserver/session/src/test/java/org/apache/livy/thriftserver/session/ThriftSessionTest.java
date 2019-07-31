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

package org.apache.livy.thriftserver.session;

import java.net.URI;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.launcher.SparkLauncher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.livy.Job;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import static org.apache.livy.rsc.RSCConf.Entry.*;

public class ThriftSessionTest {

  private static LivyClient livy;
  private static int SESSION_ID = 0;
  private static int STATEMENT_ID = 0;

  @BeforeClass
  public static void setUp() throws Exception {
    String warehouse = Files.createTempDirectory("spark-warehouse-").toFile().getAbsolutePath();

    Properties conf = new Properties();
    conf.put(LIVY_JARS.key(), "");
    conf.put("spark.app.name", ThriftSessionTest.class.getName());
    conf.put(CLIENT_IN_PROCESS.key(), "true");
    conf.put(SparkLauncher.SPARK_MASTER, "local");
    conf.put("spark.sql.warehouse.dir", warehouse);
    conf.put("spark.sql.catalogImplementation", "in-memory");

    livy = new LivyClientBuilder(false)
      .setURI(new URI("rsc:/"))
      .setAll(conf)
      .build();
  }

  @AfterClass
  public static void cleanUp() throws Exception {
    if (livy != null) {
      livy.stop(true);
      livy = null;
    }
  }

  @Test
  public void testThriftSession() throws Exception {
    // Try to run a command in an invalid session.
    expectError(newSqlJob("invalid", "s1", "invalid sql"), "not found");
    expectError(new UnregisterSessionJob("invalid"), "not found");

    // Create a new session.
    String s1 = nextSession();
    waitFor(new RegisterSessionJob(s1));

    // Run a statement in the new session.
    String st1 = nextStatement();
    waitFor(newSqlJob(s1, st1, "CREATE TABLE test (id integer, desc string) USING json"));

    // Start a second session. Try to cleanup a statement that belongs to another session.
    String s2 = nextSession();
    waitFor(new RegisterSessionJob(s2));
    assertFalse(waitFor(new CleanupStatementJob(s2, st1)));
    waitFor(new UnregisterSessionJob(s2));

    // Clean up the statement's state.
    assertTrue(waitFor(new CleanupStatementJob(s1, st1)));
    assertFalse(waitFor(new CleanupStatementJob(s1, st1)));

    // Insert data into the previously created table, and fetch results from it.
    String st2 = nextStatement();
    waitFor(newSqlJob(s1, st2, "INSERT INTO test VALUES (1, \"one\"), (2, \"two\")"));
    assertTrue(waitFor(new CleanupStatementJob(s1, st2)));

    String st3 = nextStatement();
    waitFor(newSqlJob(s1, st3, "SELECT * FROM test"));

    String schema = waitFor(new FetchResultSchemaJob(s1, st3));
    assertTrue(schema, schema.contains("\"name\":\"id\",\"type\":\"integer\""));
    assertTrue(schema, schema.contains("\"name\":\"desc\",\"type\":\"string\""));

    ResultSet rs = waitFor(new FetchResultJob(s1, st3, 10));
    ColumnBuffer[] cols = rs.getColumns();
    assertEquals(2, cols.length);

    assertEquals(Integer.valueOf(1), cols[0].get(0));
    assertEquals("one", cols[1].get(0));
    assertEquals(Integer.valueOf(2), cols[0].get(1));
    assertEquals("two", cols[1].get(1));

    assertTrue(waitFor(new CleanupStatementJob(s1, st3)));

    // Run a statement that returns a null, to make sure the receiving side sees it correctly.
    String st4 = nextStatement();
    waitFor(newSqlJob(s1, st4, "SELECT cast(null as int)"));

    rs = waitFor(new FetchResultJob(s1, st4, 10));
    cols = rs.getColumns();
    assertEquals(1, cols.length);
    assertEquals(1, cols[0].size());
    assertTrue(cols[0].getNulls().get(0));

    assertTrue(waitFor(new CleanupStatementJob(s1, st4)));

    // Tear down the session.
    waitFor(new UnregisterSessionJob(s1));
  }

  private String nextSession() {
    return "session" + SESSION_ID++;
  }

  private String nextStatement() {
    return "statement" + STATEMENT_ID++;
  }

  private SqlJob newSqlJob(String session, String stId, String statement) {
    return new SqlJob(session, stId, statement, "true", "incrementalPropName");
  }

  /**
   * Waits for a future to complete expecting an error to be thrown, failing the test if run is
   * successful.
   *
   * @param job The job to run.
   * @param expected String expected to be in the exception message.
   * @return The error that was caught.
   */
  private Exception expectError(Job<?> job, String expected) throws TimeoutException {
    try {
      livy.submit(job).get(10, TimeUnit.SECONDS);
      fail("No exception was thrown.");
      return null;
    } catch (TimeoutException te) {
      throw te;
    } catch (Exception e) {
      assertTrue(e.getMessage(), e.getMessage().contains(expected));
      return e;
    }
  }

  private <T> T waitFor(Job<T> job) throws Exception {
    return livy.submit(job).get(10, TimeUnit.SECONDS);
  }

}
