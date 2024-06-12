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

package org.apache.livy.utils

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}

import org.scalatest.{BeforeAndAfter, FunSpec}

class SparkProcessBuildSuite
  extends FunSpec
    with BeforeAndAfter
    with org.scalatest.Matchers
    with LivyBaseUnitTestSuite {

  // LIVY-998: Escape backtick from spark-submit arguments
  it("should only escape unescaped backticks") {
    val livyConf = new LivyConf
    livyConf.set(LivyConf.SPARK_SUBMIT_ESCAPE_BACKTICKS, true)

    val builder = new SparkProcessBuilder(livyConf)

    assert(builder.escapeBackTicks("test_db.test_table") == "test_db.test_table")
    assert(builder.escapeBackTicks("test_db.`test_table`") == "test_db.\\`test_table\\`")
    assert(builder.escapeBackTicks("test_db.\\`test_table\\`") == "test_db.\\`test_table\\`")
  }
}
