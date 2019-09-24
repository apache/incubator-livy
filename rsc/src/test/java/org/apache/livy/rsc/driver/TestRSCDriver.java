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

import org.apache.spark.SparkConf;
import org.junit.Test;

import org.apache.livy.rsc.BaseProtocol;
import org.apache.livy.rsc.RSCConf;

public class TestRSCDriver {
  @Test(expected = IllegalArgumentException.class)
  public void testCancelJobAfterInitializeFailed()
      throws Exception {
    //use empty Conf to trigger initialize throw IllegalArgumentException
    RSCDriver rscDriver = new RSCDriver(new SparkConf(), new RSCConf());
    //add asynchronous dummy job request to trigger cancel job failure
    rscDriver.handle(null, new BaseProtocol.BypassJobRequest("RequestId", null, null, false));
    rscDriver.run();
  }
}
