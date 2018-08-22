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

package org.apache.livy

import org.scalatest.FunSuite

class EOLUtilsSuite extends FunSuite with LivyBaseUnitTestSuite {

  test("check EOL") {
    val s1 = "test\r\ntest"
    assert(!EOLUtils.Mode.hasUnixEOL(s1))
    assert(!EOLUtils.Mode.hasOldMacEOL(s1))
    assert(EOLUtils.Mode.hasWindowsEOL(s1))

    val s2 = "test\ntest"
    assert(EOLUtils.Mode.hasUnixEOL(s2))
    assert(!EOLUtils.Mode.hasOldMacEOL(s2))
    assert(!EOLUtils.Mode.hasWindowsEOL(s2))

    val s3 = "test\rtest"
    assert(!EOLUtils.Mode.hasUnixEOL(s3))
    assert(EOLUtils.Mode.hasOldMacEOL(s3))
    assert(!EOLUtils.Mode.hasWindowsEOL(s3))

    val s4 = "testtest"
    assert(!EOLUtils.Mode.hasUnixEOL(s4))
    assert(!EOLUtils.Mode.hasOldMacEOL(s4))
    assert(!EOLUtils.Mode.hasWindowsEOL(s4))
  }

  test("convert EOL") {
    val s1 = "test\r\ntest"
    val s2 = "test\ntest"
    val s3 = "test\rtest"
    val s4 = "testtest"

    assert(EOLUtils.convertToSystemEOL(s1) === EOLUtils.convertToSystemEOL(s2))
    assert(EOLUtils.convertToSystemEOL(s1) === EOLUtils.convertToSystemEOL(s3))
    assert(EOLUtils.convertToSystemEOL(s2) === EOLUtils.convertToSystemEOL(s3))
    assert(EOLUtils.convertToSystemEOL(s4) === s4)
  }
}
