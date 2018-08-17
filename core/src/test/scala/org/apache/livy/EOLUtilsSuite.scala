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
    assert(!EOLUtils.hasUnixEOL(s1))
    assert(!EOLUtils.hasOldMacEOL(s1))
    assert(EOLUtils.hasWindowsEOL(s1))

    val s2 = "test\ntest"
    assert(EOLUtils.hasUnixEOL(s2))
    assert(!EOLUtils.hasOldMacEOL(s2))
    assert(!EOLUtils.hasWindowsEOL(s2))

    val s3 = "test\rtest"
    assert(!EOLUtils.hasUnixEOL(s3))
    assert(EOLUtils.hasOldMacEOL(s3))
    assert(!EOLUtils.hasWindowsEOL(s3))

    val s4 = "testtest"
    assert(!EOLUtils.hasUnixEOL(s4))
    assert(!EOLUtils.hasOldMacEOL(s4))
    assert(!EOLUtils.hasWindowsEOL(s4))
  }

  test("convert EOL") {
    val s1 = "test\r\ntest"
    val s2 = "test\ntest"
    val s3 = "test\rtest"
    val s4 = "testtest"

    assert(EOLUtils.convertToUnixEOL(s1) === s2)
    assert(EOLUtils.convertToWindowsEOL(s1) === s1)
    assert(EOLUtils.convertToOldMacEOL(s1) === s3)

    assert(EOLUtils.convertToUnixEOL(s2) === s2)
    assert(EOLUtils.convertToWindowsEOL(s2) === s1)
    assert(EOLUtils.convertToOldMacEOL(s2) === s3)

    assert(EOLUtils.convertToUnixEOL(s3) === s2)
    assert(EOLUtils.convertToWindowsEOL(s3) === s1)
    assert(EOLUtils.convertToOldMacEOL(s3) === s3)

    assert(EOLUtils.convertToUnixEOL(s4) === s4)
    assert(EOLUtils.convertToWindowsEOL(s4) === s4)
    assert(EOLUtils.convertToOldMacEOL(s4) === s4)
  }
}
