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

package org.apache.livy.sessions

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files

import org.apache.commons.io.IOUtils
import org.scalatest.FunSuite

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}

class ResourceManagerSpec extends FunSuite with LivyBaseUnitTestSuite {

  test("register and unregister sessions") {
    val sessionId = 1
    val conf = new LivyConf(false)
    val resourceManager = new ResourceManager(conf)

    val baseDir = resourceManager.baseDir
    assert(baseDir.exists())
    assert(baseDir.isDirectory)
    assert(conf.get(LivyConf.LOCAL_FS_WHITELIST).contains(baseDir.getAbsolutePath))

    resourceManager.registerSession(sessionId)
    val sessionDir = new File(baseDir, sessionId.toString)
    assert(sessionDir.exists())
    assert(sessionDir.isDirectory)

    resourceManager.unregisterSession(sessionId)
    assert(!sessionDir.exists())
  }

  test("save and retrieve resources") {
    val sessionId = 1
    val conf = new LivyConf(false)
    val resourceManager = new ResourceManager(conf)

    resourceManager.registerSession(sessionId)

    val tmpFile = Files.createTempFile("tmp", "jar").toFile
    var os: FileOutputStream = null
    try {
      os = new FileOutputStream(tmpFile)
      IOUtils.write("test", os)
    } finally {
      if (os != null) {
        os.close()
        os = null
      }
    }

    var is: FileInputStream = null
    try {
      is = new FileInputStream(tmpFile)
      resourceManager.saveResource(sessionId, tmpFile.getName, is)
      val res = resourceManager.retrieveResource(sessionId, tmpFile.getName)
      assert(res.isDefined)
      val resPath = s"${resourceManager.baseDir.getAbsolutePath.stripSuffix("/")}" +
        s"/$sessionId/${tmpFile.getName}"
      assert(res.get == resPath)
      assert(resourceManager.retrieveResource(sessionId, "random") == None)
    } finally {
      if (is != null) {
        is.close()
        is == null
      }
    }

    resourceManager.unregisterSession(sessionId)
  }

  test("save resources without registering session") {
    val conf = new LivyConf(false)
    val resourceManager = new ResourceManager(conf)

    val tmpFile = Files.createTempFile("tmp", ".jar").toFile
    var is: FileInputStream = null
    try {
      is = new FileInputStream(tmpFile)
      intercept[IllegalStateException](resourceManager.saveResource(1, tmpFile.getName, is))
    } finally {
      if (is != null) {
        is.close()
        is = null
      }
    }
  }

  test("clean the resources manager") {
    val conf = new LivyConf(false)
    val resourceManager = new ResourceManager(conf)

    resourceManager.registerSession(1)
    resourceManager.registerSession(2)
    resourceManager.registerSession(3)

    val tmpJar = Files.createTempFile("tmpJar", ".jar").toFile
    val tmpJarIs = new FileInputStream(tmpJar)
    val tmpFile = Files.createTempFile("tmpFile", "").toFile
    val tmpFileIs = new FileInputStream(tmpFile)
    val tmpPy = Files.createTempFile("tmpPy", ".py").toFile
    val tmpPyIs = new FileInputStream(tmpPy)

    resourceManager.saveResource(1, tmpJar.getName, tmpJarIs)
    resourceManager.saveResource(2, tmpFile.getName, tmpFileIs)
    resourceManager.saveResource(3, tmpPy.getName, tmpPyIs)

    tmpJarIs.close()
    tmpFileIs.close()
    tmpPyIs.close()

    resourceManager.clean()
    assert(!resourceManager.baseDir.exists())
  }
}
