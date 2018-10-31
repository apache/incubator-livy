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

package org.apache.livy.server.jwt

import java.io.{File, PrintWriter}
import java.security.cert.CertificateException
import java.util.Base64

import org.scalatest.FunSpec

import org.apache.livy.LivyBaseUnitTestSuite
import org.apache.livy.LivyConf
import org.apache.livy.LivyConf.JWT_SIGNATURE_PUBLIC_KEY_PATH

class JWSVerifierProviderSpec extends FunSpec with LivyBaseUnitTestSuite with BaseJWTSpec {

  val invalidPublicKeyPath = {
    // This is invalid because it is storing only the RSAPublicKey, not the certificate
    writeKeyToTempFile("invalid-public-key", createNewPublicKey().getEncoded)
  }

  val publicKeyPath = {
    writeKeyToTempFile("public-key", cert.getEncoded)
  }

  val livyConf = new LivyConf()

  describe("JWSVerifierProvider") {
    it("should fail when given an invalid publicKey") {
      livyConf.set(JWT_SIGNATURE_PUBLIC_KEY_PATH, invalidPublicKeyPath)
      intercept[CertificateException] {
        new JWSVerifierProvider(livyConf).get()
      }
    }

    it("should succeed when given a valid publicKey") {
      livyConf.set(JWT_SIGNATURE_PUBLIC_KEY_PATH, publicKeyPath)
      new JWSVerifierProvider(livyConf).get()
    }

    it("should fail when no publicKey is given") {
      intercept[IllegalArgumentException] {
        new JWSVerifierProvider(new LivyConf()).get()
      }
    }
  }

  private def writeKeyToTempFile(tmpFilePrefix: String, encodedBytes: Array[Byte]): String = {
    val tmpFile = File.createTempFile(tmpFilePrefix, ".pem")
    tmpFile.deleteOnExit()
    writeKeyToFile(tmpFile, encodedBytes)
    tmpFile.getAbsolutePath
  }

  private def writeKeyToFile(file: File, encodedBytes: Array[Byte]): Unit = {
    val writer = new PrintWriter(file)
    try {
      writer.write("-----BEGIN CERTIFICATE-----\n")
      writer.append(new String(Base64.getEncoder().encode(encodedBytes)))
      writer.append("\n-----END CERTIFICATE-----")
    } finally {
      writer.close()
    }
  }
}
