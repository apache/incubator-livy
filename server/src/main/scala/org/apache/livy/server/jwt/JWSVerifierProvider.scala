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

import java.io.{ByteArrayInputStream, File, InputStream}
import java.security.cert.{CertificateFactory, X509Certificate}
import java.security.interfaces.RSAPublicKey

import com.nimbusds.jose.JWSVerifier
import com.nimbusds.jose.crypto.RSASSAVerifier
import org.apache.commons.io.FileUtils

import org.apache.livy.LivyConf
import org.apache.livy.LivyConf.JWT_SIGNATURE_PUBLIC_KEY_PATH

/**
 * A provider of JWSVerifier instances based on the given LivyConf.
 * @param livyConf Used to determine the public key path to use for the verifier
 */
class JWSVerifierProvider(livyConf: LivyConf) {

  private[this] lazy val verifier = {
    val publicKeyFile = livyConf.get(JWT_SIGNATURE_PUBLIC_KEY_PATH)
    require(publicKeyFile != null,
      s"${JWT_SIGNATURE_PUBLIC_KEY_PATH.key} must be set to verify JWT signatures.")
    val publicKey = parseRSAPublicKey(publicKeyFile)
    new RSASSAVerifier(publicKey)
  }

  def get(): JWSVerifier = verifier

  private[this] def parseRSAPublicKey(publicKeyFile: String): RSAPublicKey = {
    val is = new ByteArrayInputStream(FileUtils.readFileToByteArray(
      new File(publicKeyFile)))
    try {
      parseRSAPublicKeyFromInputStream(is)
    } finally {
      is.close()
    }
  }

  private[this] def parseRSAPublicKeyFromInputStream(inputStream: InputStream): RSAPublicKey = {
    val factory = CertificateFactory.getInstance("X.509")
    val certificate = factory.generateCertificate(inputStream).asInstanceOf[X509Certificate]
    certificate.getPublicKey.asInstanceOf[RSAPublicKey]
  }
}
