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

import java.security.interfaces.{RSAPrivateKey, RSAPublicKey}
import java.util.Date

import com.nimbusds.jose.{JWSAlgorithm, JWSHeader}
import com.nimbusds.jose.crypto.RSASSASigner
import com.nimbusds.jwt.{JWTClaimsSet, SignedJWT}
import org.joda.time.DateTime
import sun.security.tools.keytool.CertAndKeyGen
import sun.security.x509.X500Name

trait BaseJWTSpec {

  private final val BIT_STRENGTH = 512
  private final val EXPIRATION = 60 * 60 * 24 // One Day
  lazy val generator = createGenerator()
  lazy val cert = generator.getSelfCertificate(
    new X500Name("CN=LivyServer,O=Apache,L=ORG,C=DE"), EXPIRATION)
  lazy val privateKey = generator.getPrivateKey.asInstanceOf[RSAPrivateKey]
  lazy val publicKey = generator.getPublicKeyAnyway.asInstanceOf[RSAPublicKey]
  lazy val signer = new RSASSASigner(privateKey)
  lazy val unexpiredToken = createToken(new DateTime().plusDays(1), sign = true)
  lazy val expiredToken = createToken(new DateTime().minusDays(1), sign = true)
  lazy val unsignedToken = createToken(new DateTime().plusDays(1), sign = false)
  lazy val unexpiredTokenWithNoExpiration = createToken(null, sign = true)

  def createNewPublicKey(): RSAPublicKey = {
    val newGenerator = createGenerator()
    newGenerator.getPublicKeyAnyway.asInstanceOf[RSAPublicKey]
  }

  def createClaimsSet(issuer: String, expirationTime: Date): JWTClaimsSet = {
    val builder = new JWTClaimsSet.Builder().issuer(issuer)
    if (expirationTime != null) {
      builder.expirationTime(expirationTime)
    }
    builder.build()
  }

  def createToken(expirationTime: DateTime, sign: Boolean): SignedJWT = {
    val expiration = if (expirationTime == null) null else expirationTime.toDate
    val signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.RS256),
      createClaimsSet("jwt-test", expiration))
    if (sign) {
      signedJWT.sign(signer)
    }
    signedJWT
  }

  private def createGenerator(): CertAndKeyGen = {
    val newGenerator = new CertAndKeyGen("RSA", "SHA256WithRSA", null)
    newGenerator.generate(BIT_STRENGTH)
    newGenerator
  }
}
