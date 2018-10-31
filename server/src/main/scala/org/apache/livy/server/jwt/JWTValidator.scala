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

import java.util.Date

import com.nimbusds.jose.JWSVerifier
import com.nimbusds.jwt.SignedJWT

import org.apache.livy.Logging

/**
 * A validator for JSON Web Tokens (JWT).
 * @param jwsVerifier JWSVerifier used to verify the JWT token's signature
 */
class JWTValidator(jwsVerifier: JWSVerifier) extends Logging {

  def isValid(token: String): Boolean = {
    val signedJwt = SignedJWT.parse(token)

    if (!isSignatureValid(signedJwt)) {
      warn("Signature of JWT token could not be verified.")
      false
    } else if (!isExpired(signedJwt)) {
      warn("Expiration time validation of JWT token failed.")
      false
    } else true
  }

  private def isSignatureValid(signedJWT: SignedJWT): Boolean = {
    signedJWT.getSignature != null && signedJWT.verify(jwsVerifier)
  }

  private def isExpired(signedJWT: SignedJWT): Boolean = {
    val expires = signedJWT.getJWTClaimsSet.getExpirationTime
    expires == null || new Date().before(expires)
  }
}
