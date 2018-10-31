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

import java.text.ParseException

import com.nimbusds.jose.{JOSEException, JWSHeader, JWSVerifier}
import com.nimbusds.jose.util.Base64URL
import com.nimbusds.jwt.PlainJWT
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, FunSpec}
import org.scalatest.Matchers._
import org.scalatest.mock.MockitoSugar.mock

import org.apache.livy.LivyBaseUnitTestSuite

class JWTValidatorSpec extends FunSpec with LivyBaseUnitTestSuite with BaseJWTSpec
  with BeforeAndAfterEach {

  var jwsVerifier: JWSVerifier = _
  var jwtValidator: JWTValidator = _

  override def beforeEach(): Unit = {
    jwsVerifier = mock[JWSVerifier]
    jwtValidator = new JWTValidator(jwsVerifier)
    super.beforeEach()
  }

  describe("JWTValidator.validateToken") {
    it("An expired token should return false") {
      when(jwsVerifier.verify(any[JWSHeader], any[Array[Byte]], any[Base64URL])).thenReturn(true)
      jwtValidator.isValid(expiredToken.serialize()) shouldBe false
    }

    it("A unexpired token with a valid signature should return true") {
      when(jwsVerifier.verify(any[JWSHeader], any[Array[Byte]], any[Base64URL])).thenReturn(true)
      jwtValidator.isValid(unexpiredToken.serialize()) shouldBe true
    }

    it("A token with an invalid signature should return false") {
      when(jwsVerifier.verify(any[JWSHeader], any[Array[Byte]], any[Base64URL])).thenReturn(false)
      jwtValidator.isValid(unexpiredToken.serialize()) shouldBe false
    }

    it("A string that is not a JWT token should throw") {
      intercept[ParseException] {
        jwtValidator.isValid("Hello World")
      }
    }

    it("A JWT token with no signature should throw") {
      val jwt = new PlainJWT(createClaimsSet("testValidator", null))
      intercept[ParseException] {
        jwtValidator.isValid(jwt.serialize())
      }
    }

    it("A JWT token with no expiration time should be valid") {
      when(jwsVerifier.verify(any[JWSHeader], any[Array[Byte]], any[Base64URL])).thenReturn(true)
      jwtValidator.isValid(unexpiredTokenWithNoExpiration.serialize()) shouldBe true
    }

    it("A JOSEException should be passed through") {
      when(jwsVerifier.verify(any[JWSHeader], any[Array[Byte]], any[Base64URL]))
        .thenThrow(new JOSEException("Invalid"))
      intercept[JOSEException] {
        jwtValidator.isValid(unexpiredToken.serialize())
      }
    }
  }
}
