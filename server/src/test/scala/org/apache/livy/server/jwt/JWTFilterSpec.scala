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

import javax.servlet.FilterChain
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfterEach, FunSpec}
import org.scalatest.mock.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}
import org.apache.livy.LivyConf.JWT_HEADER_NAME

class JWTFilterSpec extends FunSpec with LivyBaseUnitTestSuite
  with BaseJWTSpec with BeforeAndAfterEach {

  var request: HttpServletRequest = _
  var response: HttpServletResponse = _
  var filterChain: FilterChain = _
  var jwtValidator: JWTValidator = _
  var jwtFilter: JWTFilter = _

  val livyConf = new LivyConf()
  val headerName = livyConf.get(JWT_HEADER_NAME)

  override def beforeEach() {
    request = mock[HttpServletRequest]
    response = mock[HttpServletResponse]
    filterChain = mock[FilterChain]
    jwtValidator = mock[JWTValidator]
    jwtFilter = new JWTFilter(jwtValidator, livyConf)
    when(request.getHeader(headerName)).thenReturn(unexpiredToken.serialize())
    super.beforeEach()
  }

  describe("JWTFilter") {

    it("should pass when given valid JWT.") {
      when(jwtValidator.isValid(unexpiredToken.serialize())).thenReturn(true)
      jwtFilter.doFilter(request, response, filterChain)
      verify(filterChain, times(1)).doFilter(request, response)
    }

    it("should fail when given an invalid JWT.") {
      when(jwtValidator.isValid(unexpiredToken.serialize())).thenReturn(false)
      jwtFilter.doFilter(request, response, filterChain)
      verify(response, times(1)).sendError(HttpServletResponse.SC_UNAUTHORIZED,
        "JWT token included in request failed validation.")
    }

    it("should fail when an exception is thrown validating JWT.") {
      when(jwtValidator.isValid(unexpiredToken.serialize()))
        .thenThrow(new IllegalArgumentException())
      jwtFilter.doFilter(request, response, filterChain)
      verify(response, times(1)).sendError(HttpServletResponse.SC_UNAUTHORIZED,
        "JWT token included in request failed validation.")
    }

    it("should fail if the request is missing the JWT header") {
      when(request.getHeader(livyConf.get(JWT_HEADER_NAME))).thenReturn(null)
      jwtFilter.doFilter(request, response, filterChain)
      verify(response, times(1)).sendError(HttpServletResponse.SC_BAD_REQUEST,
        s"Header: ${headerName} is missing in the request.")
    }
  }
}
