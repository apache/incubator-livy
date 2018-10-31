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

import javax.servlet.{Filter, FilterChain, FilterConfig, ServletRequest, ServletResponse}
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import scala.util.control.NonFatal

import org.apache.livy.{LivyConf, Logging}
import org.apache.livy.LivyConf.JWT_HEADER_NAME

/**
 * A JWTFilter that utilizes a JWTValidator to determine if a JWT token included in a request
 * is valid and accept/reject the request.
 * @param jwtValidator validator used to determine the validity of the jwt token
 */
class JWTFilter(jwtValidator: JWTValidator, livyConf: LivyConf) extends Filter with Logging {

  private val headerName = livyConf.get(JWT_HEADER_NAME)

  override def init(filterConfig: FilterConfig): Unit = {}

  override def destroy(): Unit = {}

  override def doFilter(servletRequest: ServletRequest,
                        servletResponse: ServletResponse,
                        filterChain: FilterChain): Unit = {
    val httpRequest = servletRequest.asInstanceOf[HttpServletRequest]
    val httpServletResponse = servletResponse.asInstanceOf[HttpServletResponse]

    val jwtToken = httpRequest.getHeader(headerName)
    if (jwtToken == null) {
      httpServletResponse.sendError(HttpServletResponse.SC_BAD_REQUEST,
        s"Header: ${headerName} is missing in the request.")
    } else {
      if (validateToken(jwtToken)) {
        filterChain.doFilter(httpRequest, servletResponse)
      } else {
        httpServletResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED,
          "JWT token included in request failed validation.")
      }
    }
  }

  private def validateToken(jwtToken: String): Boolean = {
    try {
      jwtValidator.isValid(jwtToken)
    } catch {
      case NonFatal(e) =>
        warn("Exception while validating JWTToken", e)
        false
    }
  }
}

object JWTFilter {

  def apply(livyConf: LivyConf): JWTFilter = {
    val jwsVerifier = new JWSVerifierProvider(livyConf).get()
    val jwtValidator = new JWTValidator(jwsVerifier)
    new JWTFilter(jwtValidator, livyConf)
  }
}
