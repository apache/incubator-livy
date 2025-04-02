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

package org.apache.livy.server

import javax.servlet.FilterChain
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}

import org.mockito.ArgumentCaptor
import org.mockito.Mockito.{atLeastOnce, verify}
import org.scalatest.{FunSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.{LivyBaseUnitTestSuite, LivyConf}

class SecurityHeadersFilterSpec extends FunSpec with Matchers with LivyBaseUnitTestSuite {

  val requiredHeaders = Set("X-Content-Type-Options", "X-Frame-Options", "X-XSS-Protection",
    "Content-Security-Policy")

  private def runFilterAndGetResponseHeaders(configEntries: Set[(LivyConf.Entry, String)]):
  List[(String, String)] = {
    import scala.collection.JavaConverters._
    val livyConf = createLivyConf(configEntries)
    val securityHeadersFilter = new SecurityHeadersFilter(livyConf)
    val response = mock[HttpServletResponse]
    val request = mock[HttpServletRequest]
    val chain = mock[FilterChain]
    val keyCaptor = ArgumentCaptor.forClass(classOf[String])
    val valueCaptor = ArgumentCaptor.forClass(classOf[String])

    securityHeadersFilter.doFilter(request, response, chain)
    verify(response, atLeastOnce()).addHeader(keyCaptor.capture(), valueCaptor.capture())
    keyCaptor.getAllValues.asScala.toList.zip(valueCaptor.getAllValues.asScala.toList)
  }

  private def createLivyConf(entries: Set[(LivyConf.Entry, String)]) = {
    val livyConf = new LivyConf(false)
    entries.foreach({ case (key, value) => livyConf.set(key, value) })
    livyConf
  }

  describe("SecurityHeadersFilter") {

    it("should add security headers with overrides") {
      val responseHeaders = runFilterAndGetResponseHeaders(Set(
        (LivyConf.SECURITY_HEADERS_XSS_PROTECTION, "xss"),
        (LivyConf.SECURITY_HEADERS_CONTENT_SECURITY_POLICY, "csp")))
      assert(requiredHeaders.subsetOf(responseHeaders.map(_._1).toSet))
      assert(responseHeaders.contains(("X-XSS-Protection", "xss")))
      assert(responseHeaders.contains(("Content-Security-Policy", "csp")))
    }

    it("should not add headers that are overridden with empty values") {
      val responseHeaders = runFilterAndGetResponseHeaders(Set(
        (LivyConf.SECURITY_HEADERS_XSS_PROTECTION, ""),
        (LivyConf.SECURITY_HEADERS_CONTENT_SECURITY_POLICY, "")))
      assert(!responseHeaders.exists(_._1 == "X-XSS-Protection"))
      assert(!responseHeaders.exists(_._1 == "Content-Security-Policy"))
    }

    it("should not set HSTS header if TLS is not enabled") {
      val responseHeaders = runFilterAndGetResponseHeaders(Set.empty)
      assert(!responseHeaders.exists(_._1 == "Strict-Transport-Security"))
    }

    it("should set HSTS header if TLS is enabled, value not overridden") {
      val responseHeaders = runFilterAndGetResponseHeaders(Set(
        (LivyConf.SSL_KEYSTORE, "/tmp")))
      assert(responseHeaders.exists(_._1 == "Strict-Transport-Security"))
    }

    it("should set HSTS header if TLS is enabled, value overridden") {
      val responseHeaders = runFilterAndGetResponseHeaders(Set(
        (LivyConf.SSL_KEYSTORE, "/tmp"),
        (LivyConf.SECURITY_HEADERS_STRICT_TRANSPORT_SECURITY, "sts")))
      assert(responseHeaders.contains(("Strict-Transport-Security", "sts")))
    }

    it("should not set HSTS header if TLS is enabled, value overridden with empty string") {
      val responseHeaders = runFilterAndGetResponseHeaders(Set(
        (LivyConf.SSL_KEYSTORE, "/tmp"),
        (LivyConf.SECURITY_HEADERS_STRICT_TRANSPORT_SECURITY, "")))
      assert(!responseHeaders.exists(_._1 == "Strict-Transport-Security"))
    }
  }
}
