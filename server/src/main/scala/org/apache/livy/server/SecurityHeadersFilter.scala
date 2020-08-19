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

import javax.servlet.{Filter, FilterChain, FilterConfig, ServletRequest, ServletResponse}
import javax.servlet.http.HttpServletResponse

import org.apache.livy.LivyConf

/**
 * Adds security related headers to HTTP responses.
 */
class SecurityHeadersFilter(livyConf: LivyConf) extends Filter {

  val headers : Map[String, String] = Map(
    "X-Content-Type-Options" -> livyConf.get(LivyConf.SECURITY_HEADERS_CONTENT_TYPE_OPTIONS),
    "X-Frame-Options" -> livyConf.get(LivyConf.SECURITY_HEADERS_FRAME_OPTIONS),
    "X-XSS-Protection" -> livyConf.get(LivyConf.SECURITY_HEADERS_XSS_PROTECTION))
    .filter(e => e._2 != null && e._2.trim().length > 0)

  override def init(filterConfig: FilterConfig): Unit = {}

  override def doFilter(request: ServletRequest,
                        response: ServletResponse,
                        chain: FilterChain): Unit = {
    val servletResponse = response.asInstanceOf[HttpServletResponse]
    for ((k, v) <- headers) {
      servletResponse.addHeader(k, v)
    }
    chain.doFilter(request, response)
}

  override def destroy(): Unit = {}
}
