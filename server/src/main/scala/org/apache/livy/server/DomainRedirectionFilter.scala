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

import java.io.IOException
import java.net.URL
import javax.servlet.Filter
import javax.servlet.FilterChain
import javax.servlet.FilterConfig
import javax.servlet.ServletContext
import javax.servlet.ServletException
import javax.servlet.ServletRequest
import javax.servlet.ServletRequestWrapper
import javax.servlet.ServletResponse
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletRequestWrapper
import javax.servlet.http.HttpServletResponse

import org.springframework.web.util.UriComponentsBuilder

import org.apache.livy.{LivyConf, Logging}

class DomainRedirectionFilter(haService: CuratorElectorService) extends Filter
  with Logging
{

  val METHODS_TO_IGNORE = Set("GET", "OPTIONS", "HEAD")

  val HEADER_NAME = "X-Requested-By"

  def isLeader(): Boolean = {
    haService.currentState == HAState.Active
  }

  override def init(filterConfig: FilterConfig): Unit = {}

  override def doFilter(request: ServletRequest,
                        response: ServletResponse,
                        chain: FilterChain): Unit = {
    if (!isLeader()) {
        debug("active leader's address is:" + haService.getActiveAddress())
        debug("current id:" + haService.getCurrentId())
        val httpRequest = request.asInstanceOf[HttpServletRequest]
        val requestURL = httpRequest.getRequestURL().toString()
        debug("requested url: " + requestURL)

        val builder = UriComponentsBuilder.fromHttpUrl(requestURL)
        val redirectURL = builder.host(haService.getActiveAddress()).toUriString()
        debug("redirected url:" + redirectURL)

        val httpServletResponse = response.asInstanceOf[HttpServletResponse];
        val redirectMsg = "This is a standby Livy Instance. The redirect url is: " + redirectURL
        val out = httpServletResponse.getWriter()
        // scalastyle:off println
        out.println(redirectMsg)
        // scalastyle:on println

        httpServletResponse.setHeader("Location", redirectURL)
        httpServletResponse.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT)
    } else {
        chain.doFilter(request, response);
    }
  }

  override def destroy(): Unit = {}
}