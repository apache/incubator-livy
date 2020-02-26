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

package org.apache.livy.test.framework

import java.io.File
import java.security.Principal
import java.security.PrivilegedExceptionAction
import java.util.UUID

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.http.auth.AuthScope
import org.apache.http.auth.Credentials
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.client.params.AuthPolicy
import org.apache.http.impl.auth.BasicSchemeFactory
import org.apache.http.impl.auth.SPNegoSchemeFactory
import org.apache.http.impl.client.DefaultHttpClient
import org.scalatest._

abstract class BaseIntegrationTestSuite extends FunSuite with Matchers with BeforeAndAfterAll {
  import scala.concurrent.ExecutionContext.Implicits.global

  var cluster: Cluster = _
  var httpClient: DefaultHttpClient = _
  var livyClient: LivyRestClient = _

  protected def authScheme: String = cluster.authScheme
  protected def livyEndpoint: String = cluster.livyEndpoint
  protected def user: String = cluster.user
  protected def password: String = cluster.password
  protected def sslCertPath = cluster.sslCertPath

  protected val testLib = sys.props("java.class.path")
    .split(File.pathSeparator)
    .find(new File(_).getName().startsWith("livy-test-lib-"))
    .getOrElse(throw new Exception(s"Cannot find test lib in ${sys.props("java.class.path")}"))

  protected def getYarnLog(appId: String): String = {
    require(appId != null, "appId shouldn't be null")

    val appReport = cluster.yarnClient.getApplicationReport(ConverterUtils.toApplicationId(appId))
    assert(appReport != null, "appReport shouldn't be null")

    appReport.getDiagnostics()
  }

  protected def restartLivy(): Unit = {
    val f = Future {
      cluster.stopLivy()
      cluster.runLivy()
    }
    Await.result(f, 3 minutes)
  }

  /** Uploads a file to HDFS and returns just its path. */
  protected def uploadToHdfs(file: File): String = {
    val hdfsPath = new Path(cluster.hdfsScratchDir(),
      UUID.randomUUID().toString() + "-" + file.getName())

    if (authScheme == "kerberos") {
      val proxy =
        UserGroupInformation.createProxyUser(user, UserGroupInformation.getLoginUser())
      proxy.doAs(new PrivilegedExceptionAction[Unit] {
        def run() = {
          cluster.fs.copyFromLocalFile(new Path(file.toURI()), hdfsPath)
        }
      })
    } else {
      cluster.fs.copyFromLocalFile(new Path(file.toURI()), hdfsPath)
    }

    hdfsPath.toUri().getPath()
  }

  /** Clean up session and show info when test fails. */
  protected def withSession[S <: LivyRestClient#Session, R]
    (s: S)
    (f: (S) => R): R = {
    try {
      f(s)
    } catch {
      case NonFatal(e) =>
        try {
          val state = s.snapshot()
          info(s"Final session state: $state")
          state.appId.foreach { id => info(s"YARN diagnostics: ${getYarnLog(id)}") }
        } catch { case NonFatal(_) => }
        throw e
    } finally {
      try {
        s.stop()
      } catch {
        case NonFatal(e) => alert(s"Failed to stop session: $e")
      }
    }
  }

  // We need beforeAll() here because BatchIT's beforeAll() has to be executed after this.
  // Please create an issue if this breaks test logging for cluster creation.
  protected override def beforeAll() = {
    cluster = Cluster.get()
    httpClient = new DefaultHttpClient()

    if (authScheme == "kerberos") {
      val useJAASCreds = new Credentials() {
        def getPassword(): String = {
          return null
        }

        def getUserPrincipal(): Principal = {
          return null
        }
      }

      httpClient.getAuthSchemes().register(AuthPolicy.SPNEGO, new SPNegoSchemeFactory())
      httpClient.getCredentialsProvider().setCredentials(
        new AuthScope(null, -1, null),
        useJAASCreds)
    } else if (authScheme == "basic") {
      httpClient.getAuthSchemes().register(AuthPolicy.BASIC, new BasicSchemeFactory())
      httpClient.getCredentialsProvider().setCredentials(
        AuthScope.ANY,
        new UsernamePasswordCredentials(user, password))
    }

    livyClient = new LivyRestClient(httpClient, livyEndpoint)
  }
}
