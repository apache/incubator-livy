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

package org.apache.livy.server.interactive

import java.net.URI
import javax.servlet.http.HttpServletRequest

import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._

import org.json4s.jackson.Json4sScalaModule
import org.scalatra._
import org.scalatra.servlet.FileUploadSupport

import org.apache.livy.{CompletionRequest, ExecuteRequest, JobHandle, LivyConf, Logging}
import org.apache.livy.client.common.ClientConf
import org.apache.livy.client.common.HttpMessages
import org.apache.livy.client.common.HttpMessages._
import org.apache.livy.server.{AccessManager, SessionServlet}
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions._

object InteractiveSessionServlet extends Logging

class InteractiveSessionServlet(
    sessionManager: InteractiveSessionManager,
    sessionStore: SessionStore,
    livyConf: LivyConf,
    accessManager: AccessManager)
  extends SessionServlet(sessionManager, livyConf, accessManager)
  with SessionHeartbeatNotifier[InteractiveSession, InteractiveRecoveryMetadata]
  with FileUploadSupport
{

  mapper.registerModule(new SessionKindModule())
    .registerModule(new Json4sScalaModule())

  override protected def createSession(req: HttpServletRequest): InteractiveSession = {
    val createRequest = bodyAs[CreateInteractiveRequest](req)
    val sessionId = sessionManager.nextId();

    // Calling getTimeAsMs just to validate the ttl value
    if (createRequest.ttl.isDefined) {
      ClientConf.getTimeAsMs(createRequest.ttl.get);
    }

    InteractiveSession.create(
      sessionId,
      createRequest.name,
      remoteUser(req),
      proxyUser(req, createRequest.proxyUser),
      livyConf,
      accessManager,
      createRequest,
      sessionStore,
      createRequest.ttl)
  }

  override protected[interactive] def clientSessionView(
      session: InteractiveSession,
      req: HttpServletRequest): Any = {
    val logs =
      if (accessManager.hasViewAccess(session.owner,
                                      effectiveUser(req),
                                      session.proxyUser.getOrElse(""))) {
        Option(session.logLines())
          .map { lines =>
            val size = 10
            val from = math.max(0, lines.length - size)
            val until = from + size

            lines.view(from, until)
          }
          .getOrElse(Nil)
      } else {
        Nil
      }

    new SessionInfo(session.id, session.name.orNull, session.appId.orNull, session.owner,
      session.proxyUser.orNull, session.state.toString, session.kind.toString,
      session.appInfo.asJavaMap, logs.asJava, session.ttl.orNull)
  }

  post("/:id/stop") {
    withModifyAccessSession { session =>
      Await.ready(session.stop(), Duration.Inf)
      NoContent()
    }
  }

  post("/:id/interrupt") {
    withModifyAccessSession { session =>
      Await.ready(session.interrupt(), Duration.Inf)
      Ok(Map("msg" -> "interrupted"))
    }
  }

  get("/:id/statements") {
    withViewAccessSession { session =>
      val statements = session.statements
      val from = params.get("from").map(_.toInt).getOrElse(0)
      val size = params.get("size").map(_.toInt).getOrElse(statements.length)

      Map(
        "total_statements" -> statements.length,
        "statements" -> statements.view(from, from + size)
      )
    }
  }

  val getStatement = get("/:id/statements/:statementId") {
    withViewAccessSession { session =>
      val statementId = params("statementId").toInt

      session.getStatement(statementId).getOrElse(NotFound("Statement not found"))
    }
  }

  jpost[ExecuteRequest]("/:id/statements") { req =>
    withModifyAccessSession { session =>
      val statement = session.executeStatement(req)

      Created(statement,
        headers = Map(
          "Location" -> url(getStatement,
            "id" -> session.id.toString,
            "statementId" -> statement.id.toString)))
    }
  }

  jpost[CompletionRequest]("/:id/completion") { req =>
    withModifyAccessSession { session =>
      val compl = session.completion(req)
      Ok(Map("candidates" -> compl.candidates))
    }
  }

  post("/:id/statements/:statementId/cancel") {
    withModifyAccessSession { session =>
      val statementId = params("statementId")
      session.cancelStatement(statementId.toInt)
      Ok(Map("msg" -> "canceled"))
    }
  }
  // This endpoint is used by the client-http module to "connect" to an existing session and
  // update its last activity time. It performs authorization checks to make sure the caller
  // has access to the session, so even though it returns the same data, it behaves differently
  // from get("/:id").
  post("/:id/connect") {
    withModifyAccessSession { session =>
      session.recordActivity()
      Ok(clientSessionView(session, request))
    }
  }

  jpost[SerializedJob]("/:id/submit-job") { req =>
    withModifyAccessSession { session =>
      try {
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.submitJob(req.job, req.jobType)
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null))
      } catch {
        case e: Throwable =>
        throw e
      }
    }
  }

  jpost[SerializedJob]("/:id/run-job") { req =>
    withModifyAccessSession { session =>
      require(req.job != null && req.job.length > 0, "no job provided.")
      val jobId = session.runJob(req.job, req.jobType)
      Created(new JobStatus(jobId, JobHandle.State.SENT, null, null))
    }
  }

  post("/:id/upload-jar") {
    withModifyAccessSession { lsession =>
      fileParams.get("jar") match {
        case Some(file) =>
          lsession.addJar(file.getInputStream, file.name)
        case None =>
          BadRequest("No jar sent!")
      }
    }
  }

  post("/:id/upload-pyfile") {
    withModifyAccessSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.addJar(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  post("/:id/upload-file") {
    withModifyAccessSession { lsession =>
      fileParams.get("file") match {
        case Some(file) =>
          lsession.addFile(file.getInputStream, file.name)
        case None =>
          BadRequest("No file sent!")
      }
    }
  }

  jpost[AddResource]("/:id/add-jar") { req =>
    withModifyAccessSession { lsession =>
      addJarOrPyFile(req, lsession)
    }
  }

  jpost[AddResource]("/:id/add-pyfile") { req =>
    withModifyAccessSession { lsession =>
      addJarOrPyFile(req, lsession)
    }
  }

  jpost[AddResource]("/:id/add-file") { req =>
    withModifyAccessSession { lsession =>
      val uri = new URI(req.uri)
      lsession.addFile(uri)
    }
  }

  get("/:id/jobs/:jobid") {
    withViewAccessSession { lsession =>
      val jobId = params("jobid").toLong
      Ok(lsession.jobStatus(jobId))
    }
  }

  post("/:id/jobs/:jobid/cancel") {
    withModifyAccessSession { lsession =>
      val jobId = params("jobid").toLong
      lsession.cancelJob(jobId)
    }
  }

  private def addJarOrPyFile(req: HttpMessages.AddResource, session: InteractiveSession): Unit = {
    val uri = new URI(req.uri)
    session.addJar(uri)
  }
}
