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

import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse._

import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.livy.LivyConf
import org.apache.livy.server.recovery.SessionStore
import org.apache.livy.sessions.{Session, SessionManager, SessionState}
import org.apache.livy.sessions.Session.RecoveryMetadata

object SessionServletSpec {

  val PROXY_USER = "proxyUser"

  class MockSession(id: Int, owner: String, ttl: Option[String], idleTimeout: Option[String],
                    val proxyUser: Option[String], livyConf: LivyConf)
    extends Session(id, None, owner, ttl, idleTimeout, livyConf) {

    case class MockRecoveryMetadata(id: Int) extends RecoveryMetadata()

    override def recoveryMetadata: RecoveryMetadata = MockRecoveryMetadata(0)

    override def state: SessionState = SessionState.Idle

    override def start(): Unit = ()

    override protected def stopSession(): Unit = ()

    override def logLines(): IndexedSeq[String] = IndexedSeq("log")
  }

  case class MockSessionView(id: Int, owner: String, proxyUser: Option[String], logs: Seq[String])

  def createServlet(conf: LivyConf): SessionServlet[Session, RecoveryMetadata] = {
    val sessionManager = new SessionManager[Session, RecoveryMetadata](
      conf,
      { _ => assert(false).asInstanceOf[Session] },
      mock[SessionStore],
      "test",
      Some(Seq.empty))

    val accessManager = new AccessManager(conf)
    new SessionServlet(sessionManager, conf, accessManager) with RemoteUserOverride {
      override protected def createSession(req: HttpServletRequest): Session = {
        val params = bodyAs[Map[String, String]](req)
        val owner = remoteUser(req)
        val impersonatedUser = accessManager.checkImpersonation(
          proxyUser(req, params.get(PROXY_USER)), owner)
        new MockSession(sessionManager.nextId(), owner, None, None, impersonatedUser, conf)
      }

      override protected def clientSessionView(
          session: Session,
          req: HttpServletRequest): Any = {
        val hasViewAccess = accessManager.hasViewAccess(session.owner,
                                                        effectiveUser(req),
                                                        session.proxyUser.getOrElse(""))
        val logs = if (hasViewAccess) {
          session.logLines()
        } else {
          Nil
        }
        MockSessionView(session.id, session.owner, session.proxyUser, logs)
      }
    }
  }
}

class SessionServletSpec extends BaseSessionServletSpec[Session, RecoveryMetadata] {
  import SessionServletSpec._

  override def createServlet(): SessionServlet[Session, RecoveryMetadata] = {
    SessionServletSpec.createServlet(createConf())
  }

  private val aliceHeaders = makeUserHeaders("alice")
  private val bobHeaders = makeUserHeaders("bob")

  private def delete(id: Int, headers: Map[String, String], expectedStatus: Int): Unit = {
    jdelete[Map[String, Any]](s"/$id", headers = headers, expectedStatus = expectedStatus) { _ =>
      // Nothing to do.
    }
  }

  describe("SessionServlet") {

    it("should return correct Location in header") {
      // mount to "/sessions/*" to test. If request URI is "/session", getPathInfo() will
      // return null, since there's no extra path.
      // mount to "/*" will always return "/", so that it cannot reflect the issue.
      addServlet(servlet, "/sessions/*")
      jpost[MockSessionView]("/sessions", Map(), headers = aliceHeaders) { res =>
        assert(header("Location") === "/sessions/0")
        jdelete[Map[String, Any]]("/sessions/0", SC_OK, aliceHeaders) { _ => }
      }
    }

    it("should attach owner information to sessions") {
      jpost[MockSessionView]("/", Map()) { res =>
        assert(res.owner === null)
        assert(res.proxyUser === None)
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        assert(res.owner === "alice")
        assert(res.proxyUser === Some("alice"))
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, aliceHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        assert(res.owner === ADMIN)
        assert(res.proxyUser === Some("alice"))
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should allow other users to see all information due to ACLs not enabled") {
      jpost[MockSessionView]("/", Map()) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === null)
          assert(res.proxyUser === None)
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        jget[MockSessionView](s"/${res.id}") { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, aliceHeaders, SC_OK)
      }

      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          assert(res.logs === IndexedSeq("log"))
        }

        jget[MockSessionView](s"/${res.id}?doAs=bob", headers = adminHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, aliceHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === ADMIN)
          assert(res.proxyUser === Some("alice"))
          assert(res.logs === IndexedSeq("log"))
        }
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should allow non-owners to modify sessions") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        delete(res.id, bobHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        delete(res.id, bobHeaders, SC_OK)
      }
    }

    it("should not allow regular users to impersonate others") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }

      jpost[MockSessionView]("/?doAs=bob", Map(), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }
    }

    it("should allow admins to impersonate anyone") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = adminHeaders) { res =>
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=bob", Map(), headers = adminHeaders) { res =>
        delete(res.id, adminHeaders, SC_OK)
      }
    }
  }
}

class AclsEnabledSessionServletSpec extends BaseSessionServletSpec[Session, RecoveryMetadata] {

  import SessionServletSpec._

  override def createServlet(): SessionServlet[Session, RecoveryMetadata] = {
    val conf = createConf().set(LivyConf.ACCESS_CONTROL_ENABLED, true)
    SessionServletSpec.createServlet(conf)
  }

  private val aliceHeaders = makeUserHeaders("alice")
  private val bobHeaders = makeUserHeaders("bob")

  private def delete(id: Int, headers: Map[String, String], expectedStatus: Int): Unit = {
    jdelete[Map[String, Any]](s"/$id", headers = headers, expectedStatus = expectedStatus) { _ =>
      // Nothing to do.
    }
  }

  describe("SessionServlet") {
    it("should attach owner information to sessions") {
      jpost[MockSessionView]("/", Map()) { res =>
        assert(res.owner === null)
        assert(res.proxyUser === None)
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        assert(res.owner === "alice")
        assert(res.proxyUser === Some("alice"))
        assert(res.logs === IndexedSeq("log"))
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should only allow view accessible users to see non-sensitive information") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          // Other user cannot see the logs
          assert(res.logs === Nil)
        }

        jget[MockSessionView](s"/${res.id}?doAs=bob", headers = adminHeaders) { res =>
          assert(res.owner === "alice")
          assert(res.proxyUser === Some("alice"))
          // Other user cannot see the logs
          assert(res.logs === Nil)
        }

        // Users with access permission could see the logs
        jget[MockSessionView](s"/${res.id}", headers = aliceHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = viewUserHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = modifyUserHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = adminHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }

        delete(res.id, aliceHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        jget[MockSessionView](s"/${res.id}", headers = bobHeaders) { res =>
          assert(res.owner === ADMIN)
          assert(res.proxyUser === Some("alice"))
          // Other user cannot see the logs
          assert(res.logs === Nil)
        }

        // Users with access permission could see the logs
        jget[MockSessionView](s"/${res.id}", headers = viewUserHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = modifyUserHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }
        jget[MockSessionView](s"/${res.id}", headers = adminHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }

        // LIVY-592: Proxy user cannot view its session log
        // Proxy user should be able to see its session log
        jget[MockSessionView](s"/${res.id}", headers = aliceHeaders) { res =>
          assert(res.logs === IndexedSeq("log"))
        }

        delete(res.id, adminHeaders, SC_OK)
      }
    }

    it("should only allow modify accessible users from modifying sessions") {
      jpost[MockSessionView]("/", Map(), headers = aliceHeaders) { res =>
        delete(res.id, bobHeaders, SC_FORBIDDEN)
        delete(res.id, viewUserHeaders, SC_FORBIDDEN)
        delete(res.id, modifyUserHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        delete(res.id, bobHeaders, SC_FORBIDDEN)
        delete(res.id, viewUserHeaders, SC_FORBIDDEN)
        delete(res.id, modifyUserHeaders, SC_OK)
      }

      // LIVY-592: Proxy user cannot view its session log
      // Proxy user should be able to modify its session
      jpost[MockSessionView]("/?doAs=alice", Map(), headers = adminHeaders) { res =>
        delete(res.id, aliceHeaders, SC_OK)
      }
    }

    it("should not allow regular users to impersonate others") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }

      jpost[MockSessionView]("/?doAs=bob", Map(), headers = aliceHeaders,
        expectedStatus = SC_FORBIDDEN) { _ => }
    }

    it("should allow admins to impersonate anyone") {
      jpost[MockSessionView]("/", Map(PROXY_USER -> "bob"), headers = adminHeaders) { res =>
        delete(res.id, aliceHeaders, SC_FORBIDDEN)
        delete(res.id, adminHeaders, SC_OK)
      }

      jpost[MockSessionView]("/?doAs=bob", Map(), headers = adminHeaders) { res =>
        delete(res.id, aliceHeaders, SC_FORBIDDEN)
        delete(res.id, adminHeaders, SC_OK)
      }
    }
  }
}
