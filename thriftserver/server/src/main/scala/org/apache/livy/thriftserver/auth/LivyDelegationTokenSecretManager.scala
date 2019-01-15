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

package org.apache.livy.thriftserver.auth

import java.io.{ByteArrayInputStream, DataInputStream, IOException}

import org.apache.hadoop.io.Text
import org.apache.hadoop.security.token.Token
import org.apache.hadoop.security.token.delegation.{AbstractDelegationTokenIdentifier, AbstractDelegationTokenSecretManager}

import org.apache.livy.LivyConf

/**
 * A secret manager. It is taken from analogous implementation in the MapReduce client.
 */
class LivyDelegationTokenSecretManager(val livyConf: LivyConf)
  extends AbstractDelegationTokenSecretManager[LivyDelegationTokenIdentifier](
    livyConf.getTimeAsMs(LivyConf.THRIFT_DELEGATION_KEY_UPDATE_INTERVAL),
    livyConf.getTimeAsMs(LivyConf.THRIFT_DELEGATION_TOKEN_MAX_LIFETIME),
    livyConf.getTimeAsMs(LivyConf.THRIFT_DELEGATION_TOKEN_RENEW_INTERVAL),
    livyConf.getTimeAsMs(LivyConf.THRIFT_DELEGATION_TOKEN_GC_INTERVAL)) {

  override def createIdentifier: LivyDelegationTokenIdentifier = new LivyDelegationTokenIdentifier

  /**
   * Verify token string
   */
  @throws[IOException]
  def verifyDelegationToken(tokenStrForm: String): String = {
    val t = new Token[LivyDelegationTokenIdentifier]
    t.decodeFromUrlString(tokenStrForm)
    val id = getTokenIdentifier(t)
    verifyToken(id, t.getPassword)
    id.getUser.getShortUserName
  }

  @throws[IOException]
  protected def getTokenIdentifier(
      token: Token[LivyDelegationTokenIdentifier]): LivyDelegationTokenIdentifier = {
    // turn bytes back into identifier for cache lookup
    val buf = new ByteArrayInputStream(token.getIdentifier)
    val in = new DataInputStream(buf)
    val id = createIdentifier
    id.readFields(in)
    id
  }
}

/**
 * A delegation token identifier.
 *
 * @param owner    the effective username of the token owner
 * @param renewer  the username of the renewer
 * @param realUser the real username of the token owne
 */
class LivyDelegationTokenIdentifier(owner: Text, renewer: Text, realUser: Text)
  extends AbstractDelegationTokenIdentifier(owner, renewer, realUser) {

  def this() = this(new Text(), new Text(), new Text())

  override def getKind: Text = LivyDelegationTokenIdentifier.LIVY_DELEGATION_KIND
}

object LivyDelegationTokenIdentifier {
  val LIVY_DELEGATION_KIND = new Text("LIVY_DELEGATION_TOKEN")
}
