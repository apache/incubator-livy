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

package org.apache.livy.rsc.rpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of ChannelInboundHandler that dispatches incoming messages to an instance
 * method based on the method signature.
 * <p>
 * A handler's signature must be of the form:
 * <p>
 * <blockquote><tt>protected void handle(ChannelHandlerContext, MessageType)</tt></blockquote>
 * <p>
 * Where "MessageType" must match exactly the type of the message to handle. Polymorphism is not
 * supported. Handlers can return a value, which becomes the RPC reply; if a null is returned, then
 * a reply is still sent, with an empty payload.
 */
public abstract class RpcDispatcher extends SimpleChannelInboundHandler<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(RpcDispatcher.class);

  private final Map<Channel, Rpc> channelRpc = new ConcurrentHashMap<>();

  /**
   * Override this to add a name to the dispatcher, for debugging purposes.
   * @return The name of this dispatcher.
   */
  protected String name() {
    return getClass().getSimpleName();
  }

  public void registerRpc(Channel channel, Rpc rpc) {
    channelRpc.put(channel, rpc);
  }

  public void unregisterRpc(Channel channel) {
    channelRpc.remove(channel);
  }

  private Rpc getRpc(ChannelHandlerContext ctx) {
    Channel channel = ctx.channel();
    if (!channelRpc.containsKey(channel)) {
      throw new IllegalArgumentException("not existed channel:" + channel);
    }

    return channelRpc.get(channel);
  }

  @Override
  protected final void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
    getRpc(ctx).handleMsg(ctx, msg, getClass(), this);
  }

  @Override
  public final void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    getRpc(ctx).handleChannelException(ctx, cause);
  }

  @Override
  public final void channelInactive(ChannelHandlerContext ctx) throws Exception {
    getRpc(ctx).handleChannelInactive();
    super.channelInactive(ctx);
  }
}
