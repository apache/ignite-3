/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.network.recovery;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.network.handshake.HandshakeManager;
import org.apache.ignite.internal.network.netty.HandshakeHandler;
import org.apache.ignite.internal.network.netty.MessageHandler;
import org.apache.ignite.internal.network.netty.NettySender;

/**
 * Base recovery handshake manager.
 */
public abstract class BaseRecoveryHandshakeManager implements HandshakeManager {
    /** Handshake completion future. */
    protected final CompletableFuture<NettySender> handshakeCompleteFuture = new CompletableFuture<>();

    /** Remote node's launch id. */
    protected UUID remoteLaunchId;

    /** Remote node's consistent id. */
    protected String remoteConsistentId;

    /** Netty pipeline channel handler context. */
    protected ChannelHandlerContext ctx;

    /** Channel. */
    protected Channel channel;

    /** Netty pipeline handshake handler. */
    protected HandshakeHandler handler;

    /** {@inheritDoc} */
    @Override
    public void onInit(ChannelHandlerContext handlerContext) {
        this.ctx = handlerContext;
        this.channel = handlerContext.channel();
        this.handler = (HandshakeHandler) ctx.handler();
    }

    /**
     * Creates a message handler using the consistent id of a remote node.
     *
     * @return New message handler.
     */
    public MessageHandler createMessageHandler() {
        return handler.createMessageHandler(remoteConsistentId);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<NettySender> handshakeFuture() {
        return handshakeCompleteFuture;
    }

    /**
     * Finishes handshaking process by removing handshake handler from the pipeline and creating a {@link NettySender}.
     */
    protected void finishHandshake() {
        // Removes handshake handler from the pipeline as the handshake is finished
        this.ctx.pipeline().remove(this.handler);

        handshakeCompleteFuture.complete(new NettySender(channel, remoteLaunchId.toString(), remoteConsistentId));
    }
}
