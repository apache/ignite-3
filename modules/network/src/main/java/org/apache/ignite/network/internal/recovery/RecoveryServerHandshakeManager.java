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

package org.apache.ignite.network.internal.recovery;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.internal.handshake.HandshakeAction;
import org.apache.ignite.network.internal.handshake.HandshakeException;
import org.apache.ignite.network.internal.handshake.HandshakeManager;
import org.apache.ignite.network.internal.netty.NettySender;
import org.apache.ignite.network.internal.netty.NettyUtils;
import org.apache.ignite.network.internal.recovery.message.HandshakeMessageFactory;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartMessage;
import org.apache.ignite.network.internal.recovery.message.HandshakeStartResponseMessage;

/**
 * Recovery protocol handshake manager for a server.
 */
public class RecoveryServerHandshakeManager implements HandshakeManager {
    /** Launch id. */
    private final UUID launchId;

    /** Consistent id. */
    private final String consistentId;

    /** Handshake completion future. */
    private final CompletableFuture<NettySender> handshakeCompleteFuture = new CompletableFuture<>();

    /**
     * Constructor.
     *
     * @param launchId Launch id.
     * @param consistentId Consistent id.
     */
    public RecoveryServerHandshakeManager(UUID launchId, String consistentId) {
        this.launchId = launchId;
        this.consistentId = consistentId;
    }

    /** {@inheritDoc} */
    @Override public HandshakeAction init(Channel channel) {
        return HandshakeAction.NOOP;
    }

    /** {@inheritDoc} */
    @Override public HandshakeAction onConnectionOpen(Channel channel) {
        HandshakeStartMessage handshakeStartMessage = HandshakeMessageFactory.handshakeStartMessage()
            .launchId(launchId)
            .consistentId(consistentId)
            .build();

        ChannelFuture sendFuture = channel.writeAndFlush(handshakeStartMessage);

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, throwable) -> {
            if (throwable != null)
                handshakeCompleteFuture.completeExceptionally(
                    new HandshakeException("Failed to send handshake start message: " + throwable.getMessage(), throwable)
                );
        });

        return HandshakeAction.NOOP;
    }

    /** {@inheritDoc} */
    @Override public HandshakeAction onMessage(Channel channel, NetworkMessage message) {
        if (message instanceof HandshakeStartResponseMessage) {
            HandshakeStartResponseMessage msg = (HandshakeStartResponseMessage) message;

            handshakeCompleteFuture.complete(new NettySender(channel, msg.launchId().toString(), msg.consistentId()));

            return HandshakeAction.REMOVE_HANDLER;
        }

        handshakeCompleteFuture.completeExceptionally(
            new HandshakeException("Unexpected message during handshake: " + message.toString())
        );

        return HandshakeAction.FAIL;
    }

    /** {@inheritDoc} */
    @Override public CompletableFuture<NettySender> handshakeFuture() {
        return handshakeCompleteFuture;
    }
}
