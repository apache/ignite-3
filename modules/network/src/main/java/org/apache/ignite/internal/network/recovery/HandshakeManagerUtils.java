/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import static java.util.Collections.emptyList;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.netty.NettyUtils;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.network.OutNetworkObject;

class HandshakeManagerUtils {
    private static final NetworkMessagesFactory MESSAGE_FACTORY = new NetworkMessagesFactory();

    static void sendRejectionMessageAndFailHandshake(
            String message,
            HandshakeRejectionReason rejectionReason,
            Channel channel,
            CompletableFuture<NettySender> handshakeFuture,
            Function<String, Exception> exceptionFactory
    ) {
        sendRejectionMessageAndFailHandshake(message, message, rejectionReason, channel, handshakeFuture, exceptionFactory);
    }

    static void sendRejectionMessageAndFailHandshake(
            String exceptionText,
            String messageText,
            HandshakeRejectionReason rejectionReason,
            Channel channel,
            CompletableFuture<NettySender> handshakeFuture,
            Function<String, Exception> exceptionFactory
    ) {
        HandshakeRejectedMessage rejectionMessage = MESSAGE_FACTORY.handshakeRejectedMessage()
                .reasonString(rejectionReason.name())
                .message(messageText)
                .build();

        ChannelFuture sendFuture = channel.writeAndFlush(new OutNetworkObject(rejectionMessage, emptyList(), false));

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, ex) -> {
            // Ignoring ex as it's more important to tell the client about the rejection reason.
            handshakeFuture.completeExceptionally(exceptionFactory.apply(exceptionText));
        });
    }
}
