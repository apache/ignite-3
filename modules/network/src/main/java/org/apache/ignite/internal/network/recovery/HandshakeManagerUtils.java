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
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.FailureType;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.NetworkMessagesFactory;
import org.apache.ignite.internal.network.OutNetworkObject;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.network.message.ClusterNodeMessage;
import org.apache.ignite.internal.network.netty.NettySender;
import org.apache.ignite.internal.network.netty.NettyUtils;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectedMessage;
import org.apache.ignite.internal.network.recovery.message.HandshakeRejectionReason;
import org.apache.ignite.internal.network.recovery.message.StaleNodeHandlingParameters;
import org.apache.ignite.internal.tostring.S;

class HandshakeManagerUtils {
    private static final IgniteLogger LOG = Loggers.forClass(HandshakeManagerUtils.class);

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
        rejectionReason.print(false, LOG, "Rejecting handshake: {}", messageText);

        HandshakeRejectedMessage rejectionMessage = MESSAGE_FACTORY.handshakeRejectedMessage()
                .reasonString(rejectionReason.name())
                .message(messageText)
                .build();

        ChannelFuture sendFuture = channel.writeAndFlush(new OutNetworkObject(rejectionMessage, emptyList()));

        NettyUtils.toCompletableFuture(sendFuture).whenComplete((unused, ex) -> {
            // Ignoring ex as it's more important to tell the other side about the rejection reason.
            handshakeFuture.completeExceptionally(exceptionFactory.apply(exceptionText));
        });
    }

    static ClusterNodeMessage clusterNodeToMessage(InternalClusterNode node) {
        return MESSAGE_FACTORY.clusterNodeMessage()
                .id(node.id())
                .name(node.name())
                .host(node.address().host())
                .port(node.address().port())
                .build();
    }

    static Exception createExceptionFromRejectionMessage(HandshakeRejectedMessage msg) {
        return msg.reason() == HandshakeRejectionReason.STOPPING
                ? new RecipientLeftException(msg.message())
                : new HandshakeException(msg.message());
    }

    static void maybeFailOnStaleNodeDetection(
            FailureProcessor failureProcessor,
            StaleNodeHandlingParameters local,
            StaleNodeHandlingParameters remote,
            ClusterNodeMessage remoteNode
    ) {
        long localTopologyVersion = local.topologyVersion();
        long remoteTopologyVersion = remote.topologyVersion();

        if (localTopologyVersion >= remoteTopologyVersion) {
            return;
        }

        String message = S.toString(
                "Cluster segmentation detected, current node will be shut down",
                "logicalTopologyVersion", localTopologyVersion, false,
                "remoteLogicalTopologyVersion", remoteTopologyVersion, false,
                "remoteNodeId", remoteNode.id(), false,
                "remoteNodeName", remoteNode.name(), false
        );

        failureProcessor.process(new FailureContext(FailureType.CRITICAL_ERROR, null, message));
    }
}
