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

package org.apache.ignite.internal.raft.client;

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.handshake.HandshakeException;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SubscriptionLeaderChangeRequest;

/**
 * Handles sending subscription messages to RAFT group members with retry logic.
 */
class SubscriptionMessageSender {
    private static final IgniteLogger LOG = Loggers.forClass(SubscriptionMessageSender.class);

    private final ScheduledExecutorService executor;

    private final RaftConfiguration raftConfiguration;

    private final MessagingService messagingService;

    /**
     * Constructor.
     *
     * @param messagingService Messaging service.
     * @param executor Executor for async operations.
     * @param raftConfiguration RAFT configuration.
     */
    SubscriptionMessageSender(
            MessagingService messagingService,
            ScheduledExecutorService executor,
            RaftConfiguration raftConfiguration
    ) {
        this.messagingService = messagingService;
        this.executor = executor;
        this.raftConfiguration = raftConfiguration;
    }

    /**
     * Sends a subscription message to the specified node with retry logic.
     *
     * @param node Target node.
     * @param msg Subscription message to send.
     * @return Future that completes with {@code true} if the message was sent successfully,
     *         {@code false} if sending failed but should not be treated as an error,
     *         or completes exceptionally on unrecoverable errors.
     */
    CompletableFuture<Boolean> send(InternalClusterNode node, SubscriptionLeaderChangeRequest msg) {
        var msgSendFut = new CompletableFuture<Boolean>();

        sendWithRetry(node, msg, msgSendFut);

        return msgSendFut;
    }

    private void sendWithRetry(InternalClusterNode node, SubscriptionLeaderChangeRequest msg, CompletableFuture<Boolean> msgSendFut) {
        Long responseTimeout = raftConfiguration.responseTimeoutMillis().value();

        messagingService.invoke(node, msg, responseTimeout).whenCompleteAsync((unused, invokeThrowable) -> {
            if (invokeThrowable == null) {
                msgSendFut.complete(true);

                return;
            }

            Throwable retryCause = unwrapCause(invokeThrowable);
            if (!msg.subscribe()) {
                // We don't want to propagate exceptions when unsubscribing (if it's not an Error!).
                if (retryCause instanceof Error) {
                    msgSendFut.completeExceptionally(invokeThrowable);
                } else {
                    LOG.debug("An exception while trying to unsubscribe.", invokeThrowable);

                    msgSendFut.complete(false);
                }
            } else if (RaftErrorUtils.recoverable(retryCause)) {
                sendWithRetry(node, msg, msgSendFut);
            } else if (retryCause instanceof RecipientLeftException) {
                LOG.info(
                        "Could not subscribe to leader update from a specific node, because the node had left the cluster: [node={}].",
                        node
                );

                msgSendFut.complete(false);
            } else if (retryCause instanceof HandshakeException) {
                LOG.info(
                        "Could not subscribe to leader update from a specific node, because the handshake failed "
                                + "(node may be unavailable): [node={}].",
                        node
                );

                msgSendFut.complete(false);
            } else if (retryCause instanceof CancellationException) {
                LOG.info(
                        "Could not subscribe to leader update from a specific node, because the operation was cancelled "
                                + "(node may be stopping): [node={}].",
                        node
                );

                msgSendFut.complete(false);
            } else if (retryCause instanceof NodeStoppingException) {
                LOG.warn(
                        "Could not subscribe to leader update from a specific node, because the node is stopping: [node={}].",
                        node
                );

                msgSendFut.complete(false);
            } else {
                LOG.error("Could not send the subscribe message to the node: [node={}, msg={}].", invokeThrowable, node, msg);

                msgSendFut.completeExceptionally(invokeThrowable);
            }
        }, executor);
    }
}
