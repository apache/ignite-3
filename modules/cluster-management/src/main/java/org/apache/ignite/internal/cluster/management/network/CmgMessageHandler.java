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

package org.apache.ignite.internal.cluster.management.network;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.ClusterStateMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Network message handler for CMG messages.
 *
 * <p>This handler buffers incoming messages until the parent component signals that its local recovery has been complete (by calling
 * {@link #onRecoveryComplete}) after which it passes the buffered messages to the registered callback and then continues to behave like a
 * regular message handler.
 */
public class CmgMessageHandler implements NetworkMessageHandler {
    private static final IgniteLogger LOG = Loggers.forClass(ClusterManagementGroupManager.class);

    private final IgniteSpinBusyLock busyLock;

    private final CmgMessagesFactory msgFactory;

    private final ClusterService clusterService;

    private final CmgMessageCallback cmgMessageCallback;

    /**
     * A queue for incoming messages that buffers them until the parent component is ready to process them.
     *
     * <p>This field is set to {@code null} after the {@link #onRecoveryComplete} method has been called to free the used memory and to
     * signal that the parent component recovery has been complete.
     *
     * <p>Concurrent access is guarded by {@link #messageQueueMux}.
     */
    @Nullable
    private List<NetworkMessageContext> messageQueue = new ArrayList<>();

    private final Object messageQueueMux = new Object();

    private static class NetworkMessageContext {
        final NetworkMessage message;

        final ClusterNode sender;

        @Nullable
        final Long correlationId;

        NetworkMessageContext(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
            this.message = message;
            this.sender = sender;
            this.correlationId = correlationId;
        }
    }

    /**
     * Constructor.
     *
     * @param busyLock Start-stop lock of the enclosing Ignite component.
     * @param msgFactory Network message factory.
     * @param clusterService Network service.
     * @param cmgMessageCallback Message callback.
     */
    public CmgMessageHandler(
            IgniteSpinBusyLock busyLock,
            CmgMessagesFactory msgFactory,
            ClusterService clusterService,
            CmgMessageCallback cmgMessageCallback
    ) {
        this.busyLock = busyLock;
        this.msgFactory = msgFactory;
        this.clusterService = clusterService;
        this.cmgMessageCallback = cmgMessageCallback;
    }

    @Override
    public void onReceived(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
        if (!busyLock.enterBusy()) {
            if (correlationId != null) {
                clusterService.messagingService().respond(sender, initFailed(new NodeStoppingException()), correlationId);
            }

            return;
        }

        try {
            synchronized (messageQueueMux) {
                if (messageQueue != null) {
                    messageQueue.add(new NetworkMessageContext(message, sender, correlationId));

                    return;
                }
            }

            if (message instanceof ClusterStateMessage) {
                cmgMessageCallback.onClusterStateMessageReceived((ClusterStateMessage) message, sender, correlationId);
            } else if (message instanceof CancelInitMessage) {
                cmgMessageCallback.onCancelInitMessageReceived((CancelInitMessage) message, sender, correlationId);
            } else if (message instanceof CmgInitMessage) {
                cmgMessageCallback.onCmgInitMessageReceived((CmgInitMessage) message, sender, correlationId);
            }
        } catch (Exception e) {
            LOG.error("CMG message handling failed", e);

            if (correlationId != null) {
                clusterService.messagingService().respond(sender, initFailed(e), correlationId);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Notifies this handler that the CMG local recovery has been complete and we can start processing new messages instead of buffering
     * them.
     */
    public void onRecoveryComplete() {
        synchronized (messageQueueMux) {
            assert messageQueue != null;

            List<NetworkMessageContext> localQueue = messageQueue;

            messageQueue = null;

            for (NetworkMessageContext messageContext : localQueue) {
                onReceived(messageContext.message, messageContext.sender, messageContext.correlationId);
            }
        }
    }

    private NetworkMessage initFailed(Exception e) {
        return msgFactory.initErrorMessage().cause(e.getMessage()).build();
    }
}
