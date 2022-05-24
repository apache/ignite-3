/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.cluster.management.network;

import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;

/**
 * Class for creating {@link NetworkMessageHandler} instances that share some common logic.
 */
public class CmgMessageHandlerFactory {
    private static final IgniteLogger LOG = IgniteLogger.forClass(ClusterManagementGroupManager.class);

    private final IgniteSpinBusyLock busyLock;

    private final CmgMessagesFactory msgFactory;

    private final ClusterService clusterService;

    /**
     * Constructor.
     *
     * @param busyLock Start-stop lock of the enclosing Ignite component.
     * @param msgFactory Network message factory.
     * @param clusterService Network service.
     */
    public CmgMessageHandlerFactory(IgniteSpinBusyLock busyLock, CmgMessagesFactory msgFactory, ClusterService clusterService) {
        this.busyLock = busyLock;
        this.msgFactory = msgFactory;
        this.clusterService = clusterService;
    }

    /**
     * Wraps a given {@code handler}, adding error reporting and handling of the enclosing Ignite component lifecycle.
     *
     * @param handler Handler for network messages.
     * @return Handler proxy with added common logic.
     */
    public NetworkMessageHandler wrapHandler(NetworkMessageHandler handler) {
        return (message, senderAddr, correlationId) -> {
            if (!busyLock.enterBusy()) {
                if (correlationId != null) {
                    clusterService.messagingService().respond(senderAddr, initFailed(new NodeStoppingException()), correlationId);
                }

                return;
            }

            try {
                handler.onReceived(message, senderAddr, correlationId);
            } catch (Exception e) {
                LOG.error("CMG message handling failed", e);

                if (correlationId != null) {
                    clusterService.messagingService().respond(senderAddr, initFailed(e), correlationId);
                }
            } finally {
                busyLock.leaveBusy();
            }
        };
    }

    private NetworkMessage initFailed(Exception e) {
        return msgFactory.initErrorMessage().cause(e.getMessage()).build();
    }
}
