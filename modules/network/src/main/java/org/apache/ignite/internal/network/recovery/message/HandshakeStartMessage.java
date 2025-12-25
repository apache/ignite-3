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

package org.apache.ignite.internal.network.recovery.message;

import java.util.UUID;
import org.apache.ignite.internal.network.NetworkMessageTypes;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.network.message.ClusterNodeMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Handshake start message, contains info about the node.
 * This message is sent from an acceptor to an initiator at the connection opening.
 */
@Transferable(NetworkMessageTypes.HANDSHAKE_START)
public interface HandshakeStartMessage extends StaleNodeHandlingParams, InternalMessage {
    /** Returns the acceptor node that sends this. */
    ClusterNodeMessage serverNode();

    /** ID of the cluster to which the acceptor node belongs ({@code null} if it's not initialized yet. */
    @Nullable
    UUID serverClusterId();

    /** Product name of the node that sends the message. */
    String productName();

    /** Product version of the node that sends the message. */
    String productVersion();

    @Override
    int physicalTopologySize();

    @Override
    long topologyVersion();
}
