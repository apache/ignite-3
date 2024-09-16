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

import org.apache.ignite.internal.cluster.management.network.messages.CancelInitMessage;
import org.apache.ignite.internal.cluster.management.network.messages.ClusterStateMessage;
import org.apache.ignite.internal.cluster.management.network.messages.CmgInitMessage;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Callback used by the {@link CmgMessageHandler} to notify about incoming messages.
 */
public interface CmgMessageCallback {
    /**
     * Notifies about an incoming {@link ClusterStateMessage}.
     */
    void onClusterStateMessageReceived(ClusterStateMessage message, ClusterNode sender, @Nullable Long correlationId);

    /**
     * Notifies about an incoming {@link CancelInitMessage}.
     */
    void onCancelInitMessageReceived(CancelInitMessage message, ClusterNode sender, @Nullable Long correlationId);

    /**
     * Notifies about an incoming {@link CmgInitMessage}.
     */
    void onCmgInitMessageReceived(CmgInitMessage message, ClusterNode sender, @Nullable Long correlationId);
}
