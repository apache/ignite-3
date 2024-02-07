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

package org.apache.ignite.internal.cluster.management.raft.commands;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * {@link ClusterNode} as a network message class.
 */
@Transferable(CmgMessageGroup.Commands.CLUSTER_NODE)
public interface ClusterNodeMessage extends NetworkMessage, Serializable {
    String id();

    String name();

    String host();

    int port();

    default ClusterNode asClusterNode() {
        return new ClusterNodeImpl(id(), name(), new NetworkAddress(host(), port()));
    }

    @Nullable
    Map<String, String> userAttributes();

    @Nullable
    Map<String, String> systemAttributes();

    @Nullable
    List<String> storageProfiles();
}
