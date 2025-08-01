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

package org.apache.ignite.internal.disaster.system.message;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.jetbrains.annotations.Nullable;

/**
 * Message for resetting the cluster.
 */
@Transferable(SystemDisasterRecoveryMessageGroup.RESET_CLUSTER)
public interface ResetClusterMessage extends NetworkMessage, Serializable {
    /**
     * Consistent IDs of nodes that will host the new CMG.
     */
    Set<String> newCmgNodes();

    /**
     * Consistent IDs of nodes that currently host the Meta Storage.
     */
    Set<String> currentMetaStorageNodes();

    /**
     * Name of the cluster that will be a part of the generated cluster tag.
     */
    String clusterName();

    /**
     * New ID of the cluster.
     */
    UUID clusterId();

    /**
     * Initial cluster configuration ({@code null} if no initial configuration was passed on init).
     */
    @Nullable
    @IgniteToStringExclude
    String initialClusterConfiguration();

    /**
     * IDs that the cluster had before (including the current incarnation by which this message is sent).
     */
    List<UUID> formerClusterIds();

    /**
     * Number of nodes in the Raft voting member set for Metastorage. Only non-null if Metastorage is to be repaired.
     */
    @Nullable Integer metastorageReplicationFactor();

    /**
     * Consistent ID of the node that conducts the cluster reset. Only non-null if Metastorage is to be repaired.
     */
    @Nullable String conductor();

    /**
     * Consistent IDs of the nodes which were sent the message. Only non-null if Metastorage is to be repaired.
     */
    @Nullable Set<String> participatingNodes();

    /**
     * Returns whether metastorage repair is requested.
     */
    default boolean metastorageRepairRequested() {
        return metastorageReplicationFactor() != null;
    }
}
