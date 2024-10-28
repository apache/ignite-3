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

package org.apache.ignite.internal.disaster.system;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.disaster.system.exception.ClusterResetException;
import org.apache.ignite.internal.disaster.system.exception.MigrateException;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.jetbrains.annotations.Nullable;

/**
 * Manages disaster recovery of system groups, namely the Cluster Management Group (CMG) and the Metastorage group (MG).
 */
public interface SystemDisasterRecoveryManager {
    /**
     * Saves cluster state to make sure it can be used to initiate CMG/MG repair.
     *
     * @param clusterState State to save.
     */
    void saveClusterState(ClusterState clusterState);

    /**
     * Marks this node as a node that saw initial configuration application. After this happens, the initial configuration
     * in CMG is not needed anymore and can be disposed.
     */
    void markInitConfigApplied();

    /**
     * Initiates cluster reset. Only CMG repair is requested, Metastorage is left intact.
     *
     * @param proposedCmgNodeNames Names of the nodes that will be the new CMG nodes.
     * @return Future completing with the result of the operation ({@link ClusterResetException} in case of error related to reset logic).
     */
    CompletableFuture<Void> resetCluster(List<String> proposedCmgNodeNames);

    /**
     * Initiates cluster reset. CMG will be reset and Metastorage will be repaired.
     *
     * @param proposedCmgNodeNames Names of the new CMG nodes. If not specified, the current CMG nodes are used.
     * @param metastorageReplicationFactor Number of nodes in the Raft voting member set for Metastorage.
     * @return Future completing with the result of the operation ({@link ClusterResetException} in case of error related to reset logic).
     */
    CompletableFuture<Void> resetClusterRepairingMetastorage(@Nullable List<String> proposedCmgNodeNames, int metastorageReplicationFactor);

    /**
     * Migrates nodes missed during CMG repair to the new cluster (which is the result of the repair). To do so, sends the
     * corresponding {@link ResetClusterMessage} to all nodes that are in the physical topology (including itself).
     *
     * @param targetClusterState State of the new cluster.
     * @return Future completing with the result of the operation ({@link MigrateException} in case of error related to reset logic).
     */
    CompletableFuture<Void> migrate(ClusterState targetClusterState);
}
