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

package org.apache.ignite.internal.deployunit.metastore;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.DeploymentStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;

/**
 * Metastore for deployment units.
 */
public interface DeploymentUnitStore {
    /**
     * Registers node statuses change events listener.
     *
     * @param listener Node statuses update listener.
     */
    void registerNodeStatusListener(NodeStatusWatchListener listener);

    /**
     * Unregisters node statuses change events listener.
     *
     * @param listener Node statuses update listener.
     */
    void unregisterNodeStatusListener(NodeStatusWatchListener listener);

    /**
     * Registers cluster statuses change events listener.
     *
     * @param listener Cluster statuses update listener.
     */
    void registerClusterStatusListener(ClusterStatusWatchListener listener);

    /**
     * Unregisters cluster statuses change events listener.
     *
     * @param listener Cluster statuses update listener.
     */
    void unregisterClusterStatusListener(ClusterStatusWatchListener listener);

    /**
     * Returns cluster statuses of all existed deployment units.
     *
     * @return Cluster statuses of all existed deployment units.
     */
    CompletableFuture<List<UnitClusterStatus>> getClusterStatuses();

    /**
     * Returns cluster status of deployment unit with provided identifier.
     *
     * @param id Deployment unit identifier.
     * @return Cluster status of deployment unit with provided identifier.
     */
    CompletableFuture<List<UnitClusterStatus>> getClusterStatuses(String id);

    /**
     * Returns cluster status of deployment unit.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Cluster status of deployment unit.
     */
    CompletableFuture<UnitClusterStatus> getClusterStatus(String id, Version version);

    /**
     * Returns node status of deployment unit.
     *
     * @param nodeId Node consistent identifier.
     * @return Node status of deployment unit.
     */
    CompletableFuture<List<UnitNodeStatus>> getNodeStatuses(String nodeId);

    /**
     * Returns node status of deployment unit.
     *
     * @param nodeId Node consistent identifier.
     * @param unitId Deployment unit identifier.
     * @return Node status of deployment unit.
     */
    CompletableFuture<List<UnitNodeStatus>> getNodeStatuses(String nodeId, String unitId);

    /**
     * Returns node status of deployment unit.
     *
     * @param nodeId Node consistent identifier.
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Node status of deployment unit.
     */
    CompletableFuture<UnitNodeStatus> getNodeStatus(String nodeId, String id, Version version);

    /**
     * Create new cluster status for deployment unit.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return Future with {@code true} result if status created successfully
     *          or with {@code false} if status with provided {@param id} and {@param version} already existed.
     */
    CompletableFuture<UnitClusterStatus> createClusterStatus(String id, Version version, Set<String> nodesToDeploy);

    /**
     * Create new node status for deployment unit.
     *
     * @param nodeId Node consistent identifier.
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @param opId Deployment unit creation operation identifier.
     * @param status Initial deployment status.
     * @return Future with {@code true} result if status created successfully or with {@code false} if status with provided {@param id} and
     *         {@param version} and {@param nodeId} already existed.
     */
    CompletableFuture<Boolean> createNodeStatus(
            String nodeId,
            String id,
            Version version,
            UUID opId,
            DeploymentStatus status);

    /**
     * Updates cluster status for deployment unit.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment version identifier.
     * @param status New deployment status.
     * @return Future with {@code true} result if status updated successfully.
     */
    CompletableFuture<Boolean> updateClusterStatus(String id, Version version, DeploymentStatus status);

    /**
     * Updates node status for deployment unit.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment version identifier.
     * @param status New deployment status.
     * @return Future with {@code true} result if status updated successfully.
     */
    CompletableFuture<Boolean> updateNodeStatus(String nodeId, String id, Version version, DeploymentStatus status);

    /**
     * Returns a list of nodes where unit with provided identifier and version is deployed.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return A list of nodes where unit with provided identifier and version is deployed or empty list.
     */
    CompletableFuture<List<String>> getAllNodes(String id, Version version);

    /**
     * Returns a list of node statuses where unit with provided identifier and version is deployed.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment unit version.
     * @return A list of node statuses where unit with provided identifier and version is deployed or empty list.
     */
    CompletableFuture<List<UnitNodeStatus>> getAllNodeStatuses(String id, Version version);

    /**
     * Removes cluster status.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment version identifier.
     * @param opId Operation identifier.
     * @return Future with {@code true} result if removed successfully.
     */
    CompletableFuture<Boolean> removeClusterStatus(String id, Version version, UUID opId);

    /**
     * Removes node status.
     *
     * @param id Deployment unit identifier.
     * @param version Deployment version identifier.
     * @param opId Operation identifier.
     * @return Future with {@code true} result if removed successfully.
     */
    CompletableFuture<Boolean> removeNodeStatus(String nodeId, String id, Version version, UUID opId);
}
