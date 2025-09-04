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

package org.apache.ignite.internal.deployunit;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.deployunit.metastore.ClusterStatusWatchListener;
import org.apache.ignite.internal.deployunit.metastore.DeploymentUnitStore;
import org.apache.ignite.internal.deployunit.metastore.NodeStatusWatchListener;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;

public class StubDeploymentUnitStore implements DeploymentUnitStore {
    private final Map<String, Map<Version, UnitClusterStatus>> clusterStore = new HashMap<>();

    private final Map<String, Map<String, Map<Version, UnitNodeStatus>>> nodeStore = new HashMap<>();

    @Override
    public void registerNodeStatusListener(NodeStatusWatchListener listener) {

    }

    @Override
    public void unregisterNodeStatusListener(NodeStatusWatchListener listener) {

    }

    @Override
    public void registerClusterStatusListener(ClusterStatusWatchListener listener) {

    }

    @Override
    public void unregisterClusterStatusListener(ClusterStatusWatchListener listener) {

    }

    @Override
    public CompletableFuture<List<UnitClusterStatus>> getClusterStatuses() {
        return null;
    }

    @Override
    public CompletableFuture<List<UnitClusterStatus>> getClusterStatuses(String id) {
        Map<Version, UnitClusterStatus> map = clusterStore.get(id);
        List<UnitClusterStatus> result = map == null ? emptyList() : new ArrayList<>(map.values());
        return completedFuture(result);
    }

    @Override
    public CompletableFuture<UnitClusterStatus> getClusterStatus(String id, Version version) {
        Map<Version, UnitClusterStatus> map = clusterStore.get(id);
        UnitClusterStatus result = map == null ? null : map.get(version);
        return completedFuture(result);
    }

    @Override
    public CompletableFuture<List<UnitNodeStatus>> getNodeStatuses(String nodeId) {
        Map<String, Map<Version, UnitNodeStatus>> nodeContent = nodeStore.get(nodeId);
        if (nodeContent == null) {
            return completedFuture(emptyList());
        }
        List<UnitNodeStatus> result = new ArrayList<>();

        for (Map<Version, UnitNodeStatus> value : nodeContent.values()) {
            result.addAll(value.values());
        }

        return completedFuture(result);
    }

    @Override
    public CompletableFuture<List<UnitNodeStatus>> getNodeStatuses(String nodeId, String unitId) {
        Map<String, Map<Version, UnitNodeStatus>> nodeContent = nodeStore.get(nodeId);
        if (nodeContent == null) {
            return completedFuture(emptyList());
        }

        Map<Version, UnitNodeStatus> map = nodeContent.get(unitId);
        if (map == null) {
            return completedFuture(emptyList());
        }

        return completedFuture(new ArrayList<>(map.values()));

    }

    @Override
    public CompletableFuture<UnitNodeStatus> getNodeStatus(String nodeId, String id, Version version) {
        Map<String, Map<Version, UnitNodeStatus>> nodeContent = nodeStore.get(nodeId);
        if (nodeContent == null) {
            return completedFuture(null);
        }

        Map<Version, UnitNodeStatus> map = nodeContent.get(id);
        if (map == null) {
            return completedFuture(null);
        }

        return completedFuture(map.get(version));
    }

    @Override
    public CompletableFuture<UnitClusterStatus> createClusterStatus(String id, Version version, Set<String> nodesToDeploy) {
        UnitClusterStatus status = new UnitClusterStatus(id, version, DeploymentStatus.UPLOADING, UUID.randomUUID(), nodesToDeploy);

        Map<Version, UnitClusterStatus> map = clusterStore.computeIfAbsent(id, k -> new HashMap<>());
        if (map.containsKey(version)) {
            return completedFuture(null);
        } else {
            map.put(version, status);
            return completedFuture(status);
        }
    }

    @Override
    public CompletableFuture<Boolean> createNodeStatus(String nodeId, String id, Version version, UUID opId, DeploymentStatus status) {
        UnitNodeStatus nodeStatus = new UnitNodeStatus(id, version, DeploymentStatus.UPLOADING, UUID.randomUUID(), nodeId);

        Map<String, Map<Version, UnitNodeStatus>> nodesMap = nodeStore.computeIfAbsent(nodeId, k -> new HashMap<>());
        Map<Version, UnitNodeStatus> map = nodesMap.computeIfAbsent(id, k -> new HashMap<>());
        if (map.containsKey(version)) {
            return completedFuture(false);
        } else {
            map.put(version, nodeStatus);
            return completedFuture(true);
        }
    }

    @Override
    public CompletableFuture<Boolean> updateClusterStatus(String id, Version version, DeploymentStatus status) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> updateNodeStatus(String nodeId, String id, Version version, DeploymentStatus status) {
        return null;
    }

    @Override
    public CompletableFuture<List<String>> getAllNodes(String id, Version version) {
        return null;
    }

    @Override
    public CompletableFuture<List<UnitNodeStatus>> getAllNodeStatuses(String id, Version version) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> removeClusterStatus(String id, Version version, UUID opId) {
        return null;
    }

    @Override
    public CompletableFuture<Boolean> removeNodeStatus(String nodeId, String id, Version version, UUID opId) {
        return null;
    }
}
