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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.DEPLOYED;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.deployunit.metastore.accumulator.ClusterStatusAccumulator;
import org.apache.ignite.internal.deployunit.metastore.accumulator.KeyAccumulator;
import org.apache.ignite.internal.deployunit.metastore.accumulator.NodeStatusAccumulator;
import org.apache.ignite.internal.deployunit.metastore.status.ClusterStatusKey;
import org.apache.ignite.internal.deployunit.metastore.status.NodeStatusKey;
import org.apache.ignite.internal.deployunit.metastore.status.UnitClusterStatus;
import org.apache.ignite.internal.deployunit.metastore.status.UnitNodeStatus;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.rest.api.deployment.DeploymentStatus;
import org.apache.ignite.lang.ByteArray;

/**
 * Implementation of {@link DeploymentUnitStore} based on {@link MetaStorageManager}.
 */
public class DeploymentUnitStoreImpl implements DeploymentUnitStore {
    private final MetaStorageManager metaStorage;

    /**
     * Constructor.
     *
     * @param metaStorage Meta storage manager.
     */
    public DeploymentUnitStoreImpl(MetaStorageManager metaStorage) {
        this.metaStorage = metaStorage;
    }

    @Override
    public void registerListener(NodeStatusWatchListener listener) {
        metaStorage.registerPrefixWatch(NodeStatusKey.builder().build().toByteArray(), listener);
    }

    @Override
    public CompletableFuture<List<UnitClusterStatus>> getAllClusterStatuses() {
        CompletableFuture<List<UnitClusterStatus>> result = new CompletableFuture<>();
        metaStorage.prefix(ClusterStatusKey.builder().build().toByteArray())
                .subscribe(new ClusterStatusAccumulator().toSubscriber(result));
        return result;
    }

    @Override
    public CompletableFuture<List<UnitClusterStatus>> getClusterStatuses(String id) {
        CompletableFuture<List<UnitClusterStatus>> result = new CompletableFuture<>();
        metaStorage.prefix(ClusterStatusKey.builder().id(id).build().toByteArray())
                .subscribe(new ClusterStatusAccumulator().toSubscriber(result));
        return result;
    }

    @Override
    public CompletableFuture<UnitClusterStatus> getClusterStatus(String id, Version version) {
        return metaStorage.get(ClusterStatusKey.builder().id(id).version(version).build().toByteArray()).thenApply(entry -> {
            byte[] value = entry.value();
            if (value == null) {
                return null;
            }

            return UnitClusterStatus.deserialize(value);
        });
    }

    @Override
    public CompletableFuture<List<UnitNodeStatus>> getNodeStatuses(String nodeId) {
        CompletableFuture<List<UnitNodeStatus>> result = new CompletableFuture<>();
        metaStorage.prefix(NodeStatusKey.builder().build().toByteArray())
                .subscribe(new NodeStatusAccumulator(unitNodeStatus -> Objects.equals(unitNodeStatus.nodeId(), nodeId))
                        .toSubscriber(result));
        return result;
    }

    @Override
    public CompletableFuture<List<UnitNodeStatus>> getNodeStatuses(String nodeId, String id) {
        CompletableFuture<List<UnitNodeStatus>> result = new CompletableFuture<>();
        metaStorage.prefix(NodeStatusKey.builder().id(id).build().toByteArray())
                .subscribe(new NodeStatusAccumulator(unitNodeStatus -> Objects.equals(unitNodeStatus.nodeId(), nodeId))
                        .toSubscriber(result));
        return result;
    }

    @Override
    public CompletableFuture<UnitNodeStatus> getNodeStatus(String nodeId, String id, Version version) {
        return metaStorage.get(NodeStatusKey.builder().id(id).version(version).nodeId(nodeId).build().toByteArray())
                .thenApply(entry -> {
                    byte[] value = entry.value();
                    if (value == null) {
                        return null;
                    }

                    return UnitNodeStatus.deserialize(value);
                });
    }

    @Override
    public CompletableFuture<Boolean> createClusterStatus(String id, Version version, Set<String> nodes) {
        ByteArray key = ClusterStatusKey.builder().id(id).version(version).build().toByteArray();
        byte[] value = UnitClusterStatus.serialize(new UnitClusterStatus(id, version, UPLOADING, nodes));

        return metaStorage.invoke(notExists(key), put(key, value), noop());
    }

    @Override
    public CompletableFuture<Boolean> createNodeStatus(String nodeId, String id, Version version, DeploymentStatus status) {
        ByteArray key = NodeStatusKey.builder().id(id).version(version).nodeId(nodeId).build().toByteArray();
        byte[] value = UnitNodeStatus.serialize(new UnitNodeStatus(id, version, status, nodeId));
        return metaStorage.invoke(notExists(key), put(key, value), noop());
    }

    @Override
    public CompletableFuture<Boolean> updateClusterStatus(String id, Version version, DeploymentStatus status) {
        return updateStatus(ClusterStatusKey.builder().id(id).version(version).build().toByteArray(), bytes -> {
            UnitClusterStatus prev = UnitClusterStatus.deserialize(bytes);

            if (status.compareTo(prev.status()) <= 0) {
                return null;
            }

            prev.updateStatus(status);
            return UnitClusterStatus.serialize(prev);
        }, status == DEPLOYED);
    }

    @Override
    public CompletableFuture<Boolean> updateNodeStatus(String nodeId, String id, Version version, DeploymentStatus status) {
        return updateStatus(NodeStatusKey.builder().id(id).version(version).nodeId(nodeId).build().toByteArray(), bytes -> {
            UnitNodeStatus prev = UnitNodeStatus.deserialize(bytes);

            if (status.compareTo(prev.status()) <= 0) {
                return null;
            }

            prev.updateStatus(status);
            return UnitNodeStatus.serialize(prev);
        }, status == DEPLOYED);
    }

    @Override
    public CompletableFuture<List<String>> getAllNodes(String id, Version version) {
        CompletableFuture<List<UnitNodeStatus>> result = new CompletableFuture<>();
        ByteArray nodes = NodeStatusKey.builder().id(id).version(version).build().toByteArray();
        metaStorage.prefix(nodes).subscribe(new NodeStatusAccumulator(status -> status.status() == DEPLOYED).toSubscriber(result));
        return result.thenApply(status -> status.stream().map(UnitNodeStatus::nodeId).collect(Collectors.toList()));
    }

    @Override
    public CompletableFuture<Boolean> remove(String id, Version version) {
        ByteArray key = ClusterStatusKey.builder().id(id).version(version).build().toByteArray();
        CompletableFuture<List<byte[]>> nodesFuture = new CompletableFuture<>();
        metaStorage.prefix(NodeStatusKey.builder().id(id).version(version).build().toByteArray())
                .subscribe(new KeyAccumulator().toSubscriber(nodesFuture));

        return nodesFuture.thenCompose(nodes ->
                metaStorage.invoke(existsAll(key, nodes), removeAll(key, nodes), Collections.emptyList())
        );
    }

    private static Condition existsAll(ByteArray key, List<byte[]> nodeKeys) {
        Condition result = exists(key);
        for (byte[] keyArr : nodeKeys) {
            result = Conditions.and(result, exists(new ByteArray(keyArr)));
        }
        return result;
    }

    private static Collection<Operation> removeAll(ByteArray key, List<byte[]> keys) {
        List<Operation> operations = new ArrayList<>();
        operations.add(Operations.remove(key));

        keys.stream().map(ByteArray::new).map(Operations::remove).collect(Collectors.toCollection(() -> operations));
        return operations;
    }

    /**
     * Update deployment unit meta.
     *
     * @param key Status key.
     * @param mapper Status map function.
     * @param force Force update.
     * @return Future with update result.
     */
    private CompletableFuture<Boolean> updateStatus(ByteArray key, Function<byte[], byte[]> mapper, boolean force) {
        return metaStorage.get(key)
                .thenCompose(e -> {
                    byte[] value = e.value();
                    if (value == null) {
                        return completedFuture(false);
                    }
                    byte[] newValue = mapper.apply(value);


                    if (newValue == null) {
                        return completedFuture(false);
                    }

                    return metaStorage.invoke(
                                    force ? exists(key) : revision(key).le(e.revision()),
                                    put(key, newValue),
                                    noop())
                            .thenCompose(finished -> {
                                if (!finished && !force) {
                                    return updateStatus(key, mapper, false);
                                }
                                return completedFuture(finished);
                            });
                });
    }

}
