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
import static org.apache.ignite.internal.deployunit.metastore.key.UnitKey.allUnits;
import static org.apache.ignite.internal.deployunit.metastore.key.UnitKey.clusterStatusKey;
import static org.apache.ignite.internal.deployunit.metastore.key.UnitKey.nodeStatusKey;
import static org.apache.ignite.internal.deployunit.metastore.key.UnitKey.nodes;
import static org.apache.ignite.internal.deployunit.metastore.key.UnitMetaSerializer.deserialize;
import static org.apache.ignite.internal.deployunit.metastore.key.UnitMetaSerializer.serialize;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.rest.api.deployment.DeploymentStatus.UPLOADING;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.deployunit.UnitStatus;
import org.apache.ignite.internal.deployunit.UnitStatuses;
import org.apache.ignite.internal.deployunit.version.Version;
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

    public DeploymentUnitStoreImpl(MetaStorageManager metaStorage) {
        this.metaStorage = metaStorage;
    }

    @Override
    public CompletableFuture<UnitStatus> getClusterStatus(String id, Version version) {
        return metaStorage.get(clusterStatusKey(id, version)).thenApply(entry -> {
            byte[] value = entry.value();
            if (value == null) {
                return null;
            }

            return deserialize(value);
        });
    }

    @Override
    public CompletableFuture<UnitStatus> getNodeStatus(String id, Version version, String nodeId) {
        return metaStorage.get(nodeStatusKey(id, version, nodeId)).thenApply(entry -> {
            byte[] value = entry.value();
            if (value == null) {
                return null;
            }

            return deserialize(value);
        });
    }

    @Override
    public CompletableFuture<List<UnitStatuses>> getAllClusterStatuses() {
        CompletableFuture<List<UnitStatuses>> result = new CompletableFuture<>();
        metaStorage.prefix(allUnits()).subscribe(new UnitsAccumulator().toSubscriber(result));
        return result;
    }

    private CompletableFuture<List<UnitStatuses>> getClusterStatuses(Predicate<UnitStatus> filter) {
        CompletableFuture<List<UnitStatuses>> result = new CompletableFuture<>();
        metaStorage.prefix(allUnits()).subscribe(new UnitsAccumulator(filter).toSubscriber(result));
        return result;
    }

    @Override
    public CompletableFuture<UnitStatuses> getClusterStatuses(String id) {
        CompletableFuture<UnitStatuses> result = new CompletableFuture<>();
        metaStorage.prefix(allUnits()).subscribe(new ClusterStatusAccumulator(id).toSubscriber(result));
        return result;
    }

    @Override
    public CompletableFuture<Boolean> createClusterStatus(String id, Version version) {
        ByteArray key = clusterStatusKey(id, version);
        byte[] value = serialize(new UnitStatus(id, version, UPLOADING));

        return metaStorage.invoke(notExists(key), put(key, value), noop());
    }

    @Override
    public CompletableFuture<Boolean> createNodeStatus(String id, Version version, String nodeId, DeploymentStatus status) {
        ByteArray key = nodeStatusKey(id, version, nodeId);
        byte[] value = serialize(new UnitStatus(id, version, status));
        return metaStorage.invoke(notExists(key), put(key, value), noop());
    }

    @Override
    public CompletableFuture<Boolean> updateClusterStatus(String id, Version version, DeploymentStatus status) {
        return updateStatus(clusterStatusKey(id, version), status);
    }

    @Override
    public CompletableFuture<Boolean> updateNodeStatus(String id, Version version, String nodeId, DeploymentStatus status) {
        return updateStatus(nodeStatusKey(id, version, nodeId), status);
    }

    @Override
    public CompletableFuture<List<UnitStatuses>> findAllByNodeConsistentId(String nodeId) {
        CompletableFuture<List<String>> result = new CompletableFuture<>();
        metaStorage.prefix(nodes()).subscribe(new UnitsByNodeAccumulator(nodeId).toSubscriber(result));
        return result.thenCompose(ids -> getClusterStatuses(meta -> ids.contains(meta.id())));
    }

    @Override
    public CompletableFuture<Boolean> remove(String id, Version version) {
        ByteArray key = clusterStatusKey(id, version);
        CompletableFuture<List<byte[]>> nodesFuture = new CompletableFuture<>();
        metaStorage.prefix(nodes(id, version)).subscribe(new KeyAccumulator().toSubscriber(nodesFuture));

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
     * @param status Deployment unit meta transformer.
     * @return Future with update result.
     */
    private CompletableFuture<Boolean> updateStatus(ByteArray key, DeploymentStatus status) {
        return metaStorage.get(key)
                .thenCompose(e -> {
                    if (e.value() == null) {
                        return completedFuture(false);
                    }
                    UnitStatus prev = deserialize(e.value());

                    if (status.compareTo(prev.status()) <= 0) {
                        return completedFuture(false);
                    }

                    prev.updateStatus(status);

                    boolean force = status == DeploymentStatus.OBSOLETE;

                    return metaStorage.invoke(
                                    force ? exists(key) : revision(key).le(e.revision()),
                                    put(key, serialize(prev)),
                                    noop())
                            .thenCompose(finished -> {
                                if (!finished && !force) {
                                    return updateStatus(key, status);
                                }
                                return completedFuture(finished);
                            });
                });
    }
}
