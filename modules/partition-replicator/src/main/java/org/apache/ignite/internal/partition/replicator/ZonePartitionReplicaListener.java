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

package org.apache.ignite.internal.partition.replicator;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicaResult;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.listener.ReplicaListener;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.replicator.message.ReplicaSafeTimeSyncRequest;
import org.apache.ignite.internal.replicator.message.TableAware;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.message.TxFinishReplicaRequest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Zone partition replica listener.
 */
public class ZonePartitionReplicaListener implements ReplicaListener {
    private static final IgniteLogger LOG = Loggers.forClass(ZonePartitionReplicaListener.class);

    // TODO: https://issues.apache.org/jira/browse/IGNITE-22624 await for the table replica listener if needed.
    private final Map<TablePartitionId, ReplicaListener> replicas = new ConcurrentHashMap<>();

    private final RaftCommandRunner raftClient;

    private final TxFinishReplicaRequestHandler txFinishReplicaRequestHandler;

    /**
     * The constructor.
     *
     * @param raftClient Raft client.
     */
    public ZonePartitionReplicaListener(
            TxStatePartitionStorage txStatePartitionStorage,
            ClockService clockService,
            TxManager txManager,
            ValidationSchemasSource validationSchemasSource,
            SchemaSyncService schemaSyncService,
            CatalogService catalogService,
            RaftCommandRunner raftClient,
            ZonePartitionId replicationGroupId
    ) {
        this.raftClient = raftClient;

        txFinishReplicaRequestHandler = new TxFinishReplicaRequestHandler(
                txStatePartitionStorage,
                clockService,
                txManager,
                validationSchemasSource,
                schemaSyncService,
                catalogService,
                raftClient,
                replicationGroupId
        );
    }

    @Override
    public CompletableFuture<ReplicaResult> invoke(ReplicaRequest request, UUID senderId) {
        if (!(request instanceof TableAware)) {
            // TODO: https://issues.apache.org/jira/browse/IGNITE-22620 implement ReplicaSafeTimeSyncRequest processing.
            if (request instanceof TxFinishReplicaRequest) {
                return txFinishReplicaRequestHandler.handle((TxFinishReplicaRequest) request)
                        .thenApply(res -> new ReplicaResult(res, null));
            } else {
                if (request instanceof ReplicaSafeTimeSyncRequest) {
                    LOG.debug("Non table request is not supported by the zone partition yet " + request);
                } else {
                    LOG.warn("Non table request is not supported by the zone partition yet " + request);
                }
            }

            return completedFuture(new ReplicaResult(null, null));
        } else {
            int partitionId;

            ReplicationGroupId replicationGroupId = request.groupId().asReplicationGroupId();

            // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 Refine this code when the zone based replication will done.
            if (replicationGroupId instanceof  TablePartitionId) {
                partitionId = ((TablePartitionId) replicationGroupId).partitionId();
            } else if (replicationGroupId instanceof ZonePartitionId) {
                partitionId = ((ZonePartitionId) replicationGroupId).partitionId();
            } else {
                throw new IllegalArgumentException("Requests with replication group type "
                        + request.groupId().getClass() + " is not supported");
            }

            return replicas.get(new TablePartitionId(((TableAware) request).tableId(), partitionId))
                    .invoke(request, senderId);
        }
    }

    @Override
    public RaftCommandRunner raftClient() {
        return raftClient;
    }

    /**
     * Add table partition listener to the current zone replica listener.
     *
     * @param partitionId Table partition id.
     * @param replicaListener Table replica listener.
     */
    public void addTableReplicaListener(TablePartitionId partitionId, Function<RaftCommandRunner, ReplicaListener> replicaListener) {
        replicas.put(partitionId, replicaListener.apply(raftClient));
    }

    /**
     * Return table replicas listeners.
     *
     * @return Table replicas listeners.
     */
    @VisibleForTesting
    public Map<TablePartitionId, ReplicaListener> tableReplicaListeners() {
        return replicas;
    }

    @Override
    public void onShutdown() {
        replicas.forEach((id, listener) -> {
                    try {
                        listener.onShutdown();
                    } catch (Throwable th) {
                        LOG.error("Error during table partition listener stop for [tableId="
                                        + id.tableId() + ", partitionId=" + id.partitionId() + "].",
                                th
                        );
                    }
                }
        );
    }
}
