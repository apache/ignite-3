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

package org.apache.ignite.internal.table.distributed.index;

import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toIndexDescriptor;
import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toTableDescriptor;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationUtils.findTableView;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;

/**
 * Controller for building indexes on primary replicas.
 *
 * <p>API notes:</p>
 * <ul>
 *     <li>When a replica becomes primary, we need to {@link #onBecomePrimary} to start building both new (via {@link #onIndexCreate}) and
 *     existing indexes for it;</li>
 *     <li>When a replica ceases to be the primary, {@link #onStopBeingPrimary} must be used to stop building all indexes for that replica
 *     (on that replica only) and prevent new indexes from being built on that replica (by {@link #onIndexCreate});</li>
 *     <li>When a new index is created, {@link #onIndexCreate} must be used to start building the index for all register primary replicas on
 *     which this index should be;</li>
 *     <li>When an index is dropped, {@link #onIndexDestroy} must be used to stop building indexes on all registered primary replicas that
 *     the index should be on;</li>
 *     <li>To avoid races between registering/unregistering primary replicas and creating/deleting indexes, the above methods are blocking
 *     and exclusive.</li>
 * </ul>
 */
public class IndexBuildController {
    private final IndexBuilder indexBuilder;

    private final TablesConfiguration tablesConfig;

    /** Guarded by {@code this}. */
    private final Map<TablePartitionId, IndexBuildReplica> replicaById = new HashMap<>();

    /**
     * Constructor.
     *
     * @param indexBuilder Index builder.
     * @param tablesConfig Tables configuration.
     */
    public IndexBuildController(IndexBuilder indexBuilder, TablesConfiguration tablesConfig) {
        this.indexBuilder = indexBuilder;
        this.tablesConfig = tablesConfig;
    }

    /**
     * Starts index building for all registered (by {@link #onBecomePrimary}) primary replicas.
     *
     * @param tableDescriptor Table descriptor.
     * @param indexDescriptor Index descriptor.
     */
    public synchronized void onIndexCreate(CatalogTableDescriptor tableDescriptor, CatalogIndexDescriptor indexDescriptor) {
        for (Entry<TablePartitionId, IndexBuildReplica> entry : replicaById.entrySet()) {
            TablePartitionId replicaId = entry.getKey();

            if (replicaId.tableId() == tableDescriptor.id()) {
                startIndexBuild(tableDescriptor, indexDescriptor, replicaId, entry.getValue());
            }
        }
    }

    /**
     * Stops index building for all registered (by {@link #onBecomePrimary}) primary replicas.
     *
     * @param tableDescriptor Table descriptor.
     * @param indexDescriptor Index descriptor.
     */
    public synchronized void onIndexDestroy(CatalogTableDescriptor tableDescriptor, CatalogIndexDescriptor indexDescriptor) {
        for (TablePartitionId replicaId : replicaById.keySet()) {
            if (replicaId.tableId() == tableDescriptor.id()) {
                indexBuilder.stopBuildIndex(tableDescriptor.id(), replicaId.partitionId(), indexDescriptor.id());
            }
        }
    }

    /**
     * Registers the primary replica to start index building on new indexes (by {@link #onIndexCreate}), and also starts index building for
     * existing indexes that should be on the replica.
     *
     * <p>It is expected that the replica will not be added a second time.</p>
     *
     * @param replicaId Replica ID.
     * @param tableStorage Multi-versioned table storage.
     * @param raftClient Raft client.
     */
    public synchronized void onBecomePrimary(TablePartitionId replicaId, MvTableStorage tableStorage, RaftGroupService raftClient) {
        IndexBuildReplica replica = replicaById.compute(replicaId, (tablePartitionId, indexBuildReplica) -> {
            assert indexBuildReplica == null : replicaId;

            return new IndexBuildReplica(tableStorage, raftClient);
        });

        TablesView tablesView = tablesConfig.value();

        for (TableIndexView indexView : tablesView.indexes()) {
            if (indexView.tableId() == replicaId.tableId()) {
                TableView tableView = findTableView(tablesView, indexView.tableId());

                assert tableView != null : "indexId=" + indexView.id() + ", tableId=" + indexView.tableId();

                startIndexBuild(toTableDescriptor(tableView), toIndexDescriptor(indexView), replicaId, replica);
            }
        }
    }

    /**
     * Unregisters the primary replica so that new indexes do not start building on it (by {@link #onIndexCreate}), and also stops all index
     * builds for this replica.
     *
     * <p>It is expected that the replica has been previously registered.</p>
     *
     * @param replicaId Replica ID.
     */
    public synchronized void onStopBeingPrimary(TablePartitionId replicaId) {
        IndexBuildReplica removed = replicaById.remove(replicaId);

        assert removed != null : replicaId;

        indexBuilder.stopBuildIndexes(replicaId.tableId(), replicaId.partitionId());
    }

    private void startIndexBuild(
            CatalogTableDescriptor tableDescriptor,
            CatalogIndexDescriptor indexDescriptor,
            TablePartitionId replicaId,
            IndexBuildReplica replica
    ) {
        int partitionId = replicaId.partitionId();

        indexBuilder.startBuildIndex(
                tableDescriptor.id(),
                partitionId,
                indexDescriptor.id(),
                replica.tableStorage.getOrCreateIndex(partitionId, StorageIndexDescriptor.create(tableDescriptor, indexDescriptor)),
                replica.tableStorage.getMvPartition(partitionId),
                replica.raftClient
        );
    }
}
