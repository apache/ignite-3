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

package org.apache.ignite.internal.partition.replicator.raft;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.UUID;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing.PartitionSnapshots;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.StorageUpdateHandler;
import org.apache.ignite.internal.table.distributed.index.IndexMetaStorage;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.raft.snapshot.SnapshotAwarePartitionDataStorage;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SafeTimeValuesTracker;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ZonePartitionRaftListenerTest extends BaseIgniteAbstractTest {
    private static final int ZONE_ID = 0;

    private static final int PARTITION_ID = 0;

    private static final ZonePartitionKey ZONE_PARTITION_KEY = new ZonePartitionKey(ZONE_ID, PARTITION_ID);

    private ZonePartitionRaftListener listener;

    @Mock
    private OutgoingSnapshotsManager outgoingSnapshotsManager;

    @Mock
    private TxManager txManager;

    @Mock
    private TxStatePartitionStorage txStatePartitionStorage;

    @BeforeEach
    void setUp() {
        listener = new ZonePartitionRaftListener(
                txStatePartitionStorage,
                txManager,
                new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE),
                new PendingComparableValuesTracker<>(0L),
                new ZonePartitionId(ZONE_ID, PARTITION_ID),
                outgoingSnapshotsManager
        );
    }

    @Test
    void closesOngoingSnapshots(@Mock PartitionSnapshots partitionSnapshots) {
        var tablePartition1 = new TablePartitionId(1, PARTITION_ID);
        var tablePartition2 = new TablePartitionId(2, PARTITION_ID);

        listener.addTableProcessor(tablePartition1, partitionListener(tablePartition1));
        listener.addTableProcessor(tablePartition2, partitionListener(tablePartition2));

        listener.onShutdown();

        verify(outgoingSnapshotsManager).cleanupOutgoingSnapshots(ZONE_PARTITION_KEY);
    }

    private PartitionListener partitionListener(TablePartitionId tablePartitionId) {
        return new PartitionListener(
                txManager,
                new SnapshotAwarePartitionDataStorage(
                        tablePartitionId.tableId(),
                        mock(MvPartitionStorage.class),
                        outgoingSnapshotsManager,
                        ZONE_PARTITION_KEY
                ),
                mock(StorageUpdateHandler.class),
                txStatePartitionStorage,
                new SafeTimeValuesTracker(HybridTimestamp.MIN_VALUE),
                new PendingComparableValuesTracker<>(0L),
                mock(CatalogService.class),
                mock(SchemaRegistry.class),
                mock(IndexMetaStorage.class),
                UUID.randomUUID(),
                mock(MinimumRequiredTimeCollectorService.class)
        );
    }
}
