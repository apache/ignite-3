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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.schema.BinaryRowArgumentMatcher.equalToRow;
import static org.apache.ignite.internal.schema.SchemaTestUtils.binaryRow;
import static org.apache.ignite.internal.schema.SchemaUtils.columnMapper;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.tx.TransactionIds.beginTimestamp;
import static org.apache.ignite.internal.tx.TransactionIds.transactionId;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.INT64;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.DefaultValueProvider;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.storage.index.CatalogIndexStatusSupplier;
import org.apache.ignite.internal.table.distributed.gc.GcUpdateHandler;
import org.apache.ignite.internal.table.distributed.gc.MvGc;
import org.apache.ignite.internal.table.distributed.index.IndexUpdateHandler;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** For {@link PartitionAccessImpl} testing. */
public class PartitionAccessImplTest extends BaseIgniteAbstractTest {
    private static final int TABLE_ID = 1;

    private static final int INDEX_ID_0 = 2;

    private static final int INDEX_ID_1 = 3;

    private static final int TEST_PARTITION_ID = 0;

    private static final SchemaDescriptor SCHEMA_V0 = new SchemaDescriptor(
            1,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{new Column("v", INT32, true)}
    );

    private static final SchemaDescriptor SCHEMA_V1 = new SchemaDescriptor(
            2,
            new Column[]{new Column("k", INT32, false)},
            new Column[]{
                    new Column("v", INT32, true),
                    new Column("v2", INT64, true, DefaultValueProvider.constantProvider(100500L)),
            }
    );

    private static final SchemaRegistry SCHEMA_REGISTRY = new SchemaRegistryImpl(schemaVersion -> {
        if (schemaVersion == SCHEMA_V0.version()) {
            return SCHEMA_V0;
        } else if (schemaVersion == SCHEMA_V1.version()) {
            return SCHEMA_V1;
        }

        return null;
    }, SCHEMA_V0);

    @BeforeAll
    static void beforeAll() {
        SCHEMA_V1.columnMapping(columnMapper(SCHEMA_V0, SCHEMA_V1));
    }

    @Test
    void testMinMaxLastAppliedIndex() {
        TestMvTableStorage mvTableStorage = createMvTableStorage();
        TestTxStateTableStorage txStateTableStorage = new TestTxStateTableStorage();

        MvPartitionStorage mvPartitionStorage = createMvPartition(mvTableStorage, TEST_PARTITION_ID);
        TxStateStorage txStateStorage = txStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION_ID);

        PartitionAccess partitionAccess = createPartitionAccessImpl(mvTableStorage, txStateTableStorage);

        assertEquals(0, partitionAccess.minLastAppliedIndex());
        assertEquals(0, partitionAccess.maxLastAppliedIndex());

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(10, 1);
            txStateStorage.lastApplied(5, 1);

            return null;
        });

        assertEquals(5, partitionAccess.minLastAppliedIndex());
        assertEquals(10, partitionAccess.maxLastAppliedIndex());

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(15, 2);

            txStateStorage.lastApplied(20, 2);

            return null;
        });

        assertEquals(15, partitionAccess.minLastAppliedIndex());
        assertEquals(20, partitionAccess.maxLastAppliedIndex());
    }

    private static PartitionKey testPartitionKey() {
        return new PartitionKey(1, TEST_PARTITION_ID);
    }

    @Test
    void testMinMaxLastAppliedTerm() {
        TestMvTableStorage mvTableStorage = createMvTableStorage();
        TestTxStateTableStorage txStateTableStorage = new TestTxStateTableStorage();

        MvPartitionStorage mvPartitionStorage = createMvPartition(mvTableStorage, TEST_PARTITION_ID);
        TxStateStorage txStateStorage = txStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION_ID);

        PartitionAccess partitionAccess = createPartitionAccessImpl(mvTableStorage, txStateTableStorage);

        assertEquals(0, partitionAccess.minLastAppliedTerm());
        assertEquals(0, partitionAccess.maxLastAppliedTerm());

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(1, 10);
            txStateStorage.lastApplied(1, 5);

            return null;
        });

        assertEquals(5, partitionAccess.minLastAppliedTerm());
        assertEquals(10, partitionAccess.maxLastAppliedTerm());

        mvPartitionStorage.runConsistently(locker -> {
            mvPartitionStorage.lastApplied(2, 15);

            txStateStorage.lastApplied(2, 20);

            return null;
        });

        assertEquals(15, partitionAccess.minLastAppliedTerm());
        assertEquals(20, partitionAccess.maxLastAppliedTerm());
    }

    @Test
    void testAddWrite() {
        TestMvTableStorage mvTableStorage = createMvTableStorage();

        MvPartitionStorage mvPartitionStorage = createMvPartition(mvTableStorage, TEST_PARTITION_ID);

        IndexUpdateHandler indexUpdateHandler = mock(IndexUpdateHandler.class);

        FullStateTransferIndexChooser fullStateTransferIndexChooser = mock(FullStateTransferIndexChooser.class);

        PartitionAccess partitionAccess = createPartitionAccessImpl(mvTableStorage, indexUpdateHandler, fullStateTransferIndexChooser);

        List<IndexIdAndTableVersion> indexIds = List.of(
                new IndexIdAndTableVersion(INDEX_ID_0, SCHEMA_V0.version()),
                new IndexIdAndTableVersion(INDEX_ID_1, SCHEMA_V1.version())
        );

        when(fullStateTransferIndexChooser.chooseForAddWrite(anyInt(), anyInt(), any())).thenReturn(indexIds);

        RowId rowId = new RowId(TEST_PARTITION_ID);
        BinaryRow binaryRowV0 = binaryRow(SCHEMA_V0, 1, 2);
        BinaryRow binaryRowV1 = binaryRow(SCHEMA_V1, 1, 2, 100500L);
        UUID txId = transactionId(hybridTimestamp(System.currentTimeMillis()), 1);
        int commitTableId = 999;
        int snapshotCatalogVersion = 666;
        HybridTimestamp beginTs = beginTimestamp(txId);

        partitionAccess.addWrite(rowId, binaryRowV0, txId, commitTableId, TEST_PARTITION_ID, snapshotCatalogVersion);

        verify(mvPartitionStorage).addWrite(eq(rowId), same(binaryRowV0), eq(txId), eq(commitTableId), eq(TEST_PARTITION_ID));

        verify(fullStateTransferIndexChooser).chooseForAddWrite(eq(snapshotCatalogVersion), eq(TABLE_ID), eq(beginTs));

        verify(indexUpdateHandler).addToIndex(same(binaryRowV0), eq(rowId), eq(INDEX_ID_0));
        verify(indexUpdateHandler).addToIndex(argThat(equalToRow(binaryRowV1)), eq(rowId), eq(INDEX_ID_1));

        // Let's check with a null binaryRowV0.
        binaryRowV0 = null;

        clearInvocations(mvPartitionStorage, indexUpdateHandler, fullStateTransferIndexChooser);

        partitionAccess.addWrite(rowId, binaryRowV0, txId, commitTableId, TEST_PARTITION_ID, snapshotCatalogVersion);

        verify(mvPartitionStorage).addWrite(eq(rowId), same(binaryRowV0), eq(txId), eq(commitTableId), eq(TEST_PARTITION_ID));

        verify(fullStateTransferIndexChooser).chooseForAddWrite(eq(snapshotCatalogVersion), eq(TABLE_ID), eq(beginTs));

        verify(indexUpdateHandler, never()).addToIndex(any(), any(), anyInt());
    }

    @Test
    void testAddWriteCommitted() {
        TestMvTableStorage mvTableStorage = createMvTableStorage();

        MvPartitionStorage mvPartitionStorage = createMvPartition(mvTableStorage, TEST_PARTITION_ID);

        IndexUpdateHandler indexUpdateHandler = mock(IndexUpdateHandler.class);

        FullStateTransferIndexChooser fullStateTransferIndexChooser = mock(FullStateTransferIndexChooser.class);

        PartitionAccess partitionAccess = createPartitionAccessImpl(mvTableStorage, indexUpdateHandler, fullStateTransferIndexChooser);

        List<IndexIdAndTableVersion> indexIdTableVersionList = List.of(
                new IndexIdAndTableVersion(INDEX_ID_0, SCHEMA_V0.version()),
                new IndexIdAndTableVersion(INDEX_ID_1, SCHEMA_V1.version())
        );

        when(fullStateTransferIndexChooser.chooseForAddWriteCommitted(anyInt(), anyInt(), any())).thenReturn(indexIdTableVersionList);

        RowId rowId = new RowId(TEST_PARTITION_ID);
        BinaryRow binaryRowV0 = binaryRow(SCHEMA_V0, 1, 2);
        BinaryRow binaryRowV1 = binaryRow(SCHEMA_V1, 1, 2, 100500L);
        HybridTimestamp commitTimestamp = HybridTimestamp.MIN_VALUE.addPhysicalTime(100500);
        int snapshotCatalogVersion = 666;

        partitionAccess.addWriteCommitted(rowId, binaryRowV0, commitTimestamp, snapshotCatalogVersion);

        verify(mvPartitionStorage).addWriteCommitted(eq(rowId), same(binaryRowV0), eq(commitTimestamp));

        verify(fullStateTransferIndexChooser).chooseForAddWriteCommitted(eq(snapshotCatalogVersion), eq(TABLE_ID), eq(commitTimestamp));

        verify(indexUpdateHandler).addToIndex(same(binaryRowV0), eq(rowId), eq(INDEX_ID_0));
        verify(indexUpdateHandler).addToIndex(argThat(equalToRow(binaryRowV1)), eq(rowId), eq(INDEX_ID_1));

        // Let's check with a null binaryRowV0.
        binaryRowV0 = null;

        clearInvocations(mvPartitionStorage, indexUpdateHandler, fullStateTransferIndexChooser);

        partitionAccess.addWriteCommitted(rowId, binaryRowV0, commitTimestamp, snapshotCatalogVersion);

        verify(mvPartitionStorage).addWriteCommitted(eq(rowId), same(binaryRowV0), eq(commitTimestamp));

        verify(fullStateTransferIndexChooser).chooseForAddWriteCommitted(eq(snapshotCatalogVersion), eq(TABLE_ID), eq(commitTimestamp));

        verify(indexUpdateHandler, never()).addToIndex(any(), any(), anyInt());
    }

    private static MvPartitionStorage createMvPartition(MvTableStorage tableStorage, int partitionId) {
        CompletableFuture<MvPartitionStorage> createMvPartitionFuture = tableStorage.createMvPartition(partitionId);

        assertThat(createMvPartitionFuture, willCompleteSuccessfully());

        return createMvPartitionFuture.join();
    }

    private static PartitionAccessImpl createPartitionAccessImpl(
            MvTableStorage mvTableStorage,
            TxStateTableStorage txStateTableStorage
    ) {
        return new PartitionAccessImpl(
                testPartitionKey(),
                mvTableStorage,
                txStateTableStorage,
                mock(MvGc.class),
                mock(IndexUpdateHandler.class),
                mock(GcUpdateHandler.class),
                mock(FullStateTransferIndexChooser.class),
                SCHEMA_REGISTRY,
                mock(LowWatermark.class)
        );
    }

    private static PartitionAccessImpl createPartitionAccessImpl(
            MvTableStorage mvTableStorage,
            IndexUpdateHandler indexUpdateHandler,
            FullStateTransferIndexChooser fullStateTransferIndexChooser
    ) {
        return new PartitionAccessImpl(
                testPartitionKey(),
                mvTableStorage,
                new TestTxStateTableStorage(),
                mock(MvGc.class),
                indexUpdateHandler,
                mock(GcUpdateHandler.class),
                fullStateTransferIndexChooser,
                SCHEMA_REGISTRY,
                mock(LowWatermark.class)
        );
    }

    private static TestMvTableStorage createMvTableStorage() {
        return new TestMvTableStorage(TABLE_ID, DEFAULT_PARTITION_COUNT, mock(CatalogIndexStatusSupplier.class));
    }
}
