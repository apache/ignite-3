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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link VolatilePageMemoryTableStorage}.
 */
public class VolatilePageMemoryMvTableStorageTest extends AbstractMvTableStorageTest {
    private VolatilePageMemoryStorageEngine engine;

    @BeforeEach
    void setUp(
            @InjectConfiguration("mock.profiles.default = {engine = aimem}") StorageConfiguration storageConfig,
            @InjectConfiguration SystemLocalConfiguration systemConfig
    ) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new VolatilePageMemoryStorageEngine("node", storageConfig, systemConfig, ioRegistry, mock(FailureProcessor.class), clock);

        engine.start();

        initialize();
    }

    @AfterEach
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        IgniteUtils.closeAllManually(engine == null ? null : engine::stop);
    }

    @Override
    protected MvTableStorage createMvTableStorage() {
        return engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                indexDescriptorSupplier
        );
    }

    @Test
    void partitionDestructionFreesPartitionPages() throws Exception {
        MvPartitionStorage partitionStorage = getOrCreateMvPartition(0);

        insertOneRow(partitionStorage);

        long emptyDataPagesBeforeDestroy = dataRegion().freeList().emptyDataPages();

        assertThat(tableStorage.destroyPartition(0), willSucceedFast());

        assertMvDataDestructionCompletes(emptyDataPagesBeforeDestroy);
    }

    private void assertMvDataDestructionCompletes(long emptyDataPagesBeforeDestroy)
            throws InterruptedException, IgniteInternalCheckedException {
        assertTrue(waitForCondition(
                () -> dataRegion().freeList().emptyDataPages() > emptyDataPagesBeforeDestroy,
                5_000
        ));
    }

    private void insertOneRow(MvPartitionStorage partitionStorage) {
        BinaryRow binaryRow = binaryRow(new TestKey(0, "0"), new TestValue(1, "1"));

        partitionStorage.runConsistently(locker -> {
            RowId rowId = new RowId(PARTITION_ID);

            locker.lock(rowId);

            partitionStorage.addWriteCommitted(rowId, binaryRow, clock.now());

            return null;
        });
    }

    @Test
    void tableStorageDestructionFreesPartitionsPages() throws Exception {
        MvPartitionStorage partitionStorage = getOrCreateMvPartition(0);

        insertOneRow(partitionStorage);

        long emptyDataPagesBeforeDestroy = dataRegion().freeList().emptyDataPages();

        assertThat(tableStorage.destroy(), willSucceedFast());

        assertMvDataDestructionCompletes(emptyDataPagesBeforeDestroy);
    }

    @Test
    void partitionDestructionFreesHashIndexPages() throws Exception {
        getOrCreateMvPartition(0);

        tableStorage.createHashIndex(0, hashIdx);
        IndexStorage indexStorage = tableStorage.getIndex(0, hashIdx.id());

        indexStorage.put(nonInlinableIndexRow());

        long emptyIndexPagesBeforeDestroy = dataRegion().freeList().emptyDataPages();

        assertThat(tableStorage.destroyPartition(0), willSucceedFast());

        assertIndexDataDestructionCompletes(emptyIndexPagesBeforeDestroy);
    }

    private static IndexRowImpl nonInlinableIndexRow() {
        RowId rowId = new RowId(PARTITION_ID);

        BinaryTupleSchema schema = BinaryTupleSchema.create(new Element[]{
                new Element(NativeTypes.INT32, false),
                new Element(NativeTypes.stringOf(300), false)
        });

        ByteBuffer buffer = new BinaryTupleBuilder(schema.elementCount())
                .appendInt(1)
                .appendString("a".repeat(300))
                .build();

        BinaryTuple tuple = new BinaryTuple(schema.elementCount(), buffer);

        return new IndexRowImpl(tuple, rowId);
    }

    private void assertIndexDataDestructionCompletes(long emptyIndexPagesBeforeDestroy)
            throws InterruptedException, IgniteInternalCheckedException {
        assertTrue(waitForCondition(
                () -> dataRegion().freeList().emptyDataPages() > emptyIndexPagesBeforeDestroy,
                5_000
        ));
    }

    @Test
    void partitionDestructionFreesSortedIndexPages() throws Exception {
        getOrCreateMvPartition(0);

        tableStorage.createSortedIndex(0, sortedIdx);
        SortedIndexStorage indexStorage = (SortedIndexStorage) tableStorage.getIndex(0, sortedIdx.id());

        indexStorage.put(nonInlinableIndexRow());

        long emptyIndexPagesBeforeDestroy = dataRegion().freeList().emptyDataPages();

        assertThat(tableStorage.destroyPartition(0), willSucceedFast());

        assertIndexDataDestructionCompletes(emptyIndexPagesBeforeDestroy);
    }

    @Test
    void tableStorageDestructionFreesHashIndexPages() throws Exception {
        getOrCreateMvPartition(0);

        tableStorage.createHashIndex(0, hashIdx);
        IndexStorage indexStorage = tableStorage.getIndex(0, hashIdx.id());

        indexStorage.put(nonInlinableIndexRow());

        long emptyIndexPagesBeforeDestroy = dataRegion().freeList().emptyDataPages();

        assertThat(tableStorage.destroy(), willSucceedFast());

        assertIndexDataDestructionCompletes(emptyIndexPagesBeforeDestroy);
    }

    @Test
    void tableStorageDestructionFreesSortedIndexPages() throws Exception {
        getOrCreateMvPartition(0);

        tableStorage.createSortedIndex(0, sortedIdx);
        SortedIndexStorage indexStorage = (SortedIndexStorage) tableStorage.getIndex(0, sortedIdx.id());

        indexStorage.put(nonInlinableIndexRow());

        long emptyIndexPagesBeforeDestroy = dataRegion().freeList().emptyDataPages();

        assertThat(tableStorage.destroy(), willSucceedFast());

        assertIndexDataDestructionCompletes(emptyIndexPagesBeforeDestroy);
    }

    @Test
    public void testDestroyTablesNoLeakages() {
        int limit = 100000;
        for (int i = 1; i < limit; i++) {
            MvTableStorage mvTableStorage = createMvTableStorage();

            getOrCreateMvPartition(mvTableStorage, PARTITION_ID);

            assertThat(mvTableStorage.destroy(), willCompleteSuccessfully());
        }
    }

    private VolatilePageMemoryDataRegion dataRegion() {
        return ((VolatilePageMemoryTableStorage) tableStorage).dataRegion();
    }
}
