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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link VolatilePageMemoryTableStorage}.
 */
@ExtendWith(ConfigurationExtension.class)
public class VolatilePageMemoryMvTableStorageTest extends AbstractMvTableStorageTest {
    private final PageEvictionTracker pageEvictionTracker = spy(PageEvictionTrackerNoOp.INSTANCE);

    @BeforeEach
    void setUp(
            @InjectConfiguration
            VolatilePageMemoryStorageEngineConfiguration engineConfig,
            @InjectConfiguration("mock.tables.foo {}")
            TablesConfiguration tablesConfig,
            @InjectConfiguration("mock { partitions = 512, dataStorage.name = " + VolatilePageMemoryStorageEngine.ENGINE_NAME + "}")
            DistributionZoneConfiguration distributionZoneConfiguration
    ) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        initialize(new VolatilePageMemoryStorageEngine("node", engineConfig, ioRegistry, pageEvictionTracker),
                tablesConfig, distributionZoneConfiguration);
    }

    @Test
    void partitionDestructionFreesPartitionPages() throws Exception {
        MvPartitionStorage partitionStorage = getOrCreateMvPartition(0);

        insertOneRow(partitionStorage);

        long emptyDataPagesBeforeDestroy = dataRegion().rowVersionFreeList().emptyDataPages();

        assertThat(tableStorage.destroyPartition(0), willSucceedFast());

        assertMvDataDestructionCompletes(emptyDataPagesBeforeDestroy);
    }

    private void assertMvDataDestructionCompletes(long emptyDataPagesBeforeDestroy)
            throws InterruptedException, IgniteInternalCheckedException {
        assertTrue(waitForCondition(
                () -> dataRegion().rowVersionFreeList().emptyDataPages() > emptyDataPagesBeforeDestroy,
                5_000
        ));

        // Make sure that some page storing row versions gets emptied (so we can be sure that row versions
        // get removed).
        verify(pageEvictionTracker, timeout(5_000)).forgetPage(anyLong());
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

        long emptyDataPagesBeforeDestroy = dataRegion().rowVersionFreeList().emptyDataPages();

        assertThat(tableStorage.destroy(), willSucceedFast());

        assertMvDataDestructionCompletes(emptyDataPagesBeforeDestroy);
    }

    @Test
    void partitionDestructionFreesHashIndexPages() throws Exception {
        getOrCreateMvPartition(0);

        HashIndexStorage indexStorage = tableStorage.getOrCreateHashIndex(0, hashIdx.id());

        indexStorage.put(nonInlinableIndexRow());

        // Using RowVersionFreeList to track removal because RowVersionFreeList is used as a ReuseList for IndexColumnsFreeList.
        long emptyIndexPagesBeforeDestroy = dataRegion().rowVersionFreeList().emptyDataPages();

        assertThat(tableStorage.destroyPartition(0), willSucceedFast());

        assertIndexDataDestructionCompletes(emptyIndexPagesBeforeDestroy);
    }

    private static IndexRowImpl nonInlinableIndexRow() {
        RowId rowId = new RowId(PARTITION_ID);

        BinaryTupleSchema schema = BinaryTupleSchema.create(new Element[]{
                new Element(NativeTypes.INT32, false),
                new Element(NativeTypes.stringOf(300), false)
        });

        ByteBuffer buffer = new BinaryTupleBuilder(schema.elementCount(), schema.hasNullableElements())
                .appendInt(1)
                .appendString("a".repeat(300))
                .build();

        BinaryTuple tuple = new BinaryTuple(schema, buffer);

        return new IndexRowImpl(tuple, rowId);
    }

    private void assertIndexDataDestructionCompletes(long emptyIndexPagesBeforeDestroy)
            throws InterruptedException, IgniteInternalCheckedException {
        // Using RowVersionFreeList to track removal because RowVersionFreeList is used as a ReuseList for IndexColumnsFreeList.
        assertTrue(waitForCondition(
                () -> dataRegion().rowVersionFreeList().emptyDataPages() > emptyIndexPagesBeforeDestroy,
                5_000
        ));

        // Make sure that some page storing index columns gets emptied (so we can be sure that index columns
        // get removed).
        verify(pageEvictionTracker, timeout(5_000)).forgetPage(anyLong());
    }

    @Test
    void partitionDestructionFreesSortedIndexPages() throws Exception {
        getOrCreateMvPartition(0);

        SortedIndexStorage indexStorage = tableStorage.getOrCreateSortedIndex(0, sortedIdx.id());

        indexStorage.put(nonInlinableIndexRow());

        // Using RowVersionFreeList to track removal because RowVersionFreeList is used as a ReuseList for IndexColumnsFreeList.
        long emptyIndexPagesBeforeDestroy = dataRegion().rowVersionFreeList().emptyDataPages();

        assertThat(tableStorage.destroyPartition(0), willSucceedFast());

        assertIndexDataDestructionCompletes(emptyIndexPagesBeforeDestroy);
    }

    @Test
    void tableStorageDestructionFreesHashIndexPages() throws Exception {
        getOrCreateMvPartition(0);

        HashIndexStorage indexStorage = tableStorage.getOrCreateHashIndex(0, hashIdx.id());

        indexStorage.put(nonInlinableIndexRow());

        // Using RowVersionFreeList to track removal because RowVersionFreeList is used as a ReuseList for IndexColumnsFreeList.
        long emptyIndexPagesBeforeDestroy = dataRegion().rowVersionFreeList().emptyDataPages();

        assertThat(tableStorage.destroy(), willSucceedFast());

        assertIndexDataDestructionCompletes(emptyIndexPagesBeforeDestroy);
    }

    @Test
    void tableStorageDestructionFreesSortedIndexPages() throws Exception {
        getOrCreateMvPartition(0);

        SortedIndexStorage indexStorage = tableStorage.getOrCreateSortedIndex(0, sortedIdx.id());

        indexStorage.put(nonInlinableIndexRow());

        // Using RowVersionFreeList to track removal because RowVersionFreeList is used as a ReuseList for IndexColumnsFreeList.
        long emptyIndexPagesBeforeDestroy = dataRegion().rowVersionFreeList().emptyDataPages();

        assertThat(tableStorage.destroy(), willSucceedFast());

        assertIndexDataDestructionCompletes(emptyIndexPagesBeforeDestroy);
    }

    private VolatilePageMemoryDataRegion dataRegion() {
        return ((VolatilePageMemoryTableStorage) tableStorage).dataRegion();
    }
}
