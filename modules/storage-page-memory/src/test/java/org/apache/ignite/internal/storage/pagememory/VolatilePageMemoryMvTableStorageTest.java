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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTracker;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
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
            @InjectConfiguration(
                    "mock.tables.foo{ partitions = 512, dataStorage.name = " + VolatilePageMemoryStorageEngine.ENGINE_NAME + "}"
            )
            TablesConfiguration tablesConfig
    ) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        initialize(new VolatilePageMemoryStorageEngine("node", engineConfig, ioRegistry, pageEvictionTracker), tablesConfig);
    }

    @Test
    void partitionDestructionFreesPartitionPages() throws Exception {
        MvPartitionStorage partitionStorage = tableStorage.getOrCreateMvPartition(0);

        insertOneRow(partitionStorage);

        long emptyDataPagesBeforeDestroy = dataRegion().rowVersionFreeList().emptyDataPages();

        assertThat(tableStorage.destroyPartition(0), willSucceedFast());

        assertDestructionCompletes(emptyDataPagesBeforeDestroy);
    }

    private void assertDestructionCompletes(long emptyDataPagesBeforeDestroy) throws InterruptedException, IgniteInternalCheckedException {
        assertTrue(waitForCondition(
                () -> dataRegion().rowVersionFreeList().emptyDataPages() > emptyDataPagesBeforeDestroy,
                5_000
        ));

        verify(pageEvictionTracker, times(1)).forgetPage(anyLong());
    }

    private void insertOneRow(MvPartitionStorage partitionStorage) {
        TableRow tableRow = tableRow(new TestKey(0, "0"), new TestValue(1, "1"));

        partitionStorage.runConsistently(() -> {
            partitionStorage.addWriteCommitted(new RowId(PARTITION_ID), tableRow, clock.now());

            return null;
        });
    }

    @Test
    void tableStorageDestructionFreesPartitionsPages() throws Exception {
        MvPartitionStorage partitionStorage = tableStorage.getOrCreateMvPartition(0);

        insertOneRow(partitionStorage);

        long emptyDataPagesBeforeDestroy = dataRegion().rowVersionFreeList().emptyDataPages();

        assertThat(tableStorage.destroy(), willSucceedFast());

        assertDestructionCompletes(emptyDataPagesBeforeDestroy);
    }

    private VolatilePageMemoryDataRegion dataRegion() {
        return ((VolatilePageMemoryTableStorage) tableStorage).dataRegion();
    }
}
