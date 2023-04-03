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

package org.apache.ignite.internal.table.distributed;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import java.util.UUID;
import org.apache.ignite.distributed.TestPartitionDataStorage;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.table.distributed.raft.PartitionDataStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Abstract class for testing {@link StorageUpdateHandler} using different implementations of {@link MvPartitionStorage}.
 */
@ExtendWith(ConfigurationExtension.class)
abstract class AbstractMvStorageUpdateHandlerTest extends BaseMvStoragesTest {
    /** To be used in a loop. {@link RepeatedTest} has a smaller failure rate due to recreating the storage every time. */
    private static final int REPEATS = 100;

    private static final int PARTITION_ID = 0;

    private StorageEngine storageEngine;

    private MvTableStorage tableStorage;

    private MvPartitionStorage partitionStorage;

    private TestPartitionDataStorage partitionDataStorage;

    private StorageUpdateHandler storageUpdateHandler;

    @BeforeEach
    void setUp(
            @InjectConfiguration("mock.tables.foo{}") TablesConfiguration tablesConfig,
            @InjectConfiguration DistributionZoneConfiguration distributionZoneConfig
    ) {
        storageEngine = createStorageEngine();

        storageEngine.start();

        TableConfiguration tableConfig = tablesConfig.tables().get("foo");

        assertThat(
                tableConfig.dataStorage().change(dataStorageChange -> dataStorageChange.convert(storageEngine.name())),
                willCompleteSuccessfully()
        );

        tableStorage = storageEngine.createMvTable(tableConfig, tablesConfig, distributionZoneConfig);

        tableStorage.start();

        partitionStorage = getOrCreateMvPartition(tableStorage, PARTITION_ID);

        partitionDataStorage = new TestPartitionDataStorage(partitionStorage);

        storageUpdateHandler = new StorageUpdateHandler(PARTITION_ID, partitionDataStorage, Map::of, tableConfig.dataStorage());
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAllManually(
                partitionStorage,
                tableStorage,
                storageEngine == null ? null : storageEngine::stop
        );
    }

    protected abstract StorageEngine createStorageEngine();

    @Test
    void testConcurrentVacuumBatch() {
        RowId rowId0 = new RowId(PARTITION_ID);
        RowId rowId1 = new RowId(PARTITION_ID);

        BinaryRow row0 = binaryRow(new TestKey(0, "key0"), new TestValue(0, "value0"));
        BinaryRow row1 = binaryRow(new TestKey(0, "key0"), new TestValue(0, "value0"));

        for (int i = 0; i < REPEATS; i++) {
            addWriteCommitted(partitionDataStorage, rowId0, row0, clock.now());
            addWriteCommitted(partitionDataStorage, rowId1, row1, clock.now());

            addWriteCommitted(partitionDataStorage, rowId0, row0, clock.now());
            addWriteCommitted(partitionDataStorage, rowId1, row1, clock.now());

            addWriteCommitted(partitionDataStorage, rowId0, null, clock.now());
            addWriteCommitted(partitionDataStorage, rowId1, null, clock.now());

            runRace(
                    () -> storageUpdateHandler.vacuumBatch(HybridTimestamp.MAX_VALUE, 2),
                    () -> storageUpdateHandler.vacuumBatch(HybridTimestamp.MAX_VALUE, 2)
            );

            assertNull(partitionDataStorage.getStorage().closestRowId(RowId.lowestRowId(PARTITION_ID)));
        }
    }

    private static void addWriteCommitted(PartitionDataStorage storage, RowId rowId, @Nullable BinaryRow row, HybridTimestamp timestamp) {
        storage.runConsistently(() -> {
            storage.addWrite(rowId, row, UUID.randomUUID(), UUID.randomUUID(), PARTITION_ID);

            storage.commitWrite(rowId, timestamp);

            return null;
        });
    }
}
