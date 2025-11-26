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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
class TxStateRocksDbSharedStorageTest {
    @WorkDirectory
    private Path workDir;

    @InjectExecutorService
    private ScheduledExecutorService scheduler;

    @InjectExecutorService
    private ExecutorService executor;

    private TxStateRocksDbSharedStorage sharedStorage;

    @BeforeEach
    void createAndStartSharedStorage() {
        sharedStorage = new TxStateRocksDbSharedStorage(
                "test",
                workDir,
                scheduler,
                executor,
                () -> {},
                new FailureManager(new NoOpFailureHandler())
        );

        startSharedStorage();
    }

    private void startSharedStorage() {
        assertThat(sharedStorage.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void stopSharedStorage() {
        if (sharedStorage != null) {
            assertThat(sharedStorage.stopAsync(), willCompleteSuccessfully());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void remembersCreatedTableOrZoneIdsOnDisk(boolean restart) {
        createZoneTxStateStorageLeavingTraceOnDisk(1);
        createZoneTxStateStorageLeavingTraceOnDisk(3);

        if (restart) {
            flushAndRestartSharedStorage();
        }

        assertThat(sharedStorage.tableOrZoneIdsOnDisk(), containsInAnyOrder(1, 3));
    }

    private void createZoneTxStateStorageLeavingTraceOnDisk(int zoneId) {
        TxStateRocksDbStorage zoneStorage = new TxStateRocksDbStorage(zoneId, 2, sharedStorage);
        zoneStorage.start();

        TxStateRocksDbPartitionStorage partitionStorage = zoneStorage.getOrCreatePartitionStorage(0);
        partitionStorage.start();

        partitionStorage.committedGroupConfiguration(new byte[]{1, 2, 3}, 1, 1);
    }

    private void flushAndRestartSharedStorage() {
        assertThat(sharedStorage.awaitFlush(true), willCompleteSuccessfully());

        stopSharedStorage();
        startSharedStorage();
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void tableOrZoneIdsOnDiskGetRemovedOnPersistedDestruction(boolean restart) {
        createZoneTxStateStorageLeavingTraceOnDisk(1);
        createZoneTxStateStorageLeavingTraceOnDisk(3);

        sharedStorage.destroyStorage(1);

        if (restart) {
            flushAndRestartSharedStorage();
        }

        assertThat(sharedStorage.tableOrZoneIdsOnDisk(), contains(3));
    }
}
