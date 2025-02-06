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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.bypassingThreadAssertions;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbSharedStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
class ZoneResourcesManagerTest extends BaseIgniteAbstractTest {
    private TxStateRocksDbSharedStorage sharedStorage;

    @Mock
    private LogSyncer logSyncer;

    private ZoneResourcesManager manager;

    @WorkDirectory
    private Path workDir;

    @InjectExecutorService
    private ScheduledExecutorService scheduler;

    @InjectExecutorService
    private ExecutorService executor;

    @BeforeEach
    void init() {
        sharedStorage = new TxStateRocksDbSharedStorage(workDir, scheduler, executor, logSyncer, () -> 0);

        manager = new ZoneResourcesManager(sharedStorage);

        assertThat(sharedStorage.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void cleanup() {
        assertThat(sharedStorage.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void createsTxStatePartitionStorage() {
        manager.registerZonePartitionCount(1, 10);

        TxStatePartitionStorage txStatePartitionStorage = getOrCreatePartitionTxStateStorage(1, 1);

        assertThat(txStatePartitionStorage, is(notNullValue()));
    }

    @Test
    void closesResourcesOnShutdown() {
        manager.registerZonePartitionCount(1, 10);
        manager.registerZonePartitionCount(2, 10);

        TxStatePartitionStorage storage1_1 = getOrCreatePartitionTxStateStorage(1, 1);
        TxStatePartitionStorage storage1_5 = getOrCreatePartitionTxStateStorage(1, 5);
        TxStatePartitionStorage storage2_3 = getOrCreatePartitionTxStateStorage(2, 3);

        manager.close();

        assertThatStorageIsStopped(storage1_1);
        assertThatStorageIsStopped(storage1_5);
        assertThatStorageIsStopped(storage2_3);
    }

    @SuppressWarnings("ThrowableNotThrown")
    private static void assertThatStorageIsStopped(TxStatePartitionStorage storage) {
        assertThrows(
                IgniteInternalException.class,
                () -> bypassingThreadAssertions(() -> storage.get(UUID.randomUUID())),
                "Transaction state storage is stopped"
        );
    }

    private TxStatePartitionStorage getOrCreatePartitionTxStateStorage(int zoneId, int partitionId) {
        return bypassingThreadAssertions(() -> manager.getOrCreatePartitionTxStateStorage(zoneId, partitionId));
    }
}
