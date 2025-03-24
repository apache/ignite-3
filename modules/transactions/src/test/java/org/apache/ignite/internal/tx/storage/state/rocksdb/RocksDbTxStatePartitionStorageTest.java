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
import static org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage.REBALANCE_IN_PROGRESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.storage.state.AbstractTxStatePartitionStorageTest;
import org.apache.ignite.internal.tx.storage.state.TxStatePartitionStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tx storage test for RocksDB implementation.
 */
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbTxStatePartitionStorageTest extends AbstractTxStatePartitionStorageTest {
    @WorkDirectory
    private Path workDir;

    private TxStateRocksDbSharedStorage sharedStorage;

    @InjectExecutorService
    private ScheduledExecutorService scheduledExecutor;

    @InjectExecutorService
    private ExecutorService executor;

    @Override
    protected TxStateRocksDbStorage createTableStorage() {
        return new TxStateRocksDbStorage(
                TABLE_ID,
                3,
                sharedStorage
        );
    }

    @Override
    @BeforeEach
    protected void beforeTest() {
        sharedStorage = new TxStateRocksDbSharedStorage(workDir, scheduledExecutor, executor, mock(LogSyncer.class), () -> 0);
        assertThat(sharedStorage.startAsync(new ComponentContext()), willCompleteSuccessfully());

        super.beforeTest();
    }

    @Override
    @AfterEach
    protected void afterTest() throws Exception {
        super.afterTest();

        assertThat(sharedStorage.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void testRestartStorageInProgressOfRebalance() {
        TxStatePartitionStorage storage = tableStorage.getOrCreatePartitionStorage(0);

        List<IgniteBiTuple<UUID, TxMeta>> rows = List.of(
                randomTxMetaTuple(1, UUID.randomUUID()),
                randomTxMetaTuple(1, UUID.randomUUID())
        );

        fillStorage(storage, rows);

        // We emulate the situation that the rebalancing did not have time to end.
        storage.lastApplied(REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS);

        assertThat(storage.flush(), willCompleteSuccessfully());

        tableStorage.close();

        tableStorage = createTableStorage();

        tableStorage.start();

        storage = tableStorage.getOrCreatePartitionStorage(0);

        checkMeta(storage, REBALANCE_IN_PROGRESS, REBALANCE_IN_PROGRESS, GROUP_CONFIGURATION, LEASE_INFO, SNAPSHOT_INFO);

        checkStorageContainsRows(storage, rows);
    }
}
