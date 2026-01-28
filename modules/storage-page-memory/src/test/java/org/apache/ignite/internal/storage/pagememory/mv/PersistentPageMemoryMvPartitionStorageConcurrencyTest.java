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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.AbstractMvPartitionStorageConcurrencyTest;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({WorkDirectoryExtension.class, ExecutorServiceExtension.class})
class PersistentPageMemoryMvPartitionStorageConcurrencyTest extends AbstractMvPartitionStorageConcurrencyTest {
    private PersistentPageMemoryStorageEngine engine;

    private MvTableStorage table;

    @InjectExecutorService
    ExecutorService executorService;

    @BeforeEach
    void setUp(
            @WorkDirectory Path workDir,
            @InjectConfiguration("mock.profiles.default = {engine = aipersist}")
            StorageConfiguration storageConfig
    ) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new PersistentPageMemoryStorageEngine(
                "test",
                mock(MetricManager.class),
                storageConfig,
                null,
                ioRegistry,
                workDir,
                null,
                mock(FailureManager.class),
                mock(LogSyncer.class),
                executorService,
                clock
        );

        engine.start();

        table = engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                mock(StorageIndexDescriptorSupplier.class)
        );

        initialize(table);
    }

    @AfterEach
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        IgniteUtils.closeAllManually(
                table,
                engine == null ? null : engine::stop
        );
    }

    /**
     * Reproducer for a <a href="https://issues.apache.org/jira/browse/IGNITE-27638">corrupted Write Intent list</a>.
     * During replace of the value found both in inner and leaf nodes of VersionChain tree, we tried to remove WI from the WI list twice.
     * If neighboring WI in the WI double-linked list was invalidated between these removals, we would get an exception trying to access it
     * to change its links.
     * <p>
     * Test builds a 3-level tree, and creates a race between aborting A and D write intents. WI are large, so they don't share the same
     * page.
     * <p>           B
     * <p>          /  \
     * <p>        A     C
     * <p>       / \    | \
     * <p>      A   B   C  D
     */
    @Test
    void testAbortWriteIntentsListRace() {
        // Build a 3-level tree.
        int rowsCount = 50_000;
        UUID txId = newTransactionId();
        RowId[] rowIds = new RowId[rowsCount];

        for (int i = 0; i < rowsCount; i++) {
            rowIds[i] = new RowId(PARTITION_ID, i, i);
            addWriteCommitted(rowIds[i], TABLE_ROW, HybridTimestamp.MAX_VALUE);
        }

        // Left-most leaf, "A" on diagram.
        int leftMostLeafIndex = 163;

        BinaryRow largeRow = binaryRow(KEY, new TestValue(20, "A".repeat(10_000)));

        for (int i = 0; i < 1000; i++) {
            addWrite(rowIds[leftMostLeafIndex], largeRow, txId);
            addWrite(rowIds[rowsCount - 1], largeRow, txId);

            runRace(
                    () -> abortWrite(rowIds[leftMostLeafIndex], txId),
                    () -> abortWrite(rowIds[rowsCount - 1], txId)
            );
        }
    }
}
