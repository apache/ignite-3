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

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine.checkpointConfiguration;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.fileio.RandomAccessFileIo;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.DataRegion;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointMetricSource;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryDataRegion;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.StoragePartitionMeta;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfiguration;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Tests that data is not corrupted after failed checkpoints. */
@ExtendWith({WorkDirectoryExtension.class, ExecutorServiceExtension.class})
public class FailedCheckpointTest extends BaseMvStoragesTest {
    private static final int PARTITION_ID = 1;
    private static final GroupPartitionId GROUP_PARTITION_ID = new GroupPartitionId(1, PARTITION_ID);

    private final TestKey key = new TestKey(10, "foo");
    private final TestValue value = new TestValue(20, "bar");
    private final BinaryRow binaryRow = binaryRow(key, value);

    @WorkDirectory
    private Path workDir;

    @InjectConfiguration("mock.profiles.default = {engine = aipersist}")
    private StorageConfiguration storageConfig;

    private FilePageStoreManager filePageStoreManager;
    private CheckpointManager checkpointManager;
    private PersistentPageMemoryDataRegion dataRegion;
    private Set<DataRegion<PersistentPageMemory>> dataRegions = new HashSet<>();
    private PersistentPageMemoryStorageEngine mockEngine;
    private PartitionMetaManager partitionMetaManager;
    private MvTableStorage tableStorage;
    private MvPartitionStorage partitionStorage;

    @InjectExecutorService
    private ExecutorService destructionExecutor;

    @InjectExecutorService
    private ExecutorService commonExecutorService;

    private volatile boolean failWrite;

    @BeforeEach
    void setUp() throws IOException {
        startNode();
    }

    @AfterEach
    void tearDown() throws Exception {
        tableStorage.destroy();

        stopNode();
    }

    @Test
    void testRestartAfterFailedCheckpoint() throws Exception {
        writeRows();

        doNormalCheckpoint();

        int expectedPersistedPages = dataRegion.partitionMetaManager().getMeta(GROUP_PARTITION_ID).pageCount();

        writeRows();

        doFailedCheckpointAndRestart();

        // Check that we data region is not corrupted.
        assertThat(dataRegion.partitionMetaManager().getMeta(GROUP_PARTITION_ID).pageCount(), is(expectedPersistedPages));
        writeRows();

        // Check that next checkpoint will succeed.
        doNormalCheckpoint();
    }

    @Test
    void testRestartAfterFailedFirstCheckpoint() throws Exception {
        int expectedPersistedPages = dataRegion.partitionMetaManager().getMeta(GROUP_PARTITION_ID).pageCount();

        writeRows();

        doFailedCheckpointAndRestart();

        // Check that we data region is not corrupted.
        assertThat(dataRegion.partitionMetaManager().getMeta(GROUP_PARTITION_ID).pageCount(), is(expectedPersistedPages));
        writeRows();

        // Check that next checkpoint will succeed.
        doNormalCheckpoint();
    }

    private void doNormalCheckpoint() {
        failWrite = false;

        assertThat(checkpointManager.forceCheckpoint("successful checkpoint").futureFor(FINISHED), willCompleteSuccessfully());
    }

    private void doFailedCheckpointAndRestart() throws Exception {
        failWrite = true;

        assertThat(checkpointManager.forceCheckpoint("failed checkpoint").futureFor(FINISHED), willTimeoutFast());

        stopNode();
        startNode();
    }

    private void startNode() throws IOException {
        mockEngine = mock(PersistentPageMemoryStorageEngine.class);
        when(mockEngine.generateGlobalRemoveId()).thenReturn(new AtomicLong(0));

        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        PersistentPageMemoryStorageEngineConfiguration engineConfig =
                ((PersistentPageMemoryStorageEngineExtensionConfiguration) storageConfig.engines()).aipersist();

        int pageSize = engineConfig.pageSizeBytes().value();

        partitionMetaManager = new PartitionMetaManager(ioRegistry, pageSize, StoragePartitionMeta.FACTORY);

        checkpointManager = spy(checkpointManager(engineConfig, pageSize, ioRegistry));

        when(mockEngine.checkpointManager()).thenReturn(checkpointManager);

        dataRegion = new PersistentPageMemoryDataRegion(
                mock(MetricManager.class),
                (PersistentPageMemoryProfileConfiguration) storageConfig.profiles().get("default"),
                null,
                ioRegistry,
                filePageStoreManager,
                partitionMetaManager,
                checkpointManager,
                pageSize
        );

        dataRegion.start();

        dataRegions.add(dataRegion);

        tableStorage = createMvTableStorage();

        CompletableFuture<MvPartitionStorage> mvPartitionFut = tableStorage.createMvPartition(PARTITION_ID);

        assertThat(mvPartitionFut, willCompleteSuccessfully());

        partitionStorage = mvPartitionFut.join();
    }

    private MvTableStorage createMvTableStorage() {
        var tableStorage = new PersistentPageMemoryTableStorage(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                mock(StorageIndexDescriptorSupplier.class),
                mockEngine,
                dataRegion,
                destructionExecutor,
                mock(FailureManager.class)
        );

        dataRegion.addTableStorage(tableStorage);

        return tableStorage;
    }

    private CheckpointManager checkpointManager(
            PersistentPageMemoryStorageEngineConfiguration engineConfig,
            int pageSize,
            PageIoRegistry ioRegistry
    ) throws IOException {
        try {
            FileIoFactory fileIoFactory = mock(FileIoFactory.class);

            // Simulate a failure after 10 written pages.
            doAnswer(invocation -> {
                FileIo file = spy(new RandomAccessFileIo(invocation.getArgument(0), CREATE, READ, WRITE));

                AtomicInteger invokeCount = new AtomicInteger(0);

                doAnswer(invocation2 -> {
                    if (failWrite && invokeCount.incrementAndGet() > 10) {
                        throw new IgniteInternalException("failure");
                    }

                    return invocation2.callRealMethod();
                }).when(file).writeFully(any(), anyLong());

                return file;
            }).when(fileIoFactory).create(any(), any(OpenOption[].class));

            doCallRealMethod().when(fileIoFactory).create(any());

            filePageStoreManager = new FilePageStoreManager(
                    "test",
                    workDir,
                    fileIoFactory,
                    pageSize,
                    mock(FailureManager.class)
            );

            filePageStoreManager.start();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting file page store manager", e);
        }

        try {
            checkpointManager = new CheckpointManager(
                    "test",
                    mock(LongJvmPauseDetector.class),
                    mock(FailureManager.class),
                    checkpointConfiguration(engineConfig.checkpoint()),
                    filePageStoreManager,
                    partitionMetaManager,
                    dataRegions,
                    ioRegistry,
                    mock(LogSyncer.class),
                    commonExecutorService,
                    new CheckpointMetricSource("test"),
                    pageSize
            );

            checkpointManager.start();

            return checkpointManager;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error starting checkpoint manager", e);
        }
    }

    private void stopNode() throws Exception {
        tableStorage.close();
        dataRegion.stop();
        filePageStoreManager.stop();
        checkpointManager.stop();

        dataRegions = new HashSet<>();
    }

    private void writeRows() {
        partitionStorage.runConsistently(locker -> {
            for (int i = 0; i < 10000; i++) {
                RowId rowId = new RowId(PARTITION_ID);

                locker.lock(rowId);

                partitionStorage.addWriteCommitted(rowId, binaryRow, clock.now());
            }

            return null;
        });
    }
}
