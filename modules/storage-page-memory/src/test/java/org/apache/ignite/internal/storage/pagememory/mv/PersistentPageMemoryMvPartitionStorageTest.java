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
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.isRow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({WorkDirectoryExtension.class, ExecutorServiceExtension.class})
class PersistentPageMemoryMvPartitionStorageTest extends AbstractPageMemoryMvPartitionStorageTest {
    @InjectConfiguration("mock.profiles.default = {engine = aipersist}")
    private StorageConfiguration storageConfig;

    @InjectExecutorService
    private ExecutorService executorService;

    @WorkDirectory
    private Path workDir;

    private PersistentPageMemoryStorageEngine engine;

    private MvTableStorage table;

    @BeforeEach
    void setUp() {
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

    @Override
    int pageSize() {
        return engine.configuration().pageSizeBytes().value();
    }

    @Test
    void testReadAfterRestart() throws Exception {
        RowId rowId = insert(binaryRow, txId);

        restartStorage();

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), isRow(binaryRow));
    }

    private void restartStorage() throws Exception {
        assertThat(
                engine.checkpointManager().forceCheckpoint("before_stop_engine").futureFor(FINISHED),
                willCompleteSuccessfully()
        );

        tearDown();

        setUp();
    }

    @Test
    void groupConfigIsPersisted() throws Exception {
        byte[] originalConfig = {1, 2, 3};

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(originalConfig);

            return null;
        });

        restartStorage();

        byte[] readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(originalConfig)));
    }

    @Test
    void groupConfigWhichDoesNotFitInOnePageIsPersisted() throws Exception {
        byte[] originalConfig = configThatDoesNotFitInOnePage();

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(originalConfig);

            return null;
        });

        restartStorage();

        byte[] readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(originalConfig)));
    }

    private static byte[] configThatDoesNotFitInOnePage() {
        byte[] originalconfig = new byte[1_000_000];

        for (int i = 0; i < originalconfig.length; i++) {
            originalconfig[i] = (byte) (i % 100);
        }

        return originalconfig;
    }

    @Test
    void groupConfigShorteningWorksCorrectly() throws Exception {
        byte[] originalConfigOfMoreThanOnePage = configThatDoesNotFitInOnePage();

        assertThat(originalConfigOfMoreThanOnePage.length, is(greaterThan(5 * pageSize())));

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(originalConfigOfMoreThanOnePage);

            return null;
        });

        byte[] configWhichFitsInOnePage = {1, 2, 3};

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(configWhichFitsInOnePage);

            return null;
        });

        restartStorage();

        byte[] readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(configWhichFitsInOnePage)));
    }

    @Test
    void testShouldReleaseReturnsFalseWhenNoCheckpointWaiting() {
        AtomicBoolean shouldReleaseValue = new AtomicBoolean(false);

        storage.runConsistently(locker -> {
            // Initially, no checkpoint is waiting, so shouldRelease should return false
            shouldReleaseValue.set(locker.shouldRelease());
            return null;
        });

        assertFalse(shouldReleaseValue.get(), "Locker shouldRelease must return false when no checkpoint is waiting");
    }

    @Test
    void testNestedRunConsistentlyInheritsLocker() {
        AtomicBoolean outerShouldRelease = new AtomicBoolean(false);
        AtomicBoolean innerShouldRelease = new AtomicBoolean(false);

        storage.runConsistently(outerLocker -> {
            // Nested runConsistently should reuse the same locker
            storage.runConsistently(innerLocker -> {
                // Both lockers should have the same behavior
                outerShouldRelease.set(outerLocker.shouldRelease());
                innerShouldRelease.set(innerLocker.shouldRelease());

                return null;
            });

            return null;
        });

        assertEquals(
                outerShouldRelease.get(),
                innerShouldRelease.get(),
                "Inner lockers view of shouldRelease must match"
        );
    }

    @Test
    void testShouldReleaseReturnsTrueWhenWriterIsWaiting() {
        AtomicBoolean shouldReleaseValue = new AtomicBoolean(false);

        storage.runConsistently(locker -> {
            engine.checkpointManager().scheduleCheckpoint(0, "I want a checkpoint");
            // Initially, no checkpoint is waiting, so shouldRelease should return false
            await().pollInSameThread().until(locker::shouldRelease);
            shouldReleaseValue.set(locker.shouldRelease());
            return null;
        });

        assertTrue(shouldReleaseValue.get(), "Locker shouldRelease must return true when checkpoint is scheduled now");
    }

    @Override
    protected void writeIntentsCursorIsEmptyEvenWhenHavingWriteIntents() {
        // No-op as in this storage implementation the write intents cursor is expected to work.
    }

    @Test
    void writeIntentsCursorIsEmptyOnEmptyStorage() {
        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThatCursorIsEmpty(cursor);
        }
    }

    private static <T> void assertThatCursorIsEmpty(Cursor<T> cursor) {
        assertFalse(cursor.hasNext());
        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    void writeIntentsCursorContainsAddedWriteIntent() {
        addWrite(ROW_ID, binaryRow, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertTrue(cursor.hasNext());
            assertThat(cursor.next(), is(ROW_ID));

            assertThatCursorIsEmpty(cursor);
        }
    }

    @Test
    void writeIntentsCursorContainsWriteIntentsAddedForDifferentRowIds() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, txId);
        addWrite(rowId2, binaryRow, newTransactionId());
        addWrite(rowId3, binaryRow, newTransactionId());

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(ROW_ID, rowId2, rowId3));
        }
    }

    @Test
    void writeIntentsCursorContainsAddedWriteIntentAfterItIsReplaced() {
        addWrite(ROW_ID, binaryRow, txId);
        addWrite(ROW_ID, binaryRow2, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), contains(ROW_ID));
        }
    }

    @Test
    void writeIntentsCursorAfterCommitsOfAllWriteIntentsIsEmpty() {
        addWrite(ROW_ID, binaryRow, txId);
        commitWrite(ROW_ID, clock.now(), txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), is(empty()));
        }
    }

    @Test
    void writeIntentsCursorAfterAbortsOfAllWriteIntentsIsEmpty() {
        addWrite(ROW_ID, binaryRow, txId);
        abortWrite(ROW_ID, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), is(empty()));
        }
    }

    @Test
    void writeIntentsCursorWorksCorrectlyAfter2ReplacementsInaRow() {
        RowId rowId2 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, txId);
        addWrite(ROW_ID, binaryRow2, txId);
        addWrite(ROW_ID, binaryRow3, txId);
        commitWrite(ROW_ID, clock.now(), txId);

        addWrite(rowId2, binaryRow, newTransactionId());

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), contains(rowId2));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInBeginningIsCommitted() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, txId);
        addWrite(rowId2, binaryRow, newTransactionId());
        addWrite(rowId3, binaryRow, newTransactionId());

        commitWrite(ROW_ID, clock.now(), txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(rowId2, rowId3));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInMiddleIsCommitted() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, newTransactionId());
        addWrite(rowId2, binaryRow, txId);
        addWrite(rowId3, binaryRow, newTransactionId());

        commitWrite(rowId2, clock.now(), txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(ROW_ID, rowId3));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInEndIsCommitted() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, newTransactionId());
        addWrite(rowId2, binaryRow, newTransactionId());
        addWrite(rowId3, binaryRow, txId);

        commitWrite(rowId3, clock.now(), txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(ROW_ID, rowId2));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInBeginningIsAborted() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, txId);
        addWrite(rowId2, binaryRow, newTransactionId());
        addWrite(rowId3, binaryRow, newTransactionId());

        abortWrite(ROW_ID, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(rowId2, rowId3));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInMiddleIsAborted() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, newTransactionId());
        addWrite(rowId2, binaryRow, txId);
        addWrite(rowId3, binaryRow, newTransactionId());

        abortWrite(rowId2, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(ROW_ID, rowId3));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInEndIsAborted() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, newTransactionId());
        addWrite(rowId2, binaryRow, newTransactionId());
        addWrite(rowId3, binaryRow, txId);

        abortWrite(rowId3, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(ROW_ID, rowId2));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInBeginningIsReplaced() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, txId);
        addWrite(rowId2, binaryRow, newTransactionId());
        addWrite(rowId3, binaryRow, newTransactionId());

        addWrite(ROW_ID, binaryRow, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(ROW_ID, rowId2, rowId3));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInMiddleIsReplaced() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, newTransactionId());
        addWrite(rowId2, binaryRow, txId);
        addWrite(rowId3, binaryRow, newTransactionId());

        addWrite(rowId2, binaryRow, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(ROW_ID, rowId2, rowId3));
        }
    }

    @Test
    void writeIntentsCursorWorksWhenWriteIntentInEndIsReplaced() {
        RowId rowId2 = new RowId(PARTITION_ID);
        RowId rowId3 = new RowId(PARTITION_ID);

        addWrite(ROW_ID, binaryRow, newTransactionId());
        addWrite(rowId2, binaryRow, newTransactionId());
        addWrite(rowId3, binaryRow, txId);

        addWrite(rowId3, binaryRow, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(ROW_ID, rowId2, rowId3));
        }
    }

    @Test
    void multiPageWriteIntentsWorkWithWriteIntentScans() {
        RowId rowId1 = insert(rowStoredInFragments(), newTransactionId());
        RowId rowId2 = insert(rowStoredInFragments(), newTransactionId());
        RowId rowId3 = insert(rowStoredInFragments(), newTransactionId());

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            assertThat(drain(cursor), containsInAnyOrder(rowId1, rowId2, rowId3));
        }
    }

    @Test
    void scanWriteIntentsRespectsStorageClosure() {
        addWrite(ROW_ID, binaryRow, txId);

        storage.close();

        assertThrows(StorageClosedException.class, storage::scanWriteIntents);
    }

    @Test
    void writeIntentsCursorRespectsStorageClosure() {
        addWrite(ROW_ID, binaryRow, txId);

        try (Cursor<RowId> cursor = storage.scanWriteIntents()) {
            storage.close();

            assertThrows(StorageClosedException.class, cursor::hasNext);
            assertThrows(StorageClosedException.class, cursor::next);
        }
    }
}
