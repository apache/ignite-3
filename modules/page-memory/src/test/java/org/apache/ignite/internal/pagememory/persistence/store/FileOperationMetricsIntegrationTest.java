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

package org.apache.ignite.internal.pagememory.persistence.store;

import static org.apache.ignite.internal.pagememory.persistence.store.FilePageStore.LATEST_FILE_PAGE_STORE_VERSION;
import static org.apache.ignite.internal.pagememory.persistence.store.TestPageStoreUtils.createPageByteBuffer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.fileio.RandomAccessFileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryIoMetricSource;
import org.apache.ignite.internal.pagememory.persistence.PageMemoryIoMetrics;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for file operation metrics that verify metrics are actually updated
 * when file operations are performed.
 */
@ExtendWith(WorkDirectoryExtension.class)
class FileOperationMetricsIntegrationTest {
    private static final int PAGE_SIZE = 4096;

    @WorkDirectory
    private Path workDir;

    private StorageFilesMetricSource metricSource;
    private StorageFilesMetrics metrics;
    private AtomicInteger openFilesCount;
    private AtomicLong totalFileSize;
    private AtomicInteger deltaFilesCount;
    private AtomicLong deltaFilesTotalSize;

    @BeforeEach
    void setUp() {
        openFilesCount = new AtomicInteger(0);
        totalFileSize = new AtomicLong(0);
        deltaFilesCount = new AtomicInteger(0);
        deltaFilesTotalSize = new AtomicLong(0);

        metricSource = new StorageFilesMetricSource(
                openFilesCount::get,
                totalFileSize::get,
                deltaFilesCount::get,
                deltaFilesTotalSize::get
        );

        metrics = new StorageFilesMetrics(metricSource);
    }

    @Test
    void testFileCreateMetricsUpdated() throws Exception {
        Path filePath = workDir.resolve("test-create.bin");
        FilePageStoreHeader header = new FilePageStoreHeader(LATEST_FILE_PAGE_STORE_VERSION, PAGE_SIZE);

        // Verify initial state
        assertThat(metrics.fileCreateTotal().value(), is(0L));
        assertThat(metrics.fileOpenTotal().value(), is(0L));

        // Create new file (should trigger create metrics)
        PageMemoryIoMetricSource ioMetricSource = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics ioMetrics = new PageMemoryIoMetrics(ioMetricSource);

        FilePageStoreIo storeIo = new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header, metrics, ioMetrics);

        // Ensure initializes the file (should increment fileCreateTotal)
        ByteBuffer buffer = createPageByteBuffer(0L, PAGE_SIZE);
        storeIo.write(0L, buffer);

        // Verify file create was tracked
        assertThat("fileCreateTotal should be incremented", metrics.fileCreateTotal().value(), is(1L));
        assertThat("fileOpenTotal should still be 0", metrics.fileOpenTotal().value(), is(0L));

        // Verify timing was recorded
        long[] createTimeValues = metrics.fileCreateTime().value();
        long totalCreateOps = 0;
        for (long value : createTimeValues) {
            totalCreateOps += value;
        }
        assertThat("fileCreateTime should record timing", totalCreateOps, is(1L));

        storeIo.stop(true);
    }

    @Test
    void testFileOpenMetricsUpdated() throws Exception {
        Path filePath = workDir.resolve("test-open.bin");
        FilePageStoreHeader header = new FilePageStoreHeader(LATEST_FILE_PAGE_STORE_VERSION, PAGE_SIZE);

        // Create file first
        PageMemoryIoMetricSource ioMetricSource = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics ioMetrics = new PageMemoryIoMetrics(ioMetricSource);

        FilePageStoreIo createIo = new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header, metrics, ioMetrics);
        ByteBuffer buffer = createPageByteBuffer(0L, PAGE_SIZE);
        createIo.write(0L, buffer);
        createIo.stop(false);

        // Reset metrics to test open operation
        long initialCreateCount = metrics.fileCreateTotal().value();

        // Open existing file (should trigger open metrics)
        PageMemoryIoMetricSource ioMetricSource2 = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics ioMetrics2 = new PageMemoryIoMetrics(ioMetricSource2);

        FilePageStoreIo openIo = new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header, metrics, ioMetrics2);
        // Trigger ensure() by attempting to sync
        openIo.sync();

        // Verify file open was tracked
        assertThat("fileOpenTotal should be incremented", metrics.fileOpenTotal().value(), is(1L));
        assertThat("fileCreateTotal should not change", metrics.fileCreateTotal().value(), is(initialCreateCount));

        // Verify timing was recorded
        long[] openTimeValues = metrics.fileOpenTime().value();
        long totalOpenOps = 0;
        for (long value : openTimeValues) {
            totalOpenOps += value;
        }
        assertThat("fileOpenTime should record timing", totalOpenOps, is(1L));

        openIo.stop(true);
    }

    @Test
    void testFileSyncMetricsUpdated() throws Exception {
        Path filePath = workDir.resolve("test-sync.bin");
        FilePageStoreHeader header = new FilePageStoreHeader(LATEST_FILE_PAGE_STORE_VERSION, PAGE_SIZE);

        PageMemoryIoMetricSource ioMetricSource = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics ioMetrics = new PageMemoryIoMetrics(ioMetricSource);

        FilePageStoreIo storeIo = new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header, metrics, ioMetrics);
        ByteBuffer buffer = createPageByteBuffer(0L, PAGE_SIZE);
        storeIo.write(0L, buffer);

        // Verify initial state
        long[] initialSyncTimeValues = metrics.fileSyncTime().value();
        long initialSyncOps = 0;
        for (long value : initialSyncTimeValues) {
            initialSyncOps += value;
        }

        // Perform sync
        storeIo.sync();

        // Verify sync timing was recorded
        long[] syncTimeValues = metrics.fileSyncTime().value();
        long totalSyncOps = 0;
        for (long value : syncTimeValues) {
            totalSyncOps += value;
        }
        assertThat("fileSyncTime should record timing after sync", totalSyncOps, greaterThan(initialSyncOps));

        storeIo.stop(true);
    }

    @Test
    void testDeltaFileCreationMetricsUpdated() throws Exception {
        Path filePath = workDir.resolve("test-delta.bin");
        DeltaFilePageStoreIoHeader header = new DeltaFilePageStoreIoHeader(
                FilePageStore.DELTA_FILE_VERSION_1,
                0,
                PAGE_SIZE,
                new int[]{0, 1, 2}
        );

        // Verify initial state
        assertThat(metrics.deltaFileCreateTotal().value(), is(0L));

        // Create delta file (should increment deltaFileCreateTotal in constructor)
        PageMemoryIoMetricSource ioMetricSource = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics ioMetrics = new PageMemoryIoMetrics(ioMetricSource);

        DeltaFilePageStoreIo deltaIo = new DeltaFilePageStoreIo(
                new RandomAccessFileIoFactory(),
                filePath,
                header,
                metrics,
                ioMetrics
        );

        // Verify delta file creation was tracked
        assertThat("deltaFileCreateTotal should be incremented", metrics.deltaFileCreateTotal().value(), is(1L));

        deltaIo.stop(true);
    }

    @Test
    void testFileOpenErrorMetricsUpdated() throws Exception {
        Path filePath = workDir.resolve("nonexistent-dir/test.bin");
        FilePageStoreHeader header = new FilePageStoreHeader(LATEST_FILE_PAGE_STORE_VERSION, PAGE_SIZE);

        // Verify initial state
        assertThat(metrics.fileOpenErrors().value(), is(0L));

        PageMemoryIoMetricSource ioMetricSource = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics ioMetrics = new PageMemoryIoMetrics(ioMetricSource);

        FilePageStoreIo storeIo = new FilePageStoreIo(new RandomAccessFileIoFactory(), filePath, header, metrics, ioMetrics);

        try {
            // Try to write to file in non-existent directory (should fail and increment error counter)
            ByteBuffer buffer = createPageByteBuffer(0L, PAGE_SIZE);
            storeIo.write(0L, buffer);
        } catch (IgniteInternalCheckedException e) {
            // Expected - file operation should fail
        }

        // Verify error was tracked
        assertThat("fileOpenErrors should be incremented on error", metrics.fileOpenErrors().value(), greaterThan(0L));
    }

    @Test
    void testMultipleOperationsAccumulate() throws Exception {
        Path file1 = workDir.resolve("test-multi-1.bin");
        Path file2 = workDir.resolve("test-multi-2.bin");
        FilePageStoreHeader header = new FilePageStoreHeader(LATEST_FILE_PAGE_STORE_VERSION, PAGE_SIZE);

        // Create first file
        PageMemoryIoMetricSource ioMetricSource = new PageMemoryIoMetricSource();
        PageMemoryIoMetrics ioMetrics = new PageMemoryIoMetrics(ioMetricSource);

        FilePageStoreIo store1 = new FilePageStoreIo(new RandomAccessFileIoFactory(), file1, header, metrics, ioMetrics);
        ByteBuffer buffer = createPageByteBuffer(0L, PAGE_SIZE);
        store1.write(0L, buffer);

        assertThat("fileCreateTotal should be 1 after first file", metrics.fileCreateTotal().value(), is(1L));

        // Create second file
        FilePageStoreIo store2 = new FilePageStoreIo(new RandomAccessFileIoFactory(), file2, header, metrics, ioMetrics);
        store2.write(0L, createPageByteBuffer(0L, PAGE_SIZE));

        assertThat("fileCreateTotal should be 2 after second file", metrics.fileCreateTotal().value(), is(2L));

        // Perform multiple syncs
        store1.sync();
        store2.sync();

        long[] syncTimeValues = metrics.fileSyncTime().value();
        long totalSyncOps = 0;
        for (long value : syncTimeValues) {
            totalSyncOps += value;
        }
        assertThat("fileSyncTime should record both syncs", totalSyncOps, is(2L));

        store1.stop(true);
        store2.stop(true);
    }
}
