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

package org.apache.ignite.internal.raft.storage.segstore;

import static ca.seinesoftware.hamcrest.path.PathMatcher.exists;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link RaftLogGarbageCollector}.
 */
@ExtendWith(ConfigurationExtension.class)
@ExtendWith(MockitoExtension.class)
class RaftLogGarbageCollectorTest extends IgniteAbstractTest {
    private static final int FILE_SIZE = 200;

    private static final long GROUP_ID_1 = 1000;

    private static final long GROUP_ID_2 = 2000;

    private static final int STRIPES = 10;

    private static final String NODE_NAME = "test";

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration(value = "mock.segmentFileSizeBytes=" + FILE_SIZE, validate = false)
    private LogStorageConfiguration storageConfiguration;

    private SegmentFileManager fileManager;

    private IndexFileManager indexFileManager;

    private RaftLogGarbageCollector garbageCollector;

    @BeforeEach
    void setUp() throws IOException {
        fileManager = new SegmentFileManager(
                NODE_NAME,
                workDir,
                STRIPES,
                new NoOpFailureManager(),
                raftConfiguration,
                storageConfiguration
        );

        fileManager.start();

        indexFileManager = fileManager.indexFileManager();

        garbageCollector = fileManager.garbageCollector();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fileManager != null) {
            fileManager.close();
        }
    }

    @Test
    void testRunCompactionWithFileFullyCompacted() throws Exception {
        // Fill some segment files with entries.
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 10);
        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
        }

        // Truncate most of the previously inserted entries.
        fileManager.truncatePrefix(GROUP_ID_1, batches.size() - 1);

        triggerAndAwaitCheckpoint(batches.size() - 1);

        List<Path> segmentFiles = segmentFiles();

        Path segmentFilePath = segmentFiles.get(0);

        FileProperties fileProperties = SegmentFile.fileProperties(segmentFilePath);

        garbageCollector.runCompaction(fileProperties);

        assertThat(segmentFilePath, not(exists()));
        assertThat(indexFileManager.indexFilePath(fileProperties), not(exists()));

        // Validate that no files with increased generation have been created.
        var newFileProperties = new FileProperties(fileProperties.ordinal(), fileProperties.generation() + 1);

        assertThat(segmentFiles(), hasSize(segmentFiles.size() - 1));
        assertThat(fileManager.segmentFilesDir().resolve(SegmentFile.fileName(newFileProperties)), not(exists()));
        assertThat(indexFileManager.indexFilePath(newFileProperties), not(exists()));
    }

    @Test
    void testRunCompactionWithFilePartiallyCompacted() throws Exception {
        // Fill some segment files with entries from two groups.
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
            appendBytes(GROUP_ID_2, batches.get(i), i);
        }

        // Truncate entries of only one group.
        fileManager.truncatePrefix(GROUP_ID_1, batches.size() - 1);

        triggerAndAwaitCheckpoint(batches.size() - 1);

        List<Path> segmentFiles = segmentFiles();

        long originalSize = Files.size(segmentFiles.get(0));

        Path firstSegmentFile = segmentFiles.get(0);

        FileProperties originalFileProperties = SegmentFile.fileProperties(firstSegmentFile);

        garbageCollector.runCompaction(originalFileProperties);

        // Segment file should be replaced by a new one with increased generation.
        assertThat(firstSegmentFile, not(exists()));

        var newFileProperties = new FileProperties(originalFileProperties.ordinal(), originalFileProperties.generation() + 1);

        Path newSegmentFile = fileManager.segmentFilesDir().resolve(SegmentFile.fileName(newFileProperties));

        assertThat(newSegmentFile, exists());

        assertThat(Files.size(newSegmentFile), lessThan(originalSize));

        assertThat(indexFileManager.indexFilePath(newFileProperties), exists());
        assertThat(indexFileManager.indexFilePath(originalFileProperties), not(exists()));
    }

    @Test
    void testRunCompactionWithTruncationRecords() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 5);
        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
        }

        // Truncate both prefix and suffix.
        fileManager.truncatePrefix(GROUP_ID_1, batches.size() / 2);
        fileManager.truncateSuffix(GROUP_ID_1, batches.size() / 2);

        List<Path> segmentFiles = segmentFiles();

        triggerAndAwaitCheckpoint(batches.size() / 2);

        for (Path segmentFilePath : segmentFiles) {
            runCompaction(segmentFilePath);

            assertThat(segmentFilePath, not(exists()));
        }
    }

    @RepeatedTest(10)
    void testConcurrentCompactionAndReads() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 10, 50);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
        }

        await().until(this::indexFiles, hasSize(equalTo(segmentFiles().size() - 1)));

        var gcTaskDone = new AtomicBoolean(false);

        RunnableX gcTask = () -> {
            try {
                List<Path> segmentFiles = segmentFiles();

                Path lastSegmentFile = segmentFiles.get(segmentFiles.size() - 1);

                long aliveIndex = 0;

                // Don't compact the last segment file.
                while (!segmentFiles.get(0).equals(lastSegmentFile)) {
                    fileManager.truncatePrefix(GROUP_ID_1, ++aliveIndex);

                    runCompaction(segmentFiles.get(0));

                    segmentFiles = segmentFiles();
                }
            } finally {
                gcTaskDone.set(true);
            }
        };

        RunnableX readTask = () -> {
            while (!gcTaskDone.get()) {
                for (int i = 0; i < batches.size(); i++) {
                    int index = i;

                    fileManager.getEntry(GROUP_ID_1, i, bs -> {
                        if (bs != null) {
                            assertThat(bs, is(batches.get(index)));
                        }
                        return null;
                    });
                }
            }
        };

        runRace(gcTask, readTask, readTask, readTask);
    }

    @RepeatedTest(10)
    void testConcurrentCompactionAndReadsFromMultipleGroups() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 10, 50);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
            appendBytes(GROUP_ID_2, batches.get(i), i);
        }

        await().until(this::indexFiles, hasSize(equalTo(segmentFiles().size() - 1)));

        var gcTaskDone = new AtomicBoolean(false);

        RunnableX gcTask = () -> {
            try {
                List<Path> segmentFiles = segmentFiles();

                Path curSegmentFilePath = segmentFiles.get(0);

                // Unlike in the similar test, segment files will never be removed completely due to the presence of the second group.
                // Because of that we need to use more complex logic to iterate over the segment files.
                int segmentFilesIndex = 0;

                long aliveIndex = 0;

                while (true) {
                    fileManager.truncatePrefix(GROUP_ID_1, ++aliveIndex);

                    FileProperties fileProperties = SegmentFile.fileProperties(curSegmentFilePath);

                    long sizeBeforeCompaction = Files.size(curSegmentFilePath);

                    garbageCollector.runCompaction(fileProperties);

                    FileProperties newFileProperties = new FileProperties(fileProperties.ordinal(), fileProperties.generation() + 1);

                    curSegmentFilePath = fileManager.segmentFilesDir().resolve(SegmentFile.fileName(newFileProperties));

                    long sizeAfterCompaction = Files.size(curSegmentFilePath);

                    // If the files' size didn't change, there's nothing left to compact, we can switch to the next segment.
                    if (sizeAfterCompaction == sizeBeforeCompaction) {
                        segmentFilesIndex++;

                        // Don't compact the last segment file.
                        if (segmentFilesIndex == segmentFiles.size() - 1) {
                            break;
                        }

                        curSegmentFilePath = segmentFiles.get(segmentFilesIndex);
                    }
                }
            } finally {
                gcTaskDone.set(true);
            }
        };

        RunnableX readTaskFromGroup1 = () -> {
            while (!gcTaskDone.get()) {
                for (int i = 0; i < batches.size(); i++) {
                    int index = i;

                    fileManager.getEntry(GROUP_ID_1, i, bs -> {
                        if (bs != null) {
                            assertThat(bs, is(batches.get(index)));
                        }
                        return null;
                    });
                }
            }
        };

        RunnableX readTaskFromGroup2 = () -> {
            while (!gcTaskDone.get()) {
                for (int i = 0; i < batches.size(); i++) {
                    int index = i;

                    fileManager.getEntry(GROUP_ID_2, i, bs -> {
                        if (bs != null) {
                            assertThat(bs, is(batches.get(index)));
                        }
                        return null;
                    });
                }
            }
        };

        runRace(gcTask, readTaskFromGroup1, readTaskFromGroup1, readTaskFromGroup2, readTaskFromGroup2);
    }

    @Test
    void testRecoveryAfterCompaction() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 8, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
            appendBytes(GROUP_ID_2, batches.get(i), i);
        }

        // Truncate entries of only one group.
        fileManager.truncatePrefix(GROUP_ID_1, batches.size() - 1);

        triggerAndAwaitCheckpoint(batches.size() - 1);

        List<Path> segmentFiles = segmentFiles();

        runCompaction(segmentFiles.get(0));

        restartSegmentFileManager();

        for (int i = 0; i < batches.size(); i++) {
            int index = i;

            fileManager.getEntry(GROUP_ID_1, i, bs -> {
                assertThat(index, is(batches.size() - 1));
                assertThat(bs, is(batches.get(index)));

                return null;
            });

            fileManager.getEntry(GROUP_ID_2, i, bs -> {
                assertThat(bs, is(batches.get(index)));
                return null;
            });
        }
    }

    @Test
    void testCleanupLeftoverFilesOnRecovery() throws Exception {
        // Create temporary segment files
        Path tmpFile1 = fileManager.segmentFilesDir().resolve("segment-0000000001-0000000000.bin.tmp");
        Path tmpFile2 = fileManager.segmentFilesDir().resolve("segment-0000000002-0000000001.bin.tmp");

        Files.createFile(tmpFile1);
        Files.createFile(tmpFile2);

        // Create duplicate segment and index files with same ordinal but different generations
        Path segmentFile1Gen0 = fileManager.segmentFilesDir().resolve("segment-0000000003-0000000000.bin");
        Path segmentFile1Gen1 = fileManager.segmentFilesDir().resolve("segment-0000000003-0000000001.bin");
        Path indexFile1Gen0 = fileManager.indexFileManager().indexFilesDir().resolve("index-0000000003-0000000000.bin");
        Path indexFile1Gen1 = fileManager.indexFileManager().indexFilesDir().resolve("index-0000000003-0000000001.bin");

        Files.createFile(segmentFile1Gen0);
        Files.createFile(segmentFile1Gen1);
        Files.createFile(indexFile1Gen0);
        Files.createFile(indexFile1Gen1);

        // Create orphaned index files (no corresponding segment file)
        Path orphanedIndexFile = fileManager.indexFileManager().indexFilesDir().resolve("index-0000000099-0000000000.bin");

        Files.createFile(orphanedIndexFile);

        fileManager.close();

        fileManager = new SegmentFileManager(
                NODE_NAME,
                workDir,
                STRIPES,
                new NoOpFailureManager(),
                raftConfiguration,
                storageConfiguration
        );

        fileManager.garbageCollector().cleanupLeftoverFiles();

        // Verify temporary files are cleaned up.
        assertThat(tmpFile1, not(exists()));
        assertThat(tmpFile2, not(exists()));

        // Verify duplicate segment files are cleaned up (lower generation removed).
        assertThat(segmentFile1Gen0, not(exists()));
        assertThat(segmentFile1Gen1, exists());
        assertThat(indexFile1Gen0, not(exists()));
        assertThat(indexFile1Gen1, exists());

        // Verify orphaned index files are cleaned up.
        assertThat(orphanedIndexFile, not(exists()));
    }

    @Test
    void testRunCompactionWithFullyCompactedFileByTruncatedSuffix() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
        }

        fileManager.truncateSuffix(GROUP_ID_1, 1);

        triggerAndAwaitCheckpoint(1);

        // Since we truncated all entries after log index 1, we can expect that the second segment file will be fully compacted.
        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(greaterThan(2)));

        runCompaction(segmentFiles.get(1));

        assertThat(segmentFiles.get(1), not(exists()));

        // Check that no other generations of this file exist.
        var differentGenerationFileProperties = new FileProperties(1, 1);
        Path differentGenerationFile = fileManager.segmentFilesDir().resolve(SegmentFile.fileName(differentGenerationFileProperties));

        assertThat(differentGenerationFile, not(exists()));
    }

    @Test
    void testRunCompactionWithPartiallyCompactedFileByTruncatedSuffix() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 10);

        // Use two groups to guarantee that no files will be fully truncated.
        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
            appendBytes(GROUP_ID_2, batches.get(i), i);
        }

        fileManager.truncateSuffix(GROUP_ID_1, 1);

        triggerAndAwaitCheckpoint(1);

        // Since we truncated GROUP_ID_1 entries after log index 1, the second segment file will be partially compacted:
        // GROUP_ID_1 entries are dropped but GROUP_ID_2 entries survive in a new generation file.
        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(greaterThan(2)));

        runCompaction(segmentFiles.get(1));

        assertThat(segmentFiles.get(1), not(exists()));

        // Check that a new generation file has been created.
        var differentGenerationFileProperties = new FileProperties(1, 1);
        Path differentGenerationFile = fileManager.segmentFilesDir().resolve(SegmentFile.fileName(differentGenerationFileProperties));

        assertThat(differentGenerationFile, exists());
    }

    /**
     * Similar to {@link #testRunCompactionWithFullyCompactedFileByTruncatedSuffix()} but includes recovery validation.
     */
    @Test
    void testRecoveryAfterCompactionWithFullyCompactedFileByTruncatedSuffix() throws Exception {
        List<byte[]> staleBatches = createRandomData(FILE_SIZE / 4, 10);

        for (int i = 0; i < staleBatches.size(); i++) {
            appendBytes(GROUP_ID_1, staleBatches.get(i), i);
        }

        long lastLogIndexKept = 1;

        fileManager.truncateSuffix(GROUP_ID_1, lastLogIndexKept);

        List<byte[]> newBatches = createRandomData(FILE_SIZE / 4, 5);

        for (int i = 0; i < newBatches.size(); i++) {
            appendBytes(GROUP_ID_1, newBatches.get(i), i + lastLogIndexKept + 1);
        }

        await().until(this::indexFiles, hasSize(greaterThanOrEqualTo(segmentFiles().size() - 1)));

        List<Path> segmentFiles = segmentFiles();

        runCompaction(segmentFiles.get(0));

        runCompaction(segmentFiles.get(1));

        restartSegmentFileManager();

        for (int i = 0; i <= lastLogIndexKept; i++) {
            int finalI = i;

            fileManager.getEntry(GROUP_ID_1, i, bs -> {
                assertThat(bs, is(staleBatches.get(finalI)));
                return null;
            });
        }

        for (int i = 0; i < newBatches.size(); i++) {
            int finalI = i;

            fileManager.getEntry(GROUP_ID_1, lastLogIndexKept + i + 1, bs -> {
                assertThat(bs, is(newBatches.get(finalI)));
                return null;
            });
        }
    }

    /**
     * Similar to {@link #testRunCompactionWithPartiallyCompactedFileByTruncatedSuffix()} but includes recovery validation.
     */
    @Test
    void testRecoveryAfterCompactionWithPartiallyCompactedFileByTruncatedSuffix() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
            appendBytes(GROUP_ID_2, batches.get(i), i);
        }

        fileManager.truncateSuffix(GROUP_ID_1, 1);

        triggerAndAwaitCheckpoint(1);

        List<Path> segmentFiles = segmentFiles();

        runCompaction(segmentFiles.get(0));

        runCompaction(segmentFiles.get(1));

        restartSegmentFileManager();

        // GROUP_ID_1: only entries 0 and 1 are within the suffix truncation boundary and should be readable.
        fileManager.getEntry(GROUP_ID_1, 0, bs -> {
            assertThat(bs, is(batches.get(0)));
            return null;
        });

        fileManager.getEntry(GROUP_ID_1, 1, bs -> {
            assertThat(bs, is(batches.get(1)));
            return null;
        });

        // GROUP_ID_2: all original entries should still be accessible.
        for (int i = 0; i < batches.size(); i++) {
            int index = i;

            fileManager.getEntry(GROUP_ID_2, i, bs -> {
                assertThat(bs, is(batches.get(index)));
                return null;
            });
        }
    }

    /**
     * Reproducer for the stale-deque-entry corruption bug: after fully deleting a middle segment file, its {@link IndexFileMeta} remains in
     * the same deque block as the preceding file's meta. When the preceding file is subsequently partially compacted (its live log range
     * shrinks), {@link IndexFileMetaArray#onIndexCompacted} asserts that the new meta's {@code lastLogIndexExclusive} equals the next
     * entry's {@code firstLogIndexInclusive}.
     */
    @Test
    void testCompactionOfFileAdjacentToStaleEntryInDequeCausesCorruption() throws Exception {
        // Use FILE_SIZE / 8 batches so that ~4 entries fit per segment file. This ensures file 0 covers [0,4),
        // file 1 covers [4,8), etc. After suffix-truncation at index 1, file 0's live range shrinks to [0,2)
        // while the stale file 1 meta still starts at 4.
        List<byte[]> batches = createRandomData(FILE_SIZE / 8, 20);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
        }

        // Entries > 1 are now stale. File 0 has live entries [0,1] plus stale entries, file 1 is fully stale.
        fileManager.truncateSuffix(GROUP_ID_1, 1);

        triggerAndAwaitCheckpoint(1);

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(greaterThan(3)));

        // Partially compact file 0.
        runCompaction(segmentFiles.get(0));

        assertThat(segmentFiles.get(0), not(exists()));

        var newFile0Properties = new FileProperties(0, 1);

        assertThat(fileManager.segmentFilesDir().resolve(SegmentFile.fileName(newFile0Properties)), exists());

        // Fully delete file 1 and 2 (all entries stale). Its IndexFileMeta stays in block 0 of the GroupIndexMeta deque.
        runCompaction(segmentFiles.get(1));
        runCompaction(segmentFiles.get(2));

        assertThat(segmentFiles.get(1), not(exists()));
        assertThat(segmentFiles.get(2), not(exists()));

        // Entries 0 and 1 must still be readable.
        fileManager.getEntry(GROUP_ID_1, 0, bs -> {
            assertThat(bs, is(batches.get(0)));
            return null;
        });

        fileManager.getEntry(GROUP_ID_1, 1, bs -> {
            assertThat(bs, is(batches.get(1)));
            return null;
        });
    }

    @Test
    void testLogSizeBytesInitializedCorrectlyOnStartup() throws Exception {
        // Fill multiple segment files to create both segment and index files.
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 10);
        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
        }

        // Wait for at least one checkpoint so index files exist.
        await().until(this::indexFiles, hasSize(greaterThanOrEqualTo(1)));

        long expectedSize = totalLogSizeFromDisk(fileManager);

        assertThat(garbageCollector.logSizeBytes(), is(expectedSize));

        restartSegmentFileManager();

        assertThat(garbageCollector.logSizeBytes(), is(expectedSize));
    }

    private void runCompaction(Path segmentFilePath) throws IOException {
        garbageCollector.runCompaction(SegmentFile.fileProperties(segmentFilePath));
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(fileManager.segmentFilesDir())) {
            return files
                    .filter(p -> !p.getFileName().toString().endsWith(".tmp"))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private List<Path> indexFiles() throws IOException {
        try (Stream<Path> files = Files.list(fileManager.indexFilesDir())) {
            return files
                    .filter(p -> !p.getFileName().toString().endsWith(".tmp"))
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    private static List<byte[]> createRandomData(int batchLength, int numBatches) {
        return IntStream.range(0, numBatches)
                .mapToObj(i -> randomBytes(ThreadLocalRandom.current(), batchLength))
                .collect(Collectors.toList());
    }

    private void appendBytes(long groupId, byte[] serializedEntry, long index) throws IOException {
        var entry = new LogEntry();
        entry.setId(new LogId(index, 0));

        fileManager.appendEntry(groupId, entry, new LogEntryEncoder() {
            @Override
            public byte[] encode(LogEntry log) {
                return serializedEntry;
            }

            @Override
            public void encode(ByteBuffer buffer, LogEntry log) {
                buffer.put(serializedEntry);
            }

            @Override
            public int size(LogEntry logEntry) {
                return serializedEntry.length;
            }
        });
    }

    private void triggerAndAwaitCheckpoint(long lastGroupIndex) throws IOException {
        List<Path> segmentFilesBeforeCheckpoint = segmentFiles();

        // Insert some entries to trigger a rollover (and a checkpoint).
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 5);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), lastGroupIndex + i + 1);
        }

        List<Path> segmentFilesAfterCheckpoint = segmentFiles();

        assertThat(segmentFilesAfterCheckpoint, hasSize(greaterThan(segmentFilesBeforeCheckpoint.size())));

        // Wait for the checkpoint process to complete.
        await().until(this::indexFiles, hasSize(greaterThanOrEqualTo(segmentFilesAfterCheckpoint.size() - 1)));
    }

    private void restartSegmentFileManager() throws Exception {
        fileManager.close();

        fileManager = new SegmentFileManager(
                NODE_NAME,
                workDir,
                STRIPES,
                new NoOpFailureManager(),
                raftConfiguration,
                storageConfiguration
        );

        fileManager.start();

        indexFileManager = fileManager.indexFileManager();
        garbageCollector = fileManager.garbageCollector();
    }

    private static long totalLogSizeFromDisk(SegmentFileManager manager) throws IOException {
        try (Stream<Path> files = Stream.concat(Files.list(manager.segmentFilesDir()), Files.list(manager.indexFilesDir()))) {
            return files
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    })
                    .sum();
        }
    }
}
