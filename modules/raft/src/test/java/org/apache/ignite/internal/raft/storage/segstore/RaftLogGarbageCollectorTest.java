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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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
    void testRunCompactionWithAllEntriesTruncated() throws Exception {
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

        SegmentFile segmentFile = SegmentFile.openExisting(segmentFilePath, false);
        try {
            garbageCollector.runCompaction(segmentFile);
        } finally {
            segmentFile.close();
        }

        assertThat(Files.exists(segmentFilePath), is(false));
        assertThat(Files.exists(indexFileManager.indexFilePath(fileProperties)), is(false));

        // Validate that no files with increased generation have been created.
        var newFileProperties = new FileProperties(fileProperties.ordinal(), fileProperties.generation() + 1);

        assertThat(segmentFiles(), hasSize(segmentFiles.size() - 1));
        assertThat(Files.exists(fileManager.segmentFilesDir().resolve(SegmentFile.fileName(newFileProperties))), is(false));
        assertThat(Files.exists(indexFileManager.indexFilePath(newFileProperties)), is(false));
    }

    @Test
    void testRunCompactionWithSomeEntriesTruncated() throws Exception {
        // Fill some segment files with entries from two groups.
        List<byte[]> batches = createRandomData(FILE_SIZE / 8, 10);

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

        SegmentFile segmentFile = SegmentFile.openExisting(firstSegmentFile, false);
        try {
            garbageCollector.runCompaction(segmentFile);
        } finally {
            segmentFile.close();
        }

        // Segment file should be replaced by a new one with increased generation.
        assertThat(Files.exists(firstSegmentFile), is(false));

        var newFileProperties = new FileProperties(originalFileProperties.ordinal(), originalFileProperties.generation() + 1);

        Path newSegmentFile = fileManager.segmentFilesDir().resolve(SegmentFile.fileName(newFileProperties));

        assertThat(Files.exists(newSegmentFile), is(true));

        assertThat(Files.size(newSegmentFile), lessThan(originalSize));

        assertThat(Files.exists(indexFileManager.indexFilePath(newFileProperties)), is(true));
        assertThat(Files.exists(indexFileManager.indexFilePath(originalFileProperties)), is(false));
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
            SegmentFile segmentFile = SegmentFile.openExisting(segmentFilePath, false);

            try {
                garbageCollector.runCompaction(segmentFile);
            } finally {
                segmentFile.close();
            }

            assertThat(Files.exists(segmentFilePath), is(false));
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

                    SegmentFile segmentFile = SegmentFile.openExisting(segmentFiles.get(0), false);
                    try {
                        garbageCollector.runCompaction(segmentFile);
                    } finally {
                        segmentFile.close();
                    }

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

                    SegmentFile segmentFile = SegmentFile.openExisting(curSegmentFilePath, false);
                    try {
                        garbageCollector.runCompaction(segmentFile);
                    } finally {
                        segmentFile.close();
                    }

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

        SegmentFile segmentFile = SegmentFile.openExisting(segmentFiles.get(0), false);
        try {
            garbageCollector.runCompaction(segmentFile);
        } finally {
            segmentFile.close();
        }

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

        // Verify temporary files are cleaned up
        assertFalse(Files.exists(tmpFile1));
        assertFalse(Files.exists(tmpFile2));

        // Verify duplicate segment files are cleaned up (lower generation removed)
        assertFalse(Files.exists(segmentFile1Gen0));
        assertTrue(Files.exists(segmentFile1Gen1));
        assertFalse(Files.exists(indexFile1Gen0));
        assertTrue(Files.exists(indexFile1Gen1));

        // Verify orphaned index files are cleaned up
        assertFalse(Files.exists(orphanedIndexFile));
    }

    @Test
    void testRunCompactionDoesNotRetainEntriesTruncatedBySuffix() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, batches.get(i), i);
        }

        fileManager.truncateSuffix(GROUP_ID_1, 1);

        triggerAndAwaitCheckpoint(1);

        // Since we truncated all entries after log index 1, we can expect that the second segment file will be fully compacted.
        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(greaterThan(2)));

        SegmentFile segmentFile = SegmentFile.openExisting(segmentFiles.get(1), false);
        try {
            garbageCollector.runCompaction(segmentFile);
        } finally {
            segmentFile.close();
        }

        assertThat(Files.exists(segmentFiles.get(1)), is(false));
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
}
