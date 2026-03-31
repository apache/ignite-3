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
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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

/**
 * Tests for {@link RaftLogGarbageCollector} soft-limit behavior.
 */
@ExtendWith(ConfigurationExtension.class)
class RaftLogGcSoftLimitTest extends IgniteAbstractTest {
    private static final int FILE_SIZE = 200;

    private static final long SMALL_SOFT_LIMIT = 2 * FILE_SIZE;

    private static final long GROUP_ID_1 = 1000;

    private static final long GROUP_ID_2 = 2000;

    private static final int STRIPES = 10;

    private static final String NODE_NAME = "test";

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration(
            value = "mock.segmentFileSizeBytes=" + FILE_SIZE + ", mock.softLogSizeLimitBytes=" + SMALL_SOFT_LIMIT,
            validate = false
    )
    private LogStorageConfiguration storageConfiguration;

    private SegmentFileManager fileManager;

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

        garbageCollector = fileManager.garbageCollector();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (fileManager != null) {
            fileManager.close();
        }
    }

    @Test
    void testGcTriggeredWhenSoftLimitExceeded() throws Exception {
        List<byte[]> batches = createRandomData(FILE_SIZE / 4, 10);
        for (int i = 0; i < batches.size(); i++) {
            appendBytes(GROUP_ID_1, i, batches.get(i));
        }

        await().until(() -> indexFiles(fileManager), hasSize(4));

        assertThat(totalLogSizeFromDisk(fileManager), is(greaterThanOrEqualTo(SMALL_SOFT_LIMIT)));

        // Truncate all GROUP_ID_1 entries so every old file becomes fully deletable.
        fileManager.truncatePrefix(GROUP_ID_1, batches.size() - 1);

        // Write one GROUP_ID_2 batch to force a rollover so the truncation record ends up in a
        // completed (checkpointable) file.
        appendBytes(GROUP_ID_2, batches.size() + 1, createRandomData(FILE_SIZE / 4, 1).get(0));

        await().until(() -> garbageCollector.logSizeBytes(), is(lessThanOrEqualTo(SMALL_SOFT_LIMIT)));

        assertThat(garbageCollector.logSizeBytes(), is(totalLogSizeFromDisk(fileManager)));
    }

    @RepeatedTest(10)
    void testConcurrentWritesAndAutoGc() throws Exception {
        int numBatches = 20;
        List<byte[]> batches = createRandomData(FILE_SIZE / 8, numBatches);

        // Write initial data from both groups.
        for (int i = 0; i < numBatches; i++) {
            appendBytes(GROUP_ID_1, i, batches.get(i));
            appendBytes(GROUP_ID_2, i, batches.get(i));
        }

        // Wait for checkpoints so the GC has candidates to compact.
        await().until(() -> indexFiles(fileManager), hasSize(greaterThanOrEqualTo(1)));

        var writerDone = new AtomicBoolean(false);

        RunnableX writerTask = () -> {
            try {
                for (int i = numBatches; i < numBatches * 3; i++) {
                    appendBytes(GROUP_ID_1, i, batches.get(i % numBatches));
                    appendBytes(GROUP_ID_2, i, batches.get(i % numBatches));
                    fileManager.truncatePrefix(GROUP_ID_1, i - 1);
                }
            } finally {
                writerDone.set(true);
            }
        };

        RunnableX readerTask = () -> {
            while (!writerDone.get()) {
                for (int i = 0; i < numBatches; i++) {
                    int index = i;
                    fileManager.getEntry(GROUP_ID_2, i, bs -> {
                        assertThat(bs, is(batches.get(index)));

                        return null;
                    });
                }
            }
        };

        runRace(writerTask, readerTask, readerTask);
    }

    private static List<Path> indexFiles(SegmentFileManager manager) throws IOException {
        try (Stream<Path> files = Files.list(manager.indexFilesDir())) {
            return files
                    .filter(p -> !p.getFileName().toString().endsWith(".tmp"))
                    .sorted()
                    .collect(Collectors.toList());
        }
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

    private static List<byte[]> createRandomData(int batchLength, int numBatches) {
        return IntStream.range(0, numBatches)
                .mapToObj(i -> randomBytes(ThreadLocalRandom.current(), batchLength))
                .collect(Collectors.toList());
    }

    private void appendBytes(long groupId, long index, byte[] serializedEntry) throws IOException {
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
}
