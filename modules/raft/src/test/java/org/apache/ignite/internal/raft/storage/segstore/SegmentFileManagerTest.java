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

import static java.util.Comparator.comparingLong;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.raft.storage.segstore.ByteChannelUtils.readFully;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.HEADER_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class SegmentFileManagerTest extends IgniteAbstractTest {
    private static final int FILE_SIZE = 100;

    private static final long GROUP_ID = 1000;

    private static final int STRIPES = 10;

    private static final String NODE_NAME = "test";

    private SegmentFileManager fileManager;

    @BeforeEach
    void setUp() throws IOException {
        fileManager = new SegmentFileManager(NODE_NAME, workDir, FILE_SIZE, STRIPES, new NoOpFailureManager());

        fileManager.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(fileManager);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test
    void testConstructorInvariants() {
        assertThrows(IllegalArgumentException.class, () -> new SegmentFileManager(NODE_NAME, workDir, 0, 1, new NoOpFailureManager()));
        assertThrows(IllegalArgumentException.class, () -> new SegmentFileManager(NODE_NAME, workDir, 1, 1, new NoOpFailureManager()));
    }

    @Test
    void segmentFileIsInitializedAfterStart() throws IOException {
        Path segmentFile = findSoleSegmentFile();

        assertThat(segmentFile.getFileName().toString(), is("segment-0000000000-0000000000.bin"));

        assertThat(Files.size(segmentFile), is((long) FILE_SIZE));

        try (InputStream is = Files.newInputStream(segmentFile)) {
            assertThat(is.readNBytes(HEADER_RECORD.length), is(HEADER_RECORD));
        }
    }

    @Test
    void checkFileNamingAfterRollovers() throws Exception {
        int segmentFilesNum = 10;

        byte[] bytes = new byte[FILE_SIZE - HEADER_RECORD.length - SegmentPayload.overheadSize()];

        for (int i = 0; i < segmentFilesNum; i++) {
            appendBytes(bytes, i);
        }

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(segmentFilesNum));

        for (int i = 0; i < segmentFiles.size(); i++) {
            String expectedFileName = String.format("segment-%010d-0000000000.bin", i);

            assertThat(segmentFiles.get(i).getFileName().toString(), is(expectedFileName));
        }

        List<Path> indexFiles = await().until(this::indexFiles, hasSize(segmentFilesNum - 1));

        for (int i = 0; i < indexFiles.size(); i++) {
            String expectedFileName = String.format("index-%010d-0000000000.bin", i);

            assertThat(indexFiles.get(i).getFileName().toString(), is(expectedFileName));
        }
    }

    @Test
    void throwsIfReserveSizeIsTooBig() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> appendBytes(new byte[FILE_SIZE], 0));

        String expectedMessage = String.format(
                "Entry size is too big (%d bytes), maximum allowed entry size: %d bytes.",
                FILE_SIZE + SegmentPayload.overheadSize(),
                FILE_SIZE - HEADER_RECORD.length
        );

        assertThat(e.getMessage(), is(expectedMessage));
    }

    @Test
    void validateDataWithoutRollovers() throws Exception {
        int batchSize = FILE_SIZE / 10;

        List<byte[]> batches = randomData(batchSize, 3);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i);
        }

        Path segmentFile = findSoleSegmentFile();

        try (ByteChannel channel = Files.newByteChannel(segmentFile)) {
            assertThat(readFully(channel, HEADER_RECORD.length).array(), is(HEADER_RECORD));

            for (byte[] expectedBatch : batches) {
                validateSegmentEntry(channel, expectedBatch);
            }
        }

        assertThat(indexFiles(), is(empty()));
    }

    @Test
    void validateDataAfterRollovers() throws Exception {
        // This size guarantees that only one batch will fit in a single segment file with enough space for the segment switch record.
        int batchSize = FILE_SIZE / 2;

        List<byte[]> batches = randomData(batchSize, 4);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i);
        }

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(batches.size()));

        for (int i = 0; i < batches.size(); i++) {
            byte[] expectedBatch = batches.get(i);

            try (ByteChannel channel = Files.newByteChannel(segmentFiles.get(i))) {
                assertThat(readFully(channel, HEADER_RECORD.length).array(), is(HEADER_RECORD));

                validateSegmentEntry(channel, expectedBatch);

                if (i != batches.size() - 1) {
                    // All segment files except the last one must contain a segment switch record.
                    assertThat(readFully(channel, SWITCH_SEGMENT_RECORD.length).array(), is(SWITCH_SEGMENT_RECORD));
                }
            }
        }

        List<Path> indexFiles = await().until(this::indexFiles, hasSize(segmentFiles.size() - 1));

        for (int i = 0; i < indexFiles.size(); i++) {
            validateIndexFile(indexFiles.get(i), segmentFiles.get(i));
        }
    }

    @RepeatedTest(10)
    void testConcurrentWrites() throws IOException {
        int batchSize = FILE_SIZE / 5;

        List<byte[]> batches = randomData(batchSize, 10);

        var tasks = new RunnableX[batches.size()];

        for (int i = 0; i < batches.size(); i++) {
            byte[] batch = batches.get(i);

            int index = i;

            tasks[i] = () -> appendBytes(index + 1, batch, index);
        }

        runRace(tasks);

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(greaterThan(1)));

        List<byte[]> actualData = readDataFromSegmentFiles(batchSize, batches.size()).stream()
                .sorted(comparingLong(DeserializedSegmentPayload::groupId))
                .map(DeserializedSegmentPayload::payload)
                .collect(toList());

        assertThat(actualData, contains(batches.toArray()));

        List<Path> indexFiles = await().until(this::indexFiles, hasSize(segmentFiles.size() - 1));

        for (int i = 0; i < indexFiles.size(); i++) {
            validateIndexFile(indexFiles.get(i), segmentFiles.get(i));
        }
    }

    @RepeatedTest(10)
    void testConcurrentWritesWithClose(@InjectExecutorService(threadCount = 10) ExecutorService executor) throws Exception {
        int batchSize = FILE_SIZE / 10;

        List<byte[]> batches = randomData(batchSize, 100);

        @SuppressWarnings("unchecked")
        CompletableFuture<byte[]>[] tasks = new CompletableFuture[batches.size()];

        CompletableFuture<Void> stopTask = null;

        for (int i = 0; i < batches.size(); i++) {
            // Close the manager somewhere in between.
            if (i == batches.size() / 2) {
                stopTask = runAsync(() -> {
                    try {
                        fileManager.close();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, executor);
            }

            // Wait for the stop task to complete on the last iteration to guarantee that at least one task will finish with an exception.
            if (i == batches.size() - 1) {
                assertNotNull(stopTask);

                stopTask.get(1, TimeUnit.SECONDS);
            }

            byte[] batch = batches.get(i);

            int index = i;

            tasks[i] = supplyAsync(() -> {
                try {
                    appendBytes(index + 1, batch, index);

                    return batch;
                } catch (Exception e) {
                    throw new CompletionException(e);
                }
            }, executor);
        }

        assertThat(allOf(tasks), willThrow(IgniteInternalException.class));

        var writtenBatches = new ArrayList<byte[]>();
        var exceptions = new ArrayList<IgniteInternalException>();

        for (CompletableFuture<byte[]> task : tasks) {
            try {
                writtenBatches.add(task.join());
            } catch (CompletionException e) {
                if (!(e.getCause() instanceof IgniteInternalException)) {
                    throw e;
                }

                exceptions.add((IgniteInternalException) e.getCause());
            }
        }

        assertThat(writtenBatches, is(not(empty())));
        assertThat(exceptions, is(not(empty())));

        List<byte[]> actualData = readDataFromSegmentFiles(batchSize, writtenBatches.size()).stream()
                .sorted(comparingLong(DeserializedSegmentPayload::groupId))
                .map(DeserializedSegmentPayload::payload)
                .collect(toList());

        assertThat(actualData, contains(writtenBatches.toArray()));

        for (IgniteInternalException e : exceptions) {
            assertThat(e.code(), is(NODE_STOPPING_ERR));
        }

        List<Path> segmentFiles = segmentFiles();

        // We don't need to wait for index files to appear, because the file manager has been stopped.
        List<Path> indexFiles = indexFiles();

        for (int i = 0; i < indexFiles.size(); i++) {
            validateIndexFile(indexFiles.get(i), segmentFiles.get(i));
        }
    }

    private Path findSoleSegmentFile() throws IOException {
        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(1));

        return segmentFiles.get(0);
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(workDir)) {
            return files
                    .filter(p -> p.getFileName().toString().startsWith("segment"))
                    .sorted()
                    .collect(toList());
        }
    }

    private List<Path> indexFiles() throws IOException {
        try (Stream<Path> files = Files.list(workDir)) {
            return files
                    .filter(p -> {
                        String fileName = p.getFileName().toString();

                        return fileName.startsWith("index") && !fileName.endsWith(".tmp");
                    })
                    .sorted()
                    .collect(toList());
        }
    }

    private static List<byte[]> randomData(int batchLength, int numBatches) {
        return IntStream.range(0, numBatches)
                .mapToObj(i -> randomBytes(ThreadLocalRandom.current(), batchLength))
                .collect(toList());
    }

    private List<DeserializedSegmentPayload> readDataFromSegmentFiles(int batchLength, int numBatches) throws IOException {
        var result = new ArrayList<DeserializedSegmentPayload>(numBatches);

        int entrySize = batchLength + SegmentPayload.overheadSize();

        for (Path segmentFile : segmentFiles()) {
            try (SeekableByteChannel channel = Files.newByteChannel(segmentFile)) {
                assertThat(readFully(channel, HEADER_RECORD.length).array(), is(HEADER_RECORD));

                int bytesRead = HEADER_RECORD.length;

                while (bytesRead + entrySize < FILE_SIZE && result.size() < numBatches) {
                    long position = channel.position();

                    result.add(DeserializedSegmentPayload.fromByteChannel(channel));

                    assertThat(channel.position(), is(position + entrySize));

                    bytesRead += entrySize;
                }

                if (FILE_SIZE - bytesRead >= SWITCH_SEGMENT_RECORD.length) {
                    assertThat(readFully(channel, SWITCH_SEGMENT_RECORD.length).array(), is(SWITCH_SEGMENT_RECORD));
                }
            }
        }

        return result;
    }

    private void appendBytes(byte[] serializedEntry, int index) throws IOException {
        appendBytes(GROUP_ID, serializedEntry, index);
    }

    private void appendBytes(long groupId, byte[] serializedEntry, int index) throws IOException {
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

    private static void validateSegmentEntry(ReadableByteChannel channel, byte[] expectedPayload) throws IOException {
        DeserializedSegmentPayload deserializedSegmentPayload = DeserializedSegmentPayload.fromByteChannel(channel);

        assertThat(deserializedSegmentPayload, is(notNullValue()));
        assertThat(deserializedSegmentPayload.groupId(), is(GROUP_ID));
        assertThat(deserializedSegmentPayload.payload(), is(expectedPayload));
    }

    private static void validateIndexFile(Path indexFilePath, Path segmentFilePath) throws IOException {
        DeserializedIndexFile indexFile = DeserializedIndexFile.fromFile(indexFilePath);

        try (SeekableByteChannel channel = Files.newByteChannel(segmentFilePath)) {
            for (DeserializedIndexFile.Entry indexEntry : indexFile.entries()) {
                channel.position(indexEntry.segmentFileOffset());

                DeserializedSegmentPayload segmentPayload = DeserializedSegmentPayload.fromByteChannel(channel);

                assertThat(segmentPayload, is(notNullValue()));
                assertThat(segmentPayload.groupId(), is(indexEntry.groupId()));
            }
        }
    }
}
