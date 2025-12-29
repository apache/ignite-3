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
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.computeDefaultMaxLogEntrySizeBytes;
import static org.apache.ignite.internal.raft.storage.segstore.ByteChannelUtils.readFully;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.HEADER_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(ConfigurationExtension.class)
class SegmentFileManagerTest extends IgniteAbstractTest {
    private static final int FILE_SIZE = 100;

    private static final long GROUP_ID = 1000;

    private static final int STRIPES = 10;

    private static final String NODE_NAME = "test";

    private final FailureManager failureManager = new NoOpFailureManager();

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration("mock.segmentFileSizeBytes=" + FILE_SIZE)
    private LogStorageConfiguration storageConfiguration;

    private SegmentFileManager fileManager;

    @BeforeEach
    void setUp() throws IOException {
        fileManager = createFileManager();

        fileManager.start();
    }

    private SegmentFileManager createFileManager() throws IOException {
        return new SegmentFileManager(
                NODE_NAME,
                workDir,
                STRIPES,
                failureManager,
                raftConfiguration,
                storageConfiguration
        );
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(fileManager);
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

        byte[] bytes = new byte[FILE_SIZE - HEADER_RECORD.length - 2 * SegmentPayload.fixedOverheadSize()];

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
                "Segment entry is too big (%d bytes), maximum allowed segment entry size: %d bytes.",
                FILE_SIZE + SegmentPayload.fixedOverheadSize() + 2, // 2 bytes for index and term.
                computeDefaultMaxLogEntrySizeBytes(FILE_SIZE)
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

        List<byte[]> actualData = readDataFromSegmentFiles().stream()
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

                assertThat(stopTask, willCompleteSuccessfully());
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

        List<byte[]> actualData = readDataFromSegmentFiles().stream()
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

    @Test
    void truncateRecordIsWrittenOnSuffixTruncate() throws IOException {
        long groupId = 36;

        long lastLogIndexKept = 42;

        fileManager.truncateSuffix(groupId, lastLogIndexKept);

        Path path = findSoleSegmentFile();

        ByteBuffer expectedTruncateRecord = ByteBuffer.allocate(SegmentPayload.TRUNCATE_SUFFIX_RECORD_SIZE)
                .order(SegmentFile.BYTE_ORDER);

        SegmentPayload.writeTruncateSuffixRecordTo(expectedTruncateRecord, groupId, lastLogIndexKept);

        expectedTruncateRecord.rewind();

        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            channel.position(HEADER_RECORD.length);

            assertThat(readFully(channel, SegmentPayload.TRUNCATE_SUFFIX_RECORD_SIZE), is(expectedTruncateRecord));
        }
    }

    @Test
    void truncateRecordIsWrittenOnPrefixTruncate() throws IOException {
        long groupId = 36;

        long firstLogIndexKept = 42;

        fileManager.truncatePrefix(groupId, firstLogIndexKept);

        Path path = findSoleSegmentFile();

        ByteBuffer expectedTruncateRecord = ByteBuffer.allocate(SegmentPayload.TRUNCATE_PREFIX_RECORD_SIZE)
                .order(SegmentFile.BYTE_ORDER);

        SegmentPayload.writeTruncatePrefixRecordTo(expectedTruncateRecord, groupId, firstLogIndexKept);

        expectedTruncateRecord.rewind();

        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            channel.position(HEADER_RECORD.length);

            assertThat(readFully(channel, SegmentPayload.TRUNCATE_PREFIX_RECORD_SIZE), is(expectedTruncateRecord));
        }
    }

    @Test
    void resetRecordIsWrittenOnReset() throws IOException {
        long groupId = 36;

        long nextLogIndex = 42;

        fileManager.reset(groupId, nextLogIndex);

        Path path = findSoleSegmentFile();

        ByteBuffer expectedTruncateRecord = ByteBuffer.allocate(SegmentPayload.RESET_RECORD_SIZE)
                .order(SegmentFile.BYTE_ORDER);

        SegmentPayload.writeResetRecordTo(expectedTruncateRecord, groupId, nextLogIndex);

        expectedTruncateRecord.rewind();

        try (SeekableByteChannel channel = Files.newByteChannel(path)) {
            channel.position(HEADER_RECORD.length);

            assertThat(readFully(channel, SegmentPayload.RESET_RECORD_SIZE), is(expectedTruncateRecord));
        }
    }

    @Test
    void testRecovery() throws Exception {
        int batchSize = FILE_SIZE / 4;

        List<byte[]> batches = randomData(batchSize, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i);
        }

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(greaterThan(1)));

        List<Path> indexFiles = await().until(this::indexFiles, hasSize(segmentFiles.size() - 1));

        fileManager.close();

        // Delete an index file. We expect it to be re-created after recovery.
        Files.delete(indexFiles.get(0));

        fileManager = createFileManager();

        fileManager.start();

        for (int i = 0; i < batches.size(); i++) {
            byte[] expectedEntry = batches.get(i);

            fileManager.getEntry(GROUP_ID, i, bs -> {
                assertThat(bs, is(expectedEntry));

                return null;
            });
        }
    }

    @Test
    void testRecoveryWithTmpIndexFiles() throws Exception {
        List<byte[]> batches = randomData(FILE_SIZE / 4, 3);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i);
        }

        // We need rollover to happen at least once to trigger index file recovery.
        await().until(this::indexFiles, hasSize(1));

        assertThat(segmentFiles(), hasSize(2));

        // Use a mock memtable that throws an exception to force the index manager to create a temporary index file, but not rename it.
        ReadModeIndexMemTable mockMemTable = mock(ReadModeIndexMemTable.class);

        when(mockMemTable.iterator()).thenThrow(new RuntimeException("Test exception"));

        // Create two tmp index files: one for the complete segment file and one the incomplete segment file.
        try {
            fileManager.indexFileManager().recoverIndexFile(mockMemTable, 0);
        } catch (RuntimeException ignored) {
            // Ignore.
        }

        try {
            fileManager.indexFileManager().recoverIndexFile(mockMemTable, 1);
        } catch (RuntimeException ignored) {
            // Ignore.
        }

        assertThat(tmpIndexFiles(), hasSize(2));

        fileManager.close();

        for (Path indexFile : indexFiles()) {
            Files.delete(indexFile);
        }

        fileManager = createFileManager();

        fileManager.start();

        assertThat(tmpIndexFiles(), is(empty()));

        assertThat(indexFiles(), hasSize(1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTruncateSuffix(boolean restart) throws Exception {
        List<byte[]> batches = randomData(FILE_SIZE / 4, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i);
        }

        await().until(this::indexFiles, hasSize(4));

        int lastLogIndexKept = batches.size() / 2;

        fileManager.truncateSuffix(GROUP_ID, lastLogIndexKept);

        // Insert more data in order to have two truncate suffix records: one in a "rollovered" segment file and one in the latest segment
        // file.
        for (int i = lastLogIndexKept + 1; i < batches.size(); i++) {
            appendBytes(batches.get(i), i);
        }

        fileManager.truncateSuffix(GROUP_ID, lastLogIndexKept);

        if (restart) {
            fileManager.close();

            for (Path indexFile : indexFiles()) {
                Files.deleteIfExists(indexFile);
            }

            fileManager = createFileManager();

            fileManager.start();
        }

        for (int i = 0; i <= lastLogIndexKept; i++) {
            byte[] expectedEntry = batches.get(i);

            fileManager.getEntry(GROUP_ID, i, bs -> {
                assertThat(bs, is(expectedEntry));

                return null;
            });
        }

        for (int i = lastLogIndexKept + 1; i < batches.size(); i++) {
            fileManager.getEntry(GROUP_ID, i, bs -> {
                throw new AssertionError("This method should not be called.");
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testTruncatePrefix(boolean restart) throws Exception {
        List<byte[]> batches = randomData(FILE_SIZE / 4, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i);
        }

        await().until(this::indexFiles, hasSize(4));

        int firstLogIndexKept = batches.size() / 2;

        fileManager.truncatePrefix(GROUP_ID, firstLogIndexKept);

        // Insert more data, just in case.
        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i + batches.size());
        }

        if (restart) {
            fileManager.close();

            for (Path indexFile : indexFiles()) {
                Files.deleteIfExists(indexFile);
            }

            fileManager = createFileManager();

            fileManager.start();
        }

        for (int i = 0; i < batches.size() - firstLogIndexKept; i++) {
            byte[] expectedEntry = batches.get(firstLogIndexKept + i);

            fileManager.getEntry(GROUP_ID, i, bs -> {
                assertThat(bs, is(expectedEntry));

                return null;
            });
        }

        for (int i = 0; i < firstLogIndexKept; i++) {
            fileManager.getEntry(GROUP_ID, i, bs -> {
                throw new AssertionError("This method should not be called.");
            });
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testReset(boolean restart) throws Exception {
        List<byte[]> batches = randomData(FILE_SIZE / 4, 10);

        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i);
        }

        await().until(this::indexFiles, hasSize(4));

        int nextLogIndex = batches.size() / 2;

        fileManager.reset(GROUP_ID, nextLogIndex);

        // Insert more data, just in case.
        for (int i = 0; i < batches.size(); i++) {
            appendBytes(batches.get(i), i + nextLogIndex + 1);
        }

        if (restart) {
            fileManager.close();

            for (Path indexFile : indexFiles()) {
                Files.deleteIfExists(indexFile);
            }

            fileManager = createFileManager();

            fileManager.start();
        }

        fileManager.getEntry(GROUP_ID, nextLogIndex, bs -> {
            assertThat(bs, is(batches.get(nextLogIndex)));

            return null;
        });

        for (int i = 0; i < batches.size(); i++) {
            byte[] expectedEntry = batches.get(i);

            fileManager.getEntry(GROUP_ID, nextLogIndex + i + 1, bs -> {
                assertThat(bs, is(expectedEntry));

                return null;
            });
        }

        for (int i = 0; i < nextLogIndex; i++) {
            fileManager.getEntry(GROUP_ID, i, bs -> {
                throw new AssertionError("This method should not be called.");
            });
        }
    }

    private Path findSoleSegmentFile() throws IOException {
        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(1));

        return segmentFiles.get(0);
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(fileManager.segmentFilesDir())) {
            return files.sorted().collect(toList());
        }
    }

    private List<Path> indexFiles() throws IOException {
        try (Stream<Path> files = Files.list(fileManager.indexFilesDir())) {
            return files
                    .filter(p -> {
                        String fileName = p.getFileName().toString();

                        return !fileName.endsWith(".tmp");
                    })
                    .sorted()
                    .collect(toList());
        }
    }

    private List<Path> tmpIndexFiles() throws IOException {
        try (Stream<Path> files = Files.list(fileManager.indexFilesDir())) {
            return files
                    .filter(p -> {
                        String fileName = p.getFileName().toString();

                        return fileName.endsWith(".tmp");
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

    private List<DeserializedSegmentPayload> readDataFromSegmentFiles() throws IOException {
        var result = new ArrayList<DeserializedSegmentPayload>();

        for (Path segmentFile : segmentFiles()) {
            try (SeekableByteChannel channel = Files.newByteChannel(segmentFile)) {
                assertThat(readFully(channel, HEADER_RECORD.length).array(), is(HEADER_RECORD));

                int bytesRead = HEADER_RECORD.length;

                while (bytesRead < FILE_SIZE) {
                    long position = channel.position();

                    DeserializedSegmentPayload payload = DeserializedSegmentPayload.fromByteChannel(channel);

                    bytesRead += (int) (channel.position() - position);

                    if (payload == null) {
                        // EOF reached.
                        break;
                    }

                    result.add(payload);
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
