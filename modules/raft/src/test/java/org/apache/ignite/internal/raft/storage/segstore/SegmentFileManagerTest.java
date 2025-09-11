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

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.HEADER_RECORD;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.io.InputStream;
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
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.storage.segstore.SegmentFile.WriteBuffer;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class SegmentFileManagerTest extends IgniteAbstractTest {
    private static final int FILE_SIZE = 100;

    private SegmentFileManager fileManager;

    @BeforeEach
    void setUp() throws IOException {
        fileManager = new SegmentFileManager(workDir, FILE_SIZE);

        fileManager.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(fileManager);
    }

    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    @Test
    void testConstructorInvariants() {
        assertThrows(IllegalArgumentException.class, () -> new SegmentFileManager(workDir, 0));
        assertThrows(IllegalArgumentException.class, () -> new SegmentFileManager(workDir, 1));
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
    void checkSegmentNamingAfterRollovers() throws Exception {
        int segmentFilesNum = 10;

        byte[] bytes = new byte[FILE_SIZE - HEADER_RECORD.length];

        for (int i = 0; i < segmentFilesNum; i++) {
            try (WriteBuffer buffer = fileManager.reserve(bytes.length)) {
                buffer.buffer().put(bytes);
            }
        }

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(segmentFilesNum));

        for (int i = 0; i < segmentFilesNum; i++) {
            String expectedFileName = String.format("segment-%010d-0000000000.bin", i);

            assertThat(segmentFiles.get(i).getFileName().toString(), is(expectedFileName));
        }
    }

    @Test
    void throwsIfReserveSizeIsTooBig() {
        //noinspection resource
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> fileManager.reserve(FILE_SIZE));

        assertThat(e.getMessage(), is("Entry size is too big (100 bytes), maximum allowed entry size: 92 bytes."));
    }

    @Test
    void validateDataWithoutRollovers() throws Exception {
        int batchSize = FILE_SIZE / 10;

        List<byte[]> batches = randomData(batchSize, 4);

        for (byte[] batch : batches) {
            try (WriteBuffer buffer = fileManager.reserve(batchSize)) {
                buffer.buffer().put(batch);
            }
        }

        Path segmentFile = findSoleSegmentFile();

        try (InputStream is = Files.newInputStream(segmentFile)) {
            assertThat(is.readNBytes(HEADER_RECORD.length), is(HEADER_RECORD));

            for (byte[] expectedBatch : batches) {
                assertThat(is.readNBytes(expectedBatch.length), is(expectedBatch));
            }
        }
    }

    @Test
    void validateDataAfterRollovers() throws Exception {
        // This size guarantees that only one batch will fit in a single segment file with enough space for the segment switch record.
        int batchSize = FILE_SIZE / 2;

        List<byte[]> batches = randomData(batchSize, 4);

        for (byte[] batch : batches) {
            try (WriteBuffer buffer = fileManager.reserve(batchSize)) {
                buffer.buffer().put(batch);
            }
        }

        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(batches.size()));

        for (int i = 0; i < batches.size(); i++) {
            byte[] expectedBatch = batches.get(i);

            try (InputStream is = Files.newInputStream(segmentFiles.get(i))) {
                assertThat(is.readNBytes(HEADER_RECORD.length), is(HEADER_RECORD));
                assertThat(is.readNBytes(expectedBatch.length), is(expectedBatch));

                if (i != batches.size() - 1) {
                    // All segment files except the last one must contain a segment switch record.
                    assertThat(is.readNBytes(SWITCH_SEGMENT_RECORD.length), is(SWITCH_SEGMENT_RECORD));
                }
            }
        }
    }

    @Test
    void testConcurrentWrites(@InjectExecutorService(threadCount = 10) ExecutorService executor) throws IOException {
        int batchSize = FILE_SIZE / 10;

        List<byte[]> batches = randomData(batchSize, 100);

        CompletableFuture<?>[] tasks = batches.stream()
                .map(batch -> runAsync(() -> {
                    try (WriteBuffer buffer = fileManager.reserve(batchSize)) {
                        buffer.buffer().put(batch);
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, executor))
                .toArray(CompletableFuture[]::new);

        assertThat(allOf(tasks), willCompleteSuccessfully());

        assertThat(segmentFiles(), hasSize(greaterThan(1)));

        assertThat(readDataFromSegmentFiles(batchSize, batches.size()), containsInAnyOrder(batches.toArray()));
    }

    @Test
    void testConcurrentWritesWithClose(@InjectExecutorService(threadCount = 10) ExecutorService executor) throws IOException {
        int batchSize = FILE_SIZE / 10;

        List<byte[]> batches = randomData(batchSize, 1000);

        @SuppressWarnings("unchecked")
        CompletableFuture<byte[]>[] tasks = new CompletableFuture[batches.size()];

        for (int i = 0; i < batches.size(); i++) {
            // Close the manager somewhere in between.
            if (i == batches.size() / 2) {
                runAsync(() -> {
                    try {
                        fileManager.close();
                    } catch (Exception e) {
                        throw new CompletionException(e);
                    }
                }, executor);
            }

            byte[] batch = batches.get(i);

            tasks[i] = supplyAsync(() -> {
                try (WriteBuffer buffer = fileManager.reserve(batchSize)) {
                    buffer.buffer().put(batch);

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

        assertThat(readDataFromSegmentFiles(batchSize, writtenBatches.size()), containsInAnyOrder(writtenBatches.toArray()));

        assertThat(exceptions, is(not(empty())));

        for (IgniteInternalException e : exceptions) {
            assertThat(e.code(), is(NODE_STOPPING_ERR));
        }
    }

    private Path findSoleSegmentFile() throws IOException {
        List<Path> segmentFiles = segmentFiles();

        assertThat(segmentFiles, hasSize(1));

        return segmentFiles.get(0);
    }

    private List<Path> segmentFiles() throws IOException {
        try (Stream<Path> files = Files.list(workDir)) {
            return files.sorted().collect(toList());
        }
    }

    private static List<byte[]> randomData(int batchLength, int numBatches) {
        return IntStream.range(0, numBatches)
                .mapToObj(i -> randomBytes(ThreadLocalRandom.current(), batchLength))
                .collect(toList());
    }

    private List<byte[]> readDataFromSegmentFiles(int batchLength, int numBatches) throws IOException {
        var result = new ArrayList<byte[]>(numBatches);

        for (Path segmentFile : segmentFiles()) {
            try (InputStream is = Files.newInputStream(segmentFile)) {
                assertThat(is.readNBytes(HEADER_RECORD.length), is(HEADER_RECORD));

                int bytesRead = HEADER_RECORD.length;

                while (bytesRead + batchLength < FILE_SIZE && result.size() < numBatches) {
                    result.add(is.readNBytes(batchLength));

                    bytesRead += batchLength;
                }

                if (FILE_SIZE - bytesRead >= SWITCH_SEGMENT_RECORD.length) {
                    assertThat(is.readNBytes(SWITCH_SEGMENT_RECORD.length), is(SWITCH_SEGMENT_RECORD));
                }
            }
        }

        return result;
    }
}
