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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.concat;
import static org.apache.ignite.internal.util.ByteUtils.bytesToInt;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.raft.storage.segstore.SegmentFile.WriteBuffer;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.internal.util.ByteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link SegmentFile}.
 */
@ExtendWith(ExecutorServiceExtension.class)
class SegmentFileTest extends IgniteAbstractTest {
    private static final String FILE_NAME = SegmentFile.fileName(new FileProperties(0));

    private Path path;

    private SegmentFile file;

    @BeforeEach
    void setUp() {
        path = workDir.resolve(FILE_NAME);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(file);
    }

    /**
     * Tests the happy-case append scenario.
     */
    @Test
    void testReserve() throws IOException {
        createSegmentFile(300);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        int dataLength = 100;

        byte[] bytes1 = randomBytes(random, dataLength);
        byte[] bytes2 = randomBytes(random, dataLength);

        assertTrue(writeToSegmentFile(bytes1));
        assertTrue(writeToSegmentFile(bytes2));

        assertThat(readFileContent(bytes1.length + bytes2.length), is(concat(bytes1, bytes2)));
    }

    @Test
    void testCreateNewConstructorInvariants() {
        assertThrows(IllegalArgumentException.class, () -> SegmentFile.createNew(path, -1, false));
        assertThrows(IllegalArgumentException.class, () -> SegmentFile.createNew(path, Integer.MAX_VALUE + 1L, false));
    }

    @Test
    void testOpenExistingConstructorInvariants() throws IOException {
        assertThrows(IllegalArgumentException.class, () -> SegmentFile.openExisting(path, false));

        createSegmentFile(1);

        assertDoesNotThrow(() -> SegmentFile.openExisting(path, false));
    }

    /**
     * Tests a situation when file gets overflown with consecutive append calls.
     */
    @Test
    void testReserveIterativeOverflow() throws IOException {
        int fileSize = 100;

        createSegmentFile(fileSize);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        byte[] bytes1 = randomBytes(random, fileSize - 1);
        byte[] bytes2 = randomBytes(random, 1);

        assertTrue(writeToSegmentFile(bytes1));
        assertFalse(writeToSegmentFile(new byte[2]));
        assertTrue(writeToSegmentFile(bytes2));

        assertThat(readFileContent(bytes1.length + bytes2.length), is(concat(bytes1, bytes2)));
    }

    /**
     * Tests a situation when file gets overflown with a single big append call.
     */
    @Test
    void testReserveSingleBatchOverflow() throws IOException {
        int fileSize = 100;

        createSegmentFile(fileSize);

        assertFalse(writeToSegmentFile(new byte[fileSize + 1]));
    }

    /**
     * Tests that append requests return {@code false} after the file is closed.
     */
    @Test
    void testClose() throws IOException {
        createSegmentFile(100);

        file.close();

        assertFalse(writeToSegmentFile(new byte[1]));
    }

    /**
     * Tests that append requests return {@code false} after the file is closed and rollover bytes are written at the end.
     */
    @Test
    void testCloseForRollover() throws IOException {
        createSegmentFile(100);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        byte[] bytes = new byte[50];

        byte[] bytesForRollover = randomBytes(random, 50);

        assertTrue(writeToSegmentFile(bytes));

        file.closeForRollover(bytesForRollover);

        assertFalse(writeToSegmentFile(new byte[1]));

        assertThat(readFileContent(bytes.length + bytesForRollover.length), is(concat(bytes, bytesForRollover)));
    }

    /**
     * Tests that rollover bytes are not written if there's no space left in the file.
     */
    @Test
    void testCloseForRolloverOverflow() throws IOException {
        int fileSize = 100;

        createSegmentFile(fileSize);

        var bytes = new byte[fileSize];

        assertTrue(writeToSegmentFile(bytes));

        file.closeForRollover(new byte[] {1, 2, 3});

        assertThat(readFileContent(bytes.length), is(bytes));
    }

    /**
     * Tests a multi-threaded happy-case append scenario. We expect that bytes do not get intertwined.
     */
    @RepeatedTest(10)
    void testMultiThreadedReserve() throws IOException {
        int maxEntrySize = 100;

        int fileSize = 1_000;

        int numElements = fileSize / maxEntrySize;

        List<byte[]> data = generateData(numElements, maxEntrySize);

        createSegmentFile(fileSize);

        RunnableX[] tasks = data.stream()
                .map(bytes -> (RunnableX) () -> assertTrue(writeToSegmentFile(bytes)))
                .toArray(RunnableX[]::new);

        runRace(tasks);

        assertThat(readDataFromFile(numElements), containsInAnyOrder(data.toArray()));
    }

    /**
     * Tests a multi-threaded append scenario when some bytes get rejected due to an overflow. We expect that only the
     * successfully written bytes get written to the file.
     */
    @RepeatedTest(10)
    void testMultiThreadedReserveWithOverflow() throws IOException {
        int maxEntrySize = 100;

        int fileSize = 500;

        int numElements = 20;

        List<byte[]> data = generateData(numElements, maxEntrySize);

        createSegmentFile(fileSize);

        var successfullyWritten = new ConcurrentLinkedQueue<byte[]>();
        var notWritten = new ConcurrentLinkedQueue<byte[]>();

        RunnableX[] tasks = data.stream()
                .map(bytes -> (RunnableX) () -> {
                    boolean success = writeToSegmentFile(bytes);

                    if (success) {
                        successfullyWritten.add(bytes);
                    } else {
                        notWritten.add(bytes);
                    }
                })
                .toArray(RunnableX[]::new);

        runRace(tasks);

        assertThat(notWritten, is(not(empty())));

        assertThat(readDataFromFile(successfullyWritten.size()), containsInAnyOrder(successfullyWritten.toArray()));
    }

    /**
     * Tests a scenario when a file gets closed in the middle of concurrent append operations.
     */
    @RepeatedTest(10)
    void testMultithreadedClose(@InjectExecutorService(threadCount = 10) ExecutorService executor) throws IOException {
        int maxEntrySize = 100;

        int fileSize = 10_000;

        int numElements = fileSize / maxEntrySize;

        List<byte[]> data = generateData(numElements, maxEntrySize);

        createSegmentFile(fileSize);

        @SuppressWarnings("unchecked")
        CompletableFuture<Boolean>[] tasks = new CompletableFuture[numElements];

        CompletableFuture<Void> closeTask = null;

        for (int i = 0; i < numElements; i++) {
            byte[] bytes = data.get(i);

            // Post a task to close the file somewhere in the middle.
            if (i == numElements / 2) {
                closeTask = runAsync(() -> file.close(), executor);
            }

            // Wait for the file to be closed on the last iteration to guarantee that at least one write will fail.
            if (i == numElements - 1) {
                assertThat(closeTask, is(notNullValue()));

                tasks[i] = closeTask.thenApplyAsync(v -> writeToSegmentFile(bytes), executor);
            } else {
                tasks[i] = supplyAsync(() -> writeToSegmentFile(bytes), executor);
            }
        }

        assertThat(allOf(tasks), willCompleteSuccessfully());

        List<byte[]> successfullyWritten = new ArrayList<>();
        List<byte[]> notWritten = new ArrayList<>();

        for (int i = 0; i < tasks.length; i++) {
            if (tasks[i].join()) {
                successfullyWritten.add(data.get(i));
            } else {
                notWritten.add(data.get(i));
            }
        }

        assertThat(notWritten, is(not(empty())));

        assertThat(readDataFromFile(successfullyWritten.size()), containsInAnyOrder(successfullyWritten.toArray()));
    }

    /**
     * Tests a scenario when a file gets closed in the middle of concurrent append operations. We expect that rollover
     * bytes are written at the end.
     */
    @RepeatedTest(10)
    void testMultiThreadedCloseForRollover(@InjectExecutorService(threadCount = 10) ExecutorService executor) throws IOException {
        int maxEntrySize = 100;

        int fileSize = 10_000;

        int numElements = fileSize / maxEntrySize;

        List<byte[]> data = generateData(numElements, maxEntrySize);

        byte[] bytesForRollover = {1, 2, 3};

        createSegmentFile(fileSize);

        @SuppressWarnings("unchecked")
        CompletableFuture<Boolean>[] tasks = new CompletableFuture[numElements];

        CompletableFuture<Void> closeTask = null;

        for (int i = 0; i < numElements; i++) {
            byte[] bytes = data.get(i);

            // Post a task to close the file somewhere in the middle.
            if (i == numElements / 2) {
                closeTask = runAsync(() -> file.closeForRollover(bytesForRollover), executor);
            }

            // Wait for the file to be closed on the last iteration to guarantee that at least one write will fail.
            if (i == numElements - 1) {
                assertThat(closeTask, is(notNullValue()));

                tasks[i] = closeTask.thenApplyAsync(v -> writeToSegmentFile(bytes), executor);
            } else {
                tasks[i] = supplyAsync(() -> writeToSegmentFile(bytes), executor);
            }
        }

        assertThat(allOf(tasks), willCompleteSuccessfully());

        List<byte[]> successfullyWritten = new ArrayList<>();
        List<byte[]> notWritten = new ArrayList<>();

        for (int i = 0; i < tasks.length; i++) {
            if (tasks[i].join()) {
                successfullyWritten.add(data.get(i));
            } else {
                notWritten.add(data.get(i));
            }
        }

        assertThat(notWritten, is(not(empty())));

        assertThat(readDataFromFile(successfullyWritten.size()), containsInAnyOrder(successfullyWritten.toArray()));

        int offset = successfullyWritten.stream().mapToInt(bytes -> bytes.length).sum();

        assertThat(readFileContent(offset, bytesForRollover.length), is(bytesForRollover));
    }

    @Test
    void testSync() throws IOException {
        createSegmentFile(100);

        assertThat(file.syncPosition(), is(0));

        try (WriteBuffer ignored = file.reserve(5)) {
            // No-op.
        }

        try (WriteBuffer ignored = file.reserve(7)) {
            // No-op.
        }

        assertThat(file.syncPosition(), is(0));

        file.close();

        file.sync();

        assertThat(file.syncPosition(), is(12));
    }

    @Test
    void testSyncWithRollover() throws IOException {
        createSegmentFile(100);

        assertThat(file.syncPosition(), is(0));

        try (WriteBuffer ignored = file.reserve(5)) {
            // No-op.
        }

        try (WriteBuffer ignored = file.reserve(7)) {
            // No-op.
        }

        assertThat(file.syncPosition(), is(0));

        file.closeForRollover(new byte[] {1, 2, 3});

        file.sync();

        assertThat(file.syncPosition(), is(15));
    }

    @Test
    void testSyncWithNotEnoughSpaceForRollover() throws IOException {
        createSegmentFile(13);

        assertThat(file.syncPosition(), is(0));

        try (WriteBuffer ignored = file.reserve(5)) {
            // No-op.
        }

        try (WriteBuffer ignored = file.reserve(7)) {
            // No-op.
        }

        assertThat(file.syncPosition(), is(0));

        file.closeForRollover(new byte[] {1, 2, 3});

        file.sync();

        assertThat(file.syncPosition(), is(12));
    }

    private void createSegmentFile(int size) throws IOException {
        file = SegmentFile.createNew(path, size, false);
    }

    private boolean writeToSegmentFile(byte[] bytes) {
        try (WriteBuffer writeBuffer = file.reserve(bytes.length)) {
            if (writeBuffer == null) {
                return false;
            }

            writeBuffer.buffer().put(bytes);

            return true;
        }
    }

    private byte[] readFileContent(int length) throws IOException {
        return readFileContent(0, length);
    }

    private byte[] readFileContent(long offset, int length) throws IOException {
        try (InputStream is = Files.newInputStream(path)) {
            long remainingOffset = offset;

            while (remainingOffset > 0) {
                long skipped = is.skip(remainingOffset);

                if (skipped == 0) {
                    // Check for EOF.
                    assertThat(is.read(), is(not(-1)));

                    remainingOffset--;
                } else {
                    remainingOffset -= skipped;
                }
            }

            return is.readNBytes(length);
        }
    }

    private static List<byte[]> generateData(int numElements, int maxEntrySize) {
        ThreadLocalRandom random = ThreadLocalRandom.current();

        return IntStream.range(0, numElements)
                .mapToObj(i -> {
                    int dataLength = random.nextInt(Integer.BYTES, maxEntrySize);

                    var bytes = new byte[dataLength - Integer.BYTES];

                    random.nextBytes(bytes);

                    return ByteBuffer.allocate(dataLength)
                            .put(intToBytes(bytes.length))
                            .put(bytes)
                            .array();
                })
                .collect(toList());
    }

    /**
     * Reads data saved in format of {@link #generateData} from the current file.
     */
    private List<byte[]> readDataFromFile(int numElements) throws IOException {
        var dataFromFile = new ArrayList<byte[]>(numElements);

        try (InputStream is = Files.newInputStream(path)) {
            for (int i = 0; i < numElements; i++) {
                int bytesLength = bytesToInt(is.readNBytes(Integer.BYTES));

                var bytes = new byte[bytesLength + Integer.BYTES];

                ByteUtils.putIntToBytes(bytesLength, bytes, 0);

                is.readNBytes(bytes, Integer.BYTES, bytesLength);

                dataFromFile.add(bytes);
            }
        }

        return dataFromFile;
    }
}
