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

package org.apache.ignite.internal.fileio;

import static java.util.concurrent.CompletableFuture.allOf;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** For POC. */
@ExtendWith(WorkDirectoryExtension.class)
public class FileIoWriteTest {
    private static final IgniteLogger LOG = Loggers.forClass(FileIoWriteTest.class);

    private static final FileIoFactory ASYNC_FILE_IO_FACTORY = new AsyncFileIoFactory();
    private static final FileIoFactory NOT_ASYNC_FILE_IO_FACTORY = new RandomAccessFileIoFactory();

    private static final int PAGE_SIZE = 4096;

    private static final int CPU_COUNT = Runtime.getRuntime().availableProcessors();

    private static final int ROW_COUNT = 50_000;

    @WorkDirectory
    private Path workDir;

    private ExecutorService executor;

    private FileIo[] toClose;

    @BeforeEach
    void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(CPU_COUNT);

        FileIo[] toClose1 = toClose;

        if (toClose1 != null) {
            IgniteUtils.closeAll(toClose1);
        }
    }

    @AfterEach
    void tearDown() {
        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    @BeforeAll
    static void beforeAll() {
        LOG.info("Info: CPU_COUNT={}", CPU_COUNT);
    }

    @ParameterizedTest(name = "useAsyncIo={0}")
    @ValueSource(booleans = {true, false})
    void testWriteInOneThread(boolean async) throws Exception {
        FileIo[] fileIos = createFileIos(CPU_COUNT, async);
        ByteBuffer[] buffers = createByteBuffers(CPU_COUNT);

        CompletableFuture<Void>[] futures = createFutures(CPU_COUNT);

        toClose = fileIos;

        long start = System.nanoTime();

        for (int i = 0; i < CPU_COUNT; i++) {
            FileIo fileIo = fileIos[i];
            ByteBuffer buffer = buffers[i];
            CompletableFuture<Void> future = futures[i];

            executor.submit(() -> {
                try {
                    for (int j = 0; j < ROW_COUNT; j++) {
                        fileIo.writeFully(buffer.rewind(), j * PAGE_SIZE);
                    }

                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }

        allOf(futures).whenComplete((unused, throwable) -> {
            long timeNs = System.nanoTime() - start;

            LOG.info("time: {} ns, {} ms", timeNs, TimeUnit.NANOSECONDS.toMillis(timeNs));
        }).join();
    }

    @ParameterizedTest(name = "useAsyncIo={0}")
    @ValueSource(booleans = {true, false})
    void testWriteWithSeveralThread(boolean async) throws Exception {
        FileIo[] fileIos = createFileIos(CPU_COUNT, async);
        ByteBuffer[] buffers = createByteBuffers(CPU_COUNT);

        CompletableFuture<Void>[] futures = createFutures(CPU_COUNT);

        toClose = fileIos;

        long start = System.nanoTime();

        for (int i = 0; i < CPU_COUNT; i++) {
            ByteBuffer buffer = buffers[i];
            CompletableFuture<Void> future = futures[i];

            int fi = i;

            executor.submit(() -> {
                try {
                    for (FileIo fileIo : fileIos) {
                        for (int j = 0; j < ROW_COUNT / CPU_COUNT; j++) {
                            fileIo.writeFully(buffer.rewind(), (long) fi * (ROW_COUNT / CPU_COUNT) * PAGE_SIZE + j * PAGE_SIZE);
                        }
                    }

                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
        }

        allOf(futures).whenComplete((unused, throwable) -> {
            long timeNs = System.nanoTime() - start;

            LOG.info("time: {} ns, {} ms", timeNs, TimeUnit.NANOSECONDS.toMillis(timeNs));
        }).join();
    }

    private FileIo[] createFileIos(int partitionCount, boolean async) throws Exception {
        var fileIos = new FileIo[partitionCount];

        for (int i = 0; i < fileIos.length; i++) {
            fileIos[i] = createFileIo(i, async);
        }

        return fileIos;
    }

    private static ByteBuffer[] createByteBuffers(int count) {
        var buffers = new ByteBuffer[count];

        for (int i = 0; i < buffers.length; i++) {
            buffers[i] = ByteBuffer.allocate(PAGE_SIZE);
        }

        return buffers;
    }

    private static CompletableFuture<Void>[] createFutures(int count) {
        var futures = new CompletableFuture[count];

        for (int i = 0; i < futures.length; i++) {
            futures[i] = new CompletableFuture<Void>();
        }

        return futures;
    }

    private FileIo createFileIo(int partitionId, boolean async) throws Exception {
        return createFileIo(workDir.resolve(String.format("part-%s.bin", partitionId)), async);
    }

    private static FileIo createFileIo(Path path, boolean async) throws Exception {
        return async ? ASYNC_FILE_IO_FACTORY.create(path) : NOT_ASYNC_FILE_IO_FACTORY.create(path);
    }
}
