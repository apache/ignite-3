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

import static java.util.Collections.nCopies;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.raft.storage.segstore.SegmentFile.WriteBuffer;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class SyncSegmentFileTest extends IgniteAbstractTest {
    private static final String FILE_NAME = SegmentFile.fileName(new FileProperties(0));

    private static final int size = 1024;

    private SegmentFile file;

    @BeforeEach
    void setUp() throws IOException {
        Path path = workDir.resolve(FILE_NAME);

        file = SegmentFile.createNew(path, size, true);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(file);
    }

    @Test
    void testLastWrittenIncreaseOrder() {
        assertThat(file.lastWritePosition(), is(0));
        assertThat(file.syncPosition(), is(0));

        try (WriteBuffer ignored = file.reserve(10)) {
            assertThat(file.lastWritePosition(), is(0));
            assertThat(file.syncPosition(), is(0));
        }

        assertThat(file.lastWritePosition(), is(10));
        assertThat(file.syncPosition(), is(10));
    }

    @Test
    void testLastWrittenMultithreadedIncreaseOrder() {
        var task1ReservedBuffer = new CountDownLatch(1);
        var task1ReleasedBuffer = new CountDownLatch(1);
        var task2ReservedBuffer = new CountDownLatch(1);

        RunnableX task1 = () -> {
            try (WriteBuffer ignored = file.reserve(10)) {
                task1ReservedBuffer.countDown();
                task2ReservedBuffer.await(1, TimeUnit.SECONDS);
            }

            assertThat(file.lastWritePosition(), is(10));
            assertThat(file.syncPosition(), is(10));

            task1ReleasedBuffer.countDown();
        };

        RunnableX task2 = () -> {
            task1ReservedBuffer.await(1, TimeUnit.SECONDS);

            try (WriteBuffer ignored = file.reserve(10)) {
                task2ReservedBuffer.countDown();
                task1ReleasedBuffer.await(1, TimeUnit.SECONDS);
            }

            assertThat(file.lastWritePosition(), is(20));
            assertThat(file.syncPosition(), is(20));
        };

        runRace(task1, task2);
    }

    @RepeatedTest(10)
    void testSyncMultithreadedTest() {
        int chunkSize = 32;

        int numThreads = 4;

        int numChunksPerThread = size / (chunkSize * numThreads);

        RunnableX writer = () -> {
            for (int i = 0; i < numChunksPerThread; i++) {
                try (WriteBuffer ignored = file.reserve(chunkSize)) {
                    // No-op.
                }
            }
        };

        runRace(nCopies(numThreads, writer).toArray(RunnableX[]::new));

        assertThat(file.lastWritePosition(), is(size));
        assertThat(file.syncPosition(), is(size));
    }
}
