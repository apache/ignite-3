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

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.ignite.internal.raft.storage.segstore.RaftLogCheckpointer.MEM_TABLE_QUEUE_SIZE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(ExecutorServiceExtension.class)
class RaftLogCheckpointerTest extends BaseIgniteAbstractTest {
    private static final String NODE_NAME = "test";

    private RaftLogCheckpointer checkpointer;

    @Mock
    private IndexFileManager indexFileManager;

    @BeforeEach
    void setUp() {
        checkpointer = new RaftLogCheckpointer(NODE_NAME, indexFileManager, new NoOpFailureManager());

        checkpointer.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (checkpointer != null) {
            checkpointer.stop();
        }
    }

    @Test
    void testOnRollover(@Mock SegmentFile segmentFile, @Mock IndexMemTable memTable) throws IOException {
        checkpointer.onRollover(segmentFile, memTable);

        verify(segmentFile, timeout(500)).sync();
        verify(indexFileManager, timeout(500)).saveIndexMemtable(memTable);
    }

    @Test
    void testBlockOnRollover(
            @Mock SegmentFile segmentFile,
            @Mock IndexMemTable memTable,
            @InjectExecutorService(threadCount = 1) ExecutorService executor
    ) {
        var blockFuture = new CompletableFuture<Void>();

        doAnswer(invocation -> blockFuture.join()).when(segmentFile).sync();

        for (int i = 0; i < MEM_TABLE_QUEUE_SIZE; i++) {
            checkpointer.onRollover(segmentFile, memTable);
        }

        CompletableFuture<Void> addFuture = runAsync(() -> checkpointer.onRollover(segmentFile, memTable), executor);

        assertThat(addFuture, willTimeoutFast());

        blockFuture.complete(null);

        assertThat(addFuture, willCompleteSuccessfully());
    }
}
