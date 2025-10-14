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
import static org.apache.ignite.internal.raft.storage.segstore.RaftLogCheckpointer.MAX_QUEUE_SIZE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
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

        try {
            doAnswer(invocation -> blockFuture.join()).when(segmentFile).sync();

            for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
                checkpointer.onRollover(segmentFile, memTable);
            }

            CompletableFuture<Void> addFuture = runAsync(() -> checkpointer.onRollover(segmentFile, memTable), executor);

            assertThat(addFuture, willTimeoutFast());

            blockFuture.complete(null);

            assertThat(addFuture, willCompleteSuccessfully());
        } finally {
            blockFuture.complete(null);
        }
    }

    @Test
    void testReadFromQueue() {
        // Read from empty queue.
        assertThat(checkpointer.findSegmentPayloadInQueue(0, 0), is(nullValue()));

        var blockFuture = new CompletableFuture<Void>();

        try {
            var buffer = ByteBuffer.allocate(1);

            for (int i = 0; i < MAX_QUEUE_SIZE; i++) {
                SegmentFile mockFile = mock(SegmentFile.class);

                doAnswer(invocation -> blockFuture.join()).when(mockFile).sync();

                when(mockFile.buffer()).thenReturn(buffer);

                IndexMemTable mockMemTable = mock(IndexMemTable.class);

                lenient().when(mockMemTable.getSegmentFileOffset(i, i)).thenReturn(1);

                checkpointer.onRollover(mockFile, mockMemTable);
            }

            for (int groupId = 0; groupId < MAX_QUEUE_SIZE; groupId++) {
                for (int logIndex = 0; logIndex < MAX_QUEUE_SIZE; logIndex++) {
                    ByteBuffer payload = checkpointer.findSegmentPayloadInQueue(groupId, logIndex);

                    if (groupId == logIndex) {
                        assertThat(payload, is(notNullValue()));
                    } else {
                        assertThat(payload, is(nullValue()));
                    }
                }
            }

            assertThat(checkpointer.findSegmentPayloadInQueue(MAX_QUEUE_SIZE, MAX_QUEUE_SIZE), is(nullValue()));
        } finally {
            blockFuture.complete(null);
        }

        // The queue should eventually become empty again.
        await().until(() -> checkpointer.findSegmentPayloadInQueue(0, 0), is(nullValue()));
    }
}
