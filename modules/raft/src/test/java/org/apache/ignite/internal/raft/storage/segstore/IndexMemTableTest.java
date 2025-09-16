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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ExecutorServiceExtension.class)
class IndexMemTableTest extends BaseIgniteAbstractTest {
    private static final int STRIPES = 10;

    private final IndexMemTable memTable = new IndexMemTable(STRIPES);

    @Test
    void testPutGet() {
        memTable.appendSegmentFileOffset(0, 0, 1);
        memTable.appendSegmentFileOffset(0, 1, 2);
        memTable.appendSegmentFileOffset(1, 0, 3);
        memTable.appendSegmentFileOffset(1, 1, 4);

        assertThat(memTable.getSegmentFileOffset(0, 0), is(1));
        assertThat(memTable.getSegmentFileOffset(0, 1), is(2));
        assertThat(memTable.getSegmentFileOffset(1, 0), is(3));
        assertThat(memTable.getSegmentFileOffset(1, 1), is(4));
    }

    @Test
    void testMissingValue() {
        memTable.appendSegmentFileOffset(0, 5, 1);

        assertThat(memTable.getSegmentFileOffset(0, 1), is(0));
        assertThat(memTable.getSegmentFileOffset(0, 5), is(1));
        assertThat(memTable.getSegmentFileOffset(0, 6), is(0));
    }

    @Test
    void testMultithreadedPutGet(@InjectExecutorService(threadCount = STRIPES * 2) ExecutorService executor) {
        int itemsPerGroup = 1000;

        var writeTasks = new CompletableFuture<?>[STRIPES];

        for (int i = 0; i < writeTasks.length; i++) {
            long groupId = i;

            writeTasks[i] = runAsync(() -> {
                for (int j = 0; j < itemsPerGroup; j++) {
                    memTable.appendSegmentFileOffset(groupId, j, j + 1);
                }
            }, executor);
        }

        var readTasks = new CompletableFuture<?>[STRIPES];

        for (int i = 0; i < readTasks.length; i++) {
            long groupId = i;

            readTasks[i] = runAsync(() -> {
                for (int j = 0; j < itemsPerGroup; j++) {
                    int offset = memTable.getSegmentFileOffset(groupId, j);

                    assertThat(offset, either(is(j + 1)).or(is(0)));
                }
            }, executor);
        }

        assertThat(allOf(writeTasks), willCompleteSuccessfully());
        assertThat(allOf(readTasks), willCompleteSuccessfully());

        for (int groupId = 0; groupId < STRIPES; groupId++) {
            for (int j = 0; j < itemsPerGroup; j++) {
                assertThat(memTable.getSegmentFileOffset(groupId, j), is(j + 1));
            }
        }
    }
}
