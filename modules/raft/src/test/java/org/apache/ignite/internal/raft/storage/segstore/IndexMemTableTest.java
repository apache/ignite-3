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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;

import java.util.ArrayList;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

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

    @RepeatedTest(10)
    void testOneWriterMultipleReaders() {
        int numItems = 1000;

        // One thread writes and two threads read from the same group ID.
        RunnableX writer = () -> {
            for (int i = 0; i < numItems; i++) {
                memTable.appendSegmentFileOffset(0, i, i + 1);
            }
        };

        RunnableX reader = () -> {
            for (int i = 0; i < numItems; i++) {
                int offset = memTable.getSegmentFileOffset(0, i);

                assertThat(offset, either(is(i + 1)).or(is(0)));
            }
        };

        runRace(writer, reader, reader);
    }

    @RepeatedTest(10)
    void testMultithreadedPutGet() {
        int itemsPerGroup = 1000;

        var actions = new ArrayList<RunnableX>(STRIPES * 2);

        for (int i = 0; i < STRIPES; i++) {
            long groupId = i;

            actions.add(() -> {
                for (int j = 0; j < itemsPerGroup; j++) {
                    memTable.appendSegmentFileOffset(groupId, j, j + 1);
                }
            });
        }

        for (int i = 0; i < STRIPES; i++) {
            long groupId = i;

            actions.add(() -> {
                for (int j = 0; j < itemsPerGroup; j++) {
                    int offset = memTable.getSegmentFileOffset(groupId, j);

                    assertThat(offset, either(is(j + 1)).or(is(0)));
                }
            });
        }

        runRace(actions.toArray(RunnableX[]::new));

        // Check that all values are present after all writes completed.
        for (int groupId = 0; groupId < STRIPES; groupId++) {
            for (int j = 0; j < itemsPerGroup; j++) {
                assertThat(memTable.getSegmentFileOffset(groupId, j), is(j + 1));
            }
        }
    }
}
