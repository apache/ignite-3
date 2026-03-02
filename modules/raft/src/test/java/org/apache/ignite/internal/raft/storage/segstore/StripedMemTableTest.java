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

import static org.apache.ignite.internal.raft.storage.segstore.SegmentInfo.MISSING_SEGMENT_FILE_OFFSET;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import org.apache.ignite.internal.lang.RunnableX;
import org.junit.jupiter.api.RepeatedTest;

class StripedMemTableTest extends AbstractMemTableTest<StripedMemTable> {
    private static final int STRIPES = 10;

    @Override
    StripedMemTable memTable() {
        return new StripedMemTable(STRIPES);
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
                SegmentInfo segmentInfo = memTable.segmentInfo(0);

                if (segmentInfo != null) {
                    assertThat(segmentInfo.getOffset(i), either(is(i + 1)).or(is(MISSING_SEGMENT_FILE_OFFSET)));
                }
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
                    SegmentInfo segmentInfo = memTable.segmentInfo(groupId);

                    if (segmentInfo != null) {
                        assertThat(segmentInfo.getOffset(j), either(is(j + 1)).or(is(MISSING_SEGMENT_FILE_OFFSET)));
                    }
                }
            });
        }

        runRace(actions.toArray(RunnableX[]::new));

        // Check that all values are present after all writes completed.
        assertThat(memTable.numGroups(), is(STRIPES));

        for (int groupId = 0; groupId < STRIPES; groupId++) {
            SegmentInfo segmentInfo = memTable.segmentInfo(groupId);

            assertThat(segmentInfo, is(notNullValue()));

            for (int j = 0; j < itemsPerGroup; j++) {
                assertThat(segmentInfo.getOffset(j), is(j + 1));
            }
        }
    }
}
