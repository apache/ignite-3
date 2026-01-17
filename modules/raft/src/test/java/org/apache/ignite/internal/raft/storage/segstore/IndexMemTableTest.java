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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
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

        SegmentInfo segmentInfo0 = memTable.segmentInfo(0);

        assertThat(segmentInfo0, is(notNullValue()));
        assertThat(segmentInfo0.getOffset(0), is(1));
        assertThat(segmentInfo0.getOffset(1), is(2));

        SegmentInfo segmentInfo1 = memTable.segmentInfo(1);

        assertThat(segmentInfo1, is(notNullValue()));
        assertThat(segmentInfo1.getOffset(0), is(3));
        assertThat(segmentInfo1.getOffset(1), is(4));

        assertThat(memTable.numGroups(), is(2));
    }

    @Test
    void testMissingValue() {
        memTable.appendSegmentFileOffset(0, 5, 1);

        SegmentInfo segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(1), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo.getOffset(5), is(1));
        assertThat(segmentInfo.getOffset(6), is(MISSING_SEGMENT_FILE_OFFSET));

        assertThat(memTable.segmentInfo(1), is(nullValue()));
    }

    @Test
    void testIterator() {
        memTable.appendSegmentFileOffset(0, 0, 1);
        memTable.appendSegmentFileOffset(0, 1, 2);
        memTable.appendSegmentFileOffset(1, 0, 3);
        memTable.appendSegmentFileOffset(1, 1, 4);

        Iterator<Entry<Long, SegmentInfo>> it = memTable.iterator();

        it.forEachRemaining(entry -> {
            long groupId = entry.getKey();
            SegmentInfo segmentInfo = entry.getValue();

            assertThat(groupId, either(is(0L)).or(is(1L)));

            if (groupId == 0) {
                assertThat(segmentInfo.getOffset(0), is(1));
                assertThat(segmentInfo.getOffset(1), is(2));
                assertThat(segmentInfo.getOffset(2), is(MISSING_SEGMENT_FILE_OFFSET));
            } else {
                assertThat(segmentInfo.getOffset(0), is(3));
                assertThat(segmentInfo.getOffset(1), is(4));
                assertThat(segmentInfo.getOffset(2), is(MISSING_SEGMENT_FILE_OFFSET));
            }
        });
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

    @Test
    void testTruncateSuffix() {
        long groupId0 = 1;
        long groupId1 = 2;

        memTable.appendSegmentFileOffset(groupId0, 1, 42);
        memTable.appendSegmentFileOffset(groupId0, 2, 43);
        memTable.appendSegmentFileOffset(groupId0, 3, 44);
        memTable.appendSegmentFileOffset(groupId0, 4, 45);

        memTable.appendSegmentFileOffset(groupId1, 1, 55);
        memTable.appendSegmentFileOffset(groupId1, 2, 56);
        memTable.appendSegmentFileOffset(groupId1, 3, 57);
        memTable.appendSegmentFileOffset(groupId1, 4, 58);

        memTable.truncateSuffix(groupId0, 2);

        SegmentInfo segmentInfo0 = memTable.segmentInfo(groupId0);

        assertThat(segmentInfo0, is(notNullValue()));
        assertThat(segmentInfo0.getOffset(1), is(42));
        assertThat(segmentInfo0.getOffset(2), is(43));
        assertThat(segmentInfo0.getOffset(3), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo0.getOffset(4), is(MISSING_SEGMENT_FILE_OFFSET));

        SegmentInfo segmentInfo1 = memTable.segmentInfo(groupId1);

        assertThat(segmentInfo1, is(notNullValue()));
        assertThat(segmentInfo1.getOffset(1), is(55));
        assertThat(segmentInfo1.getOffset(2), is(56));
        assertThat(segmentInfo1.getOffset(3), is(57));
        assertThat(segmentInfo1.getOffset(4), is(58));

        memTable.truncateSuffix(groupId1, 4);

        segmentInfo0 = memTable.segmentInfo(groupId0);

        assertThat(segmentInfo0, is(notNullValue()));
        assertThat(segmentInfo0.getOffset(1), is(42));
        assertThat(segmentInfo0.getOffset(2), is(43));
        assertThat(segmentInfo0.getOffset(3), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo0.getOffset(4), is(MISSING_SEGMENT_FILE_OFFSET));

        segmentInfo1 = memTable.segmentInfo(groupId1);

        assertThat(segmentInfo1, is(notNullValue()));
        assertThat(segmentInfo1.getOffset(1), is(55));
        assertThat(segmentInfo1.getOffset(2), is(56));
        assertThat(segmentInfo1.getOffset(3), is(57));
        assertThat(segmentInfo1.getOffset(4), is(58));

        memTable.truncateSuffix(groupId1, 0);

        segmentInfo0 = memTable.segmentInfo(groupId0);

        assertThat(segmentInfo0, is(notNullValue()));
        assertThat(segmentInfo0.getOffset(1), is(42));
        assertThat(segmentInfo0.getOffset(2), is(43));
        assertThat(segmentInfo0.getOffset(3), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo0.getOffset(4), is(MISSING_SEGMENT_FILE_OFFSET));

        segmentInfo1 = memTable.segmentInfo(groupId1);

        assertThat(segmentInfo1, is(notNullValue()));
        assertThat(segmentInfo1.getOffset(1), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo1.getOffset(2), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo1.getOffset(3), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo1.getOffset(4), is(MISSING_SEGMENT_FILE_OFFSET));
    }

    @Test
    void testTruncateNonExistingSuffix() {
        assertDoesNotThrow(() -> memTable.truncateSuffix(0, 4));

        memTable.appendSegmentFileOffset(1, 5, 42);

        assertThrows(IllegalArgumentException.class, () -> memTable.truncateSuffix(1, 10));
    }

    @Test
    void testAppendAfterTruncateSuffix() {
        memTable.appendSegmentFileOffset(0, 1, 42);

        SegmentInfo segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(1), is(42));

        memTable.truncateSuffix(0, 0);

        segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(1), is(MISSING_SEGMENT_FILE_OFFSET));

        memTable.appendSegmentFileOffset(0, 1, 43);

        segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(1), is(43));
    }

    @Test
    void testTruncateSuffixIntoThePast() {
        memTable.appendSegmentFileOffset(0, 36, 42);

        // Truncate to a position before the moment the last segment info was added.
        memTable.truncateSuffix(0, 10);

        SegmentInfo segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(36), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo.getOffset(11), is(MISSING_SEGMENT_FILE_OFFSET));

        memTable.appendSegmentFileOffset(0, 11, 43);

        segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(11), is(43));
        assertThat(segmentInfo.getOffset(12), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo.getOffset(36), is(MISSING_SEGMENT_FILE_OFFSET));
    }

    @Test
    void testTruncatePrefix() {
        long groupId1 = 1;
        long groupId2 = 2;

        memTable.appendSegmentFileOffset(groupId1, 1, 42);
        memTable.appendSegmentFileOffset(groupId1, 2, 43);
        memTable.appendSegmentFileOffset(groupId1, 3, 44);
        memTable.appendSegmentFileOffset(groupId1, 4, 45);

        memTable.appendSegmentFileOffset(groupId2, 1, 55);
        memTable.appendSegmentFileOffset(groupId2, 2, 56);
        memTable.appendSegmentFileOffset(groupId2, 3, 57);
        memTable.appendSegmentFileOffset(groupId2, 4, 58);

        memTable.truncatePrefix(groupId1, 1);

        SegmentInfo segmentInfo1 = memTable.segmentInfo(groupId1);

        assertThat(segmentInfo1, is(notNullValue()));
        assertThat(segmentInfo1.getOffset(1), is(42));
        assertThat(segmentInfo1.getOffset(2), is(43));
        assertThat(segmentInfo1.getOffset(3), is(44));
        assertThat(segmentInfo1.getOffset(4), is(45));

        SegmentInfo segmentInfo2 = memTable.segmentInfo(groupId2);

        assertThat(segmentInfo2, is(notNullValue()));
        assertThat(segmentInfo2.getOffset(1), is(55));
        assertThat(segmentInfo2.getOffset(2), is(56));
        assertThat(segmentInfo2.getOffset(3), is(57));
        assertThat(segmentInfo2.getOffset(4), is(58));

        memTable.truncatePrefix(groupId2, 3);

        segmentInfo1 = memTable.segmentInfo(groupId1);

        assertThat(segmentInfo1, is(notNullValue()));
        assertThat(segmentInfo1.getOffset(1), is(42));
        assertThat(segmentInfo1.getOffset(2), is(43));
        assertThat(segmentInfo1.getOffset(3), is(44));
        assertThat(segmentInfo1.getOffset(4), is(45));

        segmentInfo2 = memTable.segmentInfo(groupId2);

        assertThat(segmentInfo2, is(notNullValue()));
        assertThat(segmentInfo2.getOffset(1), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo2.getOffset(2), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo2.getOffset(3), is(57));
        assertThat(segmentInfo2.getOffset(4), is(58));

        memTable.truncatePrefix(groupId1, 4);

        segmentInfo1 = memTable.segmentInfo(groupId1);

        assertThat(segmentInfo1, is(notNullValue()));
        assertThat(segmentInfo1.getOffset(1), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo1.getOffset(2), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo1.getOffset(3), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo1.getOffset(4), is(45));

        segmentInfo2 = memTable.segmentInfo(groupId2);

        assertThat(segmentInfo2, is(notNullValue()));
        assertThat(segmentInfo2.getOffset(1), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo2.getOffset(2), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo2.getOffset(3), is(57));
        assertThat(segmentInfo2.getOffset(4), is(58));
    }

    @Test
    void testTruncateNonExistingPrefix() {
        assertDoesNotThrow(() -> memTable.truncatePrefix(0, 4));

        memTable.appendSegmentFileOffset(1, 5, 42);

        assertDoesNotThrow(() -> memTable.truncatePrefix(1, 4));
        assertThrows(IllegalArgumentException.class, () -> memTable.truncatePrefix(1, 10));
    }

    @Test
    void testPrefixAndSuffixTombstones() {
        memTable.truncatePrefix(0, 10);

        memTable.truncateSuffix(0, 15);

        memTable.appendSegmentFileOffset(0, 16, 42);

        SegmentInfo segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(16), is(42));
    }

    @Test
    void testSuffixAndPrefixTombstones() {
        memTable.truncateSuffix(0, 15);

        memTable.truncatePrefix(0, 10);

        memTable.appendSegmentFileOffset(0, 16, 42);

        SegmentInfo segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(16), is(42));
    }

    @Test
    void testMultiplePrefixTombstones() {
        memTable.truncatePrefix(0, 10);

        memTable.truncatePrefix(0, 15);

        SegmentInfo segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.isPrefixTombstone(), is(true));
        assertThat(segmentInfo.firstIndexKept(), is(15L));
    }

    @Test
    void testReset() {
        memTable.appendSegmentFileOffset(0, 1, 42);
        memTable.appendSegmentFileOffset(0, 2, 43);
        memTable.appendSegmentFileOffset(0, 3, 44);
        memTable.appendSegmentFileOffset(0, 4, 45);

        memTable.reset(0, 2);

        SegmentInfo segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(1), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo.getOffset(2), is(43));
        assertThat(segmentInfo.getOffset(3), is(MISSING_SEGMENT_FILE_OFFSET));
        assertThat(segmentInfo.getOffset(4), is(MISSING_SEGMENT_FILE_OFFSET));
    }

    @Test
    void testResetTombstone() {
        memTable.reset(0, 10);

        SegmentInfo segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(10), is(MISSING_SEGMENT_FILE_OFFSET));

        memTable.appendSegmentFileOffset(0, 11, 42);

        segmentInfo = memTable.segmentInfo(0);

        assertThat(segmentInfo, is(notNullValue()));
        assertThat(segmentInfo.getOffset(11), is(42));
    }
}
