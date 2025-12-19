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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IndexFileManagerTest extends IgniteAbstractTest {
    private static final int STRIPES = 4;

    private IndexFileManager indexFileManager;

    @BeforeEach
    void setUp() throws IOException {
        indexFileManager = new IndexFileManager(workDir);

        indexFileManager.start();
    }

    @Test
    void testIndexFileNaming() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        Path path0 = indexFileManager.saveIndexMemtable(memtable);
        Path path1 = indexFileManager.saveIndexMemtable(memtable);
        Path path2 = indexFileManager.saveIndexMemtable(memtable);

        assertThat(path0, is(indexFileManager.indexFilesDir().resolve("index-0000000000-0000000000.bin")));
        assertThat(path1, is(indexFileManager.indexFilesDir().resolve("index-0000000001-0000000000.bin")));
        assertThat(path2, is(indexFileManager.indexFilesDir().resolve("index-0000000002-0000000000.bin")));
    }

    @Test
    void testFileContent() throws IOException {
        int numGroups = 10;

        int entriesPerGroup = 5;

        int[] segmentFileOffsets = IntStream.range(0, numGroups * entriesPerGroup)
                .map(i -> ThreadLocalRandom.current().nextInt())
                .toArray();

        var memtable = new IndexMemTable(STRIPES);

        for (int groupId = 1; groupId <= numGroups; groupId++) {
            for (int i = 0; i < entriesPerGroup; i++) {
                int offsetIndex = (groupId - 1) * entriesPerGroup + i;

                memtable.appendSegmentFileOffset(groupId, i, segmentFileOffsets[offsetIndex]);
            }
        }

        Path indexFile = indexFileManager.saveIndexMemtable(memtable);

        DeserializedIndexFile deserializedIndexFile = DeserializedIndexFile.fromFile(indexFile);

        for (int groupId = 1; groupId <= numGroups; groupId++) {
            for (int i = 0; i < entriesPerGroup; i++) {
                int offsetIndex = (groupId - 1) * entriesPerGroup + i;

                int expectedOffset = segmentFileOffsets[offsetIndex];

                Integer actualOffset = deserializedIndexFile.getSegmentFileOffset(groupId, i);

                assertThat(actualOffset, is(expectedOffset));
            }
        }
    }

    @Test
    void testSearchIndexMeta() throws IOException {
        int numGroups = 10;

        int entriesPerGroup = 5;

        int numMemtables = 5;

        int[] segmentFileOffsets = IntStream.range(0, numMemtables * entriesPerGroup)
                .map(i -> ThreadLocalRandom.current().nextInt())
                .toArray();

        for (int memtableIndex = 0; memtableIndex < numMemtables; memtableIndex++) {
            var memtable = new IndexMemTable(STRIPES);

            for (int groupId = 1; groupId <= numGroups; groupId++) {
                for (int i = 0; i < entriesPerGroup; i++) {
                    int logIndex = memtableIndex * entriesPerGroup + i;

                    memtable.appendSegmentFileOffset(groupId, logIndex, segmentFileOffsets[logIndex]);
                }
            }

            indexFileManager.saveIndexMemtable(memtable);
        }

        for (int memtableIndex = 0; memtableIndex < numMemtables; memtableIndex++) {
            for (int groupId = 1; groupId <= numGroups; groupId++) {
                for (int i = 0; i < entriesPerGroup; i++) {
                    int logIndex = memtableIndex * entriesPerGroup + i;

                    SegmentFilePointer pointer = indexFileManager.getSegmentFilePointer(groupId, logIndex);

                    assertThat(pointer, is(notNullValue()));
                    assertThat(pointer.fileOrdinal(), is(memtableIndex));
                    assertThat(pointer.payloadOffset(), is(segmentFileOffsets[logIndex]));
                }
            }
        }
    }

    @Test
    void testMissingIndexMeta() throws IOException {
        assertThat(indexFileManager.getSegmentFilePointer(0, 0), is(nullValue()));

        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 0, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 0), is(notNullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(1, 0), is(nullValue()));
    }

    /**
     * Tests a scenario when a group ID is missing in one of the intermediate index files.
     */
    @Test
    void getSegmentFilePointerWithGroupGaps() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 0, 1);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(1, 0, 2);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 3);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(
                indexFileManager.getSegmentFilePointer(0, 0),
                is(new SegmentFilePointer(0, 1))
        );

        assertThat(
                indexFileManager.getSegmentFilePointer(1, 0),
                is(new SegmentFilePointer(1, 2))
        );

        assertThat(
                indexFileManager.getSegmentFilePointer(0, 1),
                is(new SegmentFilePointer(2, 3))
        );
    }

    @Test
    void testFirstLastLogIndicesIndependence() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.firstLogIndexInclusive(0), is(1L));
        assertThat(indexFileManager.lastLogIndexExclusive(0), is(2L));

        assertThat(indexFileManager.firstLogIndexInclusive(1), is(-1L));
        assertThat(indexFileManager.lastLogIndexExclusive(1), is(-1L));

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(1, 2, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.firstLogIndexInclusive(0), is(1L));
        assertThat(indexFileManager.lastLogIndexExclusive(0), is(2L));

        assertThat(indexFileManager.firstLogIndexInclusive(1), is(2L));
        assertThat(indexFileManager.lastLogIndexExclusive(1), is(3L));
    }

    @Test
    void testFirstLastLogIndicesWithTruncateSuffix() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 1);
        memtable.appendSegmentFileOffset(0, 3, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.firstLogIndexInclusive(0), is(1L));
        assertThat(indexFileManager.lastLogIndexExclusive(0), is(4L));

        memtable = new IndexMemTable(STRIPES);

        memtable.truncateSuffix(0, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.firstLogIndexInclusive(0), is(1L));
        assertThat(indexFileManager.lastLogIndexExclusive(0), is(2L));
    }

    @Test
    void testFirstLastLogIndicesWithTruncatePrefix() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 1);
        memtable.appendSegmentFileOffset(0, 3, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.firstLogIndexInclusive(0), is(1L));
        assertThat(indexFileManager.lastLogIndexExclusive(0), is(4L));

        memtable = new IndexMemTable(STRIPES);

        memtable.truncatePrefix(0, 2);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.firstLogIndexInclusive(0), is(2L));
        assertThat(indexFileManager.lastLogIndexExclusive(0), is(4L));
    }

    @Test
    void testGetSegmentPointerWithTruncate() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 2);
        memtable.appendSegmentFileOffset(0, 3, 3);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(0, 2)));

        memtable = new IndexMemTable(STRIPES);

        memtable.truncateSuffix(0, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(new SegmentFilePointer(0, 1)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(nullValue()));

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 2, 2);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(2, 2)));
    }

    @Test
    void testRecovery() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 2, 2);

        indexFileManager.saveIndexMemtable(memtable);

        indexFileManager = new IndexFileManager(workDir);

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(nullValue()));

        indexFileManager.start();

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(new SegmentFilePointer(0, 1)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(1, 2)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(nullValue()));

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 3, 3);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(new SegmentFilePointer(0, 1)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(1, 2)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(new SegmentFilePointer(2, 3)));
    }

    @Test
    void testRecoveryWithTruncateSuffix() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 2);
        memtable.appendSegmentFileOffset(0, 3, 3);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.truncateSuffix(0, 2);

        indexFileManager.saveIndexMemtable(memtable);

        indexFileManager = new IndexFileManager(workDir);

        indexFileManager.start();

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(new SegmentFilePointer(0, 1)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(0, 2)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(nullValue()));
    }

    @Test
    void testRecoveryWithTruncatePrefix() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 2);
        memtable.appendSegmentFileOffset(0, 3, 3);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.truncatePrefix(0, 2);

        indexFileManager.saveIndexMemtable(memtable);

        indexFileManager = new IndexFileManager(workDir);

        indexFileManager.start();

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(0, 2)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(new SegmentFilePointer(0, 3)));
    }

    @Test
    void testExists() throws IOException {
        assertThat(indexFileManager.indexFileExists(0), is(false));
        assertThat(indexFileManager.indexFileExists(1), is(false));

        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.indexFileExists(0), is(true));
        assertThat(indexFileManager.indexFileExists(1), is(false));

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.indexFileExists(0), is(true));
        assertThat(indexFileManager.indexFileExists(1), is(true));
    }

    @Test
    void testSaveMemtableWithExplicitOrdinal() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);

        indexFileManager.recoverIndexFile(memtable, 5);

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 2, 2);

        indexFileManager.recoverIndexFile(memtable, 6);

        // Restart the manager to update in-memory meta.
        indexFileManager = new IndexFileManager(workDir);

        indexFileManager.start();

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(new SegmentFilePointer(5, 1)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(6, 2)));
    }

    @Test
    void testTruncatePrefix() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 2, 1);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 3, 1);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.truncatePrefix(0, 2);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(1, 1)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(new SegmentFilePointer(2, 1)));
    }

    @Test
    void testCombinationOfPrefixAndSuffixTombstones() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 2);
        memtable.appendSegmentFileOffset(0, 3, 3);
        memtable.appendSegmentFileOffset(0, 4, 4);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.truncatePrefix(0, 2);

        memtable.truncateSuffix(0, 3);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(0, 2)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(new SegmentFilePointer(0, 3)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 4), is(nullValue()));

        // Restart the manager to check recovery.
        indexFileManager = new IndexFileManager(workDir);

        indexFileManager.start();

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(0, 2)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(new SegmentFilePointer(0, 3)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 4), is(nullValue()));
    }

    @Test
    void testReset() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 2);
        memtable.appendSegmentFileOffset(0, 3, 3);
        memtable.appendSegmentFileOffset(0, 4, 4);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.reset(0, 2);

        memtable.appendSegmentFileOffset(0, 3, 5);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(0, 2)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(new SegmentFilePointer(1, 5)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 4), is(nullValue()));
    }

    @Test
    void testResetTombstone() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 2);
        memtable.appendSegmentFileOffset(0, 3, 3);
        memtable.appendSegmentFileOffset(0, 4, 4);

        indexFileManager.saveIndexMemtable(memtable);

        memtable = new IndexMemTable(STRIPES);

        memtable.reset(0, 2);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 2), is(new SegmentFilePointer(0, 2)));
        assertThat(indexFileManager.getSegmentFilePointer(0, 3), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 4), is(nullValue()));
    }

    @Test
    void testFirstLastLogIndicesWithReset() throws IOException {
        var memtable = new IndexMemTable(STRIPES);

        memtable.appendSegmentFileOffset(0, 1, 1);
        memtable.appendSegmentFileOffset(0, 2, 1);
        memtable.appendSegmentFileOffset(0, 3, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.firstLogIndexInclusive(0), is(1L));
        assertThat(indexFileManager.lastLogIndexExclusive(0), is(4L));

        memtable = new IndexMemTable(STRIPES);

        memtable.reset(0, 2);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.firstLogIndexInclusive(0), is(2L));
        assertThat(indexFileManager.lastLogIndexExclusive(0), is(3L));
    }
}
