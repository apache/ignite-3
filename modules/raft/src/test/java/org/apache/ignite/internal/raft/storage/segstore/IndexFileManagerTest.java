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
    private IndexFileManager indexFileManager;

    @BeforeEach
    void setUp() {
        indexFileManager = new IndexFileManager(workDir);
    }

    @Test
    void testIndexFileNaming() throws IOException {
        var memtable = new IndexMemTable(4);

        Path path0 = indexFileManager.saveIndexMemtable(memtable);
        Path path1 = indexFileManager.saveIndexMemtable(memtable);
        Path path2 = indexFileManager.saveIndexMemtable(memtable);

        assertThat(path0, is(workDir.resolve("index-0000000000-0000000000.bin")));
        assertThat(path1, is(workDir.resolve("index-0000000001-0000000000.bin")));
        assertThat(path2, is(workDir.resolve("index-0000000002-0000000000.bin")));
    }

    @Test
    void testFileContent() throws IOException {
        int numGroups = 10;

        int entriesPerGroup = 5;

        int[] offsets = IntStream.range(0, numGroups * entriesPerGroup)
                .map(i -> ThreadLocalRandom.current().nextInt())
                .toArray();

        var memtable = new IndexMemTable(4);

        for (int groupId = 1; groupId <= numGroups; groupId++) {
            for (int i = 0; i < entriesPerGroup; i++) {
                int offsetIndex = (groupId - 1) * entriesPerGroup + i;

                memtable.appendSegmentFileOffset(groupId, i, offsets[offsetIndex]);
            }
        }

        Path indexFile = indexFileManager.saveIndexMemtable(memtable);

        DeserializedIndexFile deserializedIndexFile = DeserializedIndexFile.fromFile(indexFile);

        for (int groupId = 1; groupId <= numGroups; groupId++) {
            for (int i = 0; i < entriesPerGroup; i++) {
                int offsetIndex = (groupId - 1) * entriesPerGroup + i;

                int expectedOffset = offsets[offsetIndex];

                Integer actualOffset = deserializedIndexFile.getOffset(groupId, i);

                assertThat(actualOffset, is(expectedOffset));
            }
        }
    }

    @Test
    void testSearchIndexMeta() throws IOException {
        int numGroups = 10;

        int entriesPerGroup = 5;

        int numMemtables = 5;

        int[] offsets = IntStream.range(0, numMemtables * entriesPerGroup)
                .map(i -> ThreadLocalRandom.current().nextInt())
                .toArray();

        for (int memtableIndex = 0; memtableIndex < numMemtables; memtableIndex++) {
            var memtable = new IndexMemTable(4);

            for (int groupId = 1; groupId <= numGroups; groupId++) {
                for (int i = 0; i < entriesPerGroup; i++) {
                    int logIndex = memtableIndex * entriesPerGroup + i;

                    memtable.appendSegmentFileOffset(groupId, logIndex, offsets[logIndex]);
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
                    assertThat(pointer.fileIndex(), is(memtableIndex));
                    assertThat(pointer.offset(), is(offsets[logIndex]));
                }
            }
        }
    }

    @Test
    void testMissingIndexMeta() throws IOException {
        assertThat(indexFileManager.getSegmentFilePointer(0, 0), is(nullValue()));

        var memtable = new IndexMemTable(4);

        memtable.appendSegmentFileOffset(0, 0, 1);

        indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFileManager.getSegmentFilePointer(0, 0), is(notNullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(0, 1), is(nullValue()));
        assertThat(indexFileManager.getSegmentFilePointer(1, 0), is(nullValue()));
    }
}
