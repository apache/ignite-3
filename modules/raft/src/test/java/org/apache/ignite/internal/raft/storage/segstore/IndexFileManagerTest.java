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

import java.io.IOException;
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

        IndexFile indexFile0 = indexFileManager.saveIndexMemtable(memtable);
        IndexFile indexFile1 = indexFileManager.saveIndexMemtable(memtable);
        IndexFile indexFile2 = indexFileManager.saveIndexMemtable(memtable);

        assertThat(indexFile0.name(), is("index-0000000000-0000000000.bin"));
        assertThat(indexFile0.path(), is(workDir.resolve("index-0000000000-0000000000.bin.tmp")));

        assertThat(indexFile1.name(), is("index-0000000001-0000000000.bin"));
        assertThat(indexFile1.path(), is(workDir.resolve("index-0000000001-0000000000.bin.tmp")));

        assertThat(indexFile2.name(), is("index-0000000002-0000000000.bin"));
        assertThat(indexFile2.path(), is(workDir.resolve("index-0000000002-0000000000.bin.tmp")));

        indexFile0.syncAndMove();

        assertThat(indexFile0.name(), is("index-0000000000-0000000000.bin"));
        assertThat(indexFile0.path(), is(workDir.resolve("index-0000000000-0000000000.bin")));

        indexFile1.syncAndMove();

        assertThat(indexFile1.name(), is("index-0000000001-0000000000.bin"));
        assertThat(indexFile1.path(), is(workDir.resolve("index-0000000001-0000000000.bin")));

        indexFile2.syncAndMove();

        assertThat(indexFile2.name(), is("index-0000000002-0000000000.bin"));
        assertThat(indexFile2.path(), is(workDir.resolve("index-0000000002-0000000000.bin")));
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

        IndexFile indexFile = indexFileManager.saveIndexMemtable(memtable);

        DeserializedIndexFile deserializedIndexFile = DeserializedIndexFile.fromFile(indexFile.path());

        for (int groupId = 1; groupId <= numGroups; groupId++) {
            for (int i = 0; i < entriesPerGroup; i++) {
                int offsetIndex = (groupId - 1) * entriesPerGroup + i;

                int expectedOffset = offsets[offsetIndex];

                Integer actualOffset = deserializedIndexFile.getOffset(groupId, i);

                assertThat(actualOffset, is(expectedOffset));
            }
        }
    }
}
