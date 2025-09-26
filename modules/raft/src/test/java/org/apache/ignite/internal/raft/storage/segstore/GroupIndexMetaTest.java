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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class GroupIndexMetaTest extends BaseIgniteAbstractTest {
    @Test
    void testAddGet() {
        var initialMeta = new IndexFileMeta(1, 50, 0);

        var groupMeta = new GroupIndexMeta(0, initialMeta);

        var additionalMeta = new IndexFileMeta(50, 100, 42);

        groupMeta.addIndexMeta(additionalMeta);

        assertThat(groupMeta.indexFilePointer(0), is(nullValue()));

        IndexFilePointer pointer = groupMeta.indexFilePointer(1);

        assertThat(pointer, is(notNullValue()));
        assertThat(pointer.fileIndex(), is(0));
        assertThat(pointer.fileMeta(), is(initialMeta));

        pointer = groupMeta.indexFilePointer(66);

        assertThat(pointer, is(notNullValue()));
        assertThat(pointer.fileIndex(), is(1));
        assertThat(pointer.fileMeta(), is(additionalMeta));

        pointer = groupMeta.indexFilePointer(100);

        assertThat(pointer, is(nullValue()));
    }

    @RepeatedTest(10)
    void testOneWriterMultipleReaders() {
        int startFileIndex = 100;

        int logsPerFile = 50;

        var initialMeta = new IndexFileMeta(0, logsPerFile, 0);

        var groupMeta = new GroupIndexMeta(startFileIndex, initialMeta);

        int numItems = 1000;

        RunnableX writer = () -> {
            for (int fileIndex = 1; fileIndex < numItems; fileIndex++) {
                long startIndex = fileIndex * logsPerFile;

                groupMeta.addIndexMeta(new IndexFileMeta(startIndex, startIndex + logsPerFile, fileIndex));
            }
        };

        RunnableX reader = () -> {
            for (int logIndex = 0; logIndex < numItems * logsPerFile; logIndex++) {
                IndexFilePointer pointer = groupMeta.indexFilePointer(logIndex);

                if (pointer != null) {
                    int expectedFileIndex = logIndex / logsPerFile;

                    int expectedStartLogIndex = expectedFileIndex * logsPerFile;

                    var expectedMeta = new IndexFileMeta(expectedStartLogIndex, expectedStartLogIndex + logsPerFile, expectedFileIndex);

                    assertThat(pointer.fileIndex(), is(startFileIndex + expectedFileIndex));
                    assertThat(pointer.fileMeta(), is(expectedMeta));
                }
            }
        };

        runRace(writer, reader, reader, reader);
    }
}
