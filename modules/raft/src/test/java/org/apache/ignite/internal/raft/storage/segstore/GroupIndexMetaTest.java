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

        var additionalMeta = new IndexFileMeta(51, 100, 42);

        groupMeta.addIndexMeta(additionalMeta);

        assertThat(groupMeta.indexFilePointer(0), is(nullValue()));

        IndexFilePointer pointer = groupMeta.indexFilePointer(1);

        assertThat(pointer, is(notNullValue()));
        assertThat(pointer.fileOrdinal(), is(0));
        assertThat(pointer.fileMeta(), is(initialMeta));

        pointer = groupMeta.indexFilePointer(66);

        assertThat(pointer, is(notNullValue()));
        assertThat(pointer.fileOrdinal(), is(1));
        assertThat(pointer.fileMeta(), is(additionalMeta));

        pointer = groupMeta.indexFilePointer(101);

        assertThat(pointer, is(nullValue()));
    }

    @RepeatedTest(10)
    void testOneWriterMultipleReaders() {
        int startFileOrdinal = 100;

        int logEntriesPerFile = 50;

        var initialMeta = new IndexFileMeta(0, logEntriesPerFile - 1, 0);

        var groupMeta = new GroupIndexMeta(startFileOrdinal, initialMeta);

        int totalIndexFiles = 1000;

        RunnableX writer = () -> {
            for (int relativeFileOrdinal = 1; relativeFileOrdinal < totalIndexFiles; relativeFileOrdinal++) {
                long startLogIndex = relativeFileOrdinal * logEntriesPerFile;
                long lastLogIndex = startLogIndex + logEntriesPerFile - 1;

                groupMeta.addIndexMeta(new IndexFileMeta(startLogIndex, lastLogIndex, 0));
            }
        };

        int totalLogEntries = totalIndexFiles * logEntriesPerFile;

        RunnableX reader = () -> {
            for (int logIndex = 0; logIndex < totalLogEntries; logIndex++) {
                IndexFilePointer pointer = groupMeta.indexFilePointer(logIndex);

                if (pointer != null) {
                    int relativeFileOrdinal = logIndex / logEntriesPerFile;

                    int expectedFileOrdinal = startFileOrdinal + relativeFileOrdinal;

                    int expectedStartLogIndex = relativeFileOrdinal * logEntriesPerFile;

                    int expectedEndLogIndex = expectedStartLogIndex + logEntriesPerFile - 1;

                    var expectedMeta = new IndexFileMeta(expectedStartLogIndex, expectedEndLogIndex, 0);

                    assertThat(pointer.fileOrdinal(), is(expectedFileOrdinal));
                    assertThat(pointer.fileMeta(), is(expectedMeta));
                }
            }
        };

        runRace(writer, reader, reader, reader);
    }
}
