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
import static org.hamcrest.Matchers.nullValue;

import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class GroupIndexMetaTest extends BaseIgniteAbstractTest {
    @Test
    void testAddGet() {
        var initialMeta = new IndexFileMeta(1, 50, 0, 0);

        var groupMeta = new GroupIndexMeta(initialMeta);

        var additionalMeta = new IndexFileMeta(50, 100, 42, 1);

        groupMeta.addIndexMeta(additionalMeta);

        assertThat(groupMeta.indexMeta(0), is(nullValue()));

        assertThat(groupMeta.indexMeta(1), is(initialMeta));

        assertThat(groupMeta.indexMeta(50), is(additionalMeta));

        assertThat(groupMeta.indexMeta(66), is(additionalMeta));

        assertThat(groupMeta.indexMeta(100), is(nullValue()));
    }

    @RepeatedTest(10)
    void testOneWriterMultipleReaders() {
        int startFileOrdinal = 100;

        int logEntriesPerFile = 50;

        var initialMeta = new IndexFileMeta(0, logEntriesPerFile, 0, startFileOrdinal);

        var groupMeta = new GroupIndexMeta(initialMeta);

        int totalIndexFiles = 1000;

        RunnableX writer = () -> {
            for (int relativeFileOrdinal = 1; relativeFileOrdinal < totalIndexFiles; relativeFileOrdinal++) {
                long startLogIndex = relativeFileOrdinal * logEntriesPerFile;
                long lastLogIndex = startLogIndex + logEntriesPerFile;

                groupMeta.addIndexMeta(new IndexFileMeta(startLogIndex, lastLogIndex, 0, startFileOrdinal + relativeFileOrdinal));
            }
        };

        int totalLogEntries = totalIndexFiles * logEntriesPerFile;

        RunnableX reader = () -> {
            for (int logIndex = 0; logIndex < totalLogEntries; logIndex++) {
                IndexFileMeta indexFileMeta = groupMeta.indexMeta(logIndex);

                if (indexFileMeta != null) {
                    int relativeFileOrdinal = logIndex / logEntriesPerFile;

                    int expectedFileOrdinal = startFileOrdinal + relativeFileOrdinal;

                    int expectedStartLogIndex = relativeFileOrdinal * logEntriesPerFile;

                    int expectedEndLogIndex = expectedStartLogIndex + logEntriesPerFile;

                    var expectedMeta = new IndexFileMeta(expectedStartLogIndex, expectedEndLogIndex, 0, expectedFileOrdinal);

                    assertThat(indexFileMeta, is(expectedMeta));
                }
            }
        };

        runRace(writer, reader, reader, reader);
    }
}
