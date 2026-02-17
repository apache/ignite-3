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
import static org.hamcrest.Matchers.nullValue;

import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

class GroupIndexMetaTest extends BaseIgniteAbstractTest {
    @Test
    void testAddGet() {
        var initialMeta = new IndexFileMeta(1, 50, 0, new FileProperties(0));

        var groupMeta = new GroupIndexMeta(initialMeta);

        var additionalMeta = new IndexFileMeta(50, 100, 42, new FileProperties(1));

        groupMeta.addIndexMeta(additionalMeta);

        assertThat(groupMeta.indexMeta(0), is(nullValue()));

        assertThat(groupMeta.indexMeta(1), is(initialMeta));

        assertThat(groupMeta.indexMeta(50), is(additionalMeta));

        assertThat(groupMeta.indexMeta(66), is(additionalMeta));

        assertThat(groupMeta.indexMeta(100), is(nullValue()));
    }

    @Test
    void testAddGetWithOverlap() {
        var initialMeta = new IndexFileMeta(1, 100, 0, new FileProperties(0));

        var groupMeta = new GroupIndexMeta(initialMeta);

        var additionalMeta = new IndexFileMeta(42, 100, 42, new FileProperties(1));

        groupMeta.addIndexMeta(additionalMeta);

        assertThat(groupMeta.indexMeta(0), is(nullValue()));

        assertThat(groupMeta.indexMeta(1), is(initialMeta));

        assertThat(groupMeta.indexMeta(41), is(initialMeta));

        assertThat(groupMeta.indexMeta(42), is(additionalMeta));

        assertThat(groupMeta.indexMeta(66), is(additionalMeta));

        assertThat(groupMeta.indexMeta(100), is(nullValue()));
    }

    @Test
    void testEmptyMetas() {
        var initialMeta = new IndexFileMeta(1, 1, 0, new FileProperties(0));

        var groupMeta = new GroupIndexMeta(initialMeta);

        assertThat(groupMeta.indexMeta(1), is(nullValue()));

        assertThat(groupMeta.firstLogIndexInclusive(), is(-1L));

        assertThat(groupMeta.lastLogIndexExclusive(), is(1L));

        var additionalMeta = new IndexFileMeta(1, 2, 42, new FileProperties(1));

        groupMeta.addIndexMeta(additionalMeta);

        assertThat(groupMeta.indexMeta(1), is(additionalMeta));

        assertThat(groupMeta.indexMeta(2), is(nullValue()));

        assertThat(groupMeta.firstLogIndexInclusive(), is(1L));

        assertThat(groupMeta.lastLogIndexExclusive(), is(2L));
    }

    @RepeatedTest(10)
    void testOneWriterMultipleReaders() {
        int startFileOrdinal = 100;

        int logEntriesPerFile = 50;

        var initialMeta = new IndexFileMeta(0, logEntriesPerFile, 0, new FileProperties(startFileOrdinal));

        var groupMeta = new GroupIndexMeta(initialMeta);

        int totalIndexFiles = 1000;

        RunnableX writer = () -> {
            for (int relativeFileOrdinal = 1; relativeFileOrdinal < totalIndexFiles; relativeFileOrdinal++) {
                long startLogIndex = relativeFileOrdinal * logEntriesPerFile;
                long lastLogIndex = startLogIndex + logEntriesPerFile;

                groupMeta.addIndexMeta(
                        new IndexFileMeta(startLogIndex, lastLogIndex, 0, new FileProperties(startFileOrdinal + relativeFileOrdinal)));
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

                    var expectedMeta = new IndexFileMeta(expectedStartLogIndex, expectedEndLogIndex, 0,
                            new FileProperties(expectedFileOrdinal));

                    assertThat(indexFileMeta, is(expectedMeta));
                }
            }
        };

        runRace(writer, reader, reader, reader);
    }

    @RepeatedTest(10)
    void testOneWriterMultipleReadersWithOverlaps() {
        int startFileOrdinal = 100;

        int logEntriesPerFile = 50;

        var initialMeta = new IndexFileMeta(0, logEntriesPerFile - 1, 0, new FileProperties(startFileOrdinal));

        var groupMeta = new GroupIndexMeta(initialMeta);

        int totalIndexFiles = 1000;

        int overlap = 10;

        RunnableX writer = () -> {
            for (int relativeFileOrdinal = 1; relativeFileOrdinal < totalIndexFiles; relativeFileOrdinal++) {
                long startLogIndex = relativeFileOrdinal * (logEntriesPerFile - overlap);
                long lastLogIndex = startLogIndex + logEntriesPerFile - 1;

                groupMeta.addIndexMeta(
                        new IndexFileMeta(startLogIndex, lastLogIndex, 0, new FileProperties(startFileOrdinal + relativeFileOrdinal)));
            }
        };

        int totalLogEntries = totalIndexFiles * logEntriesPerFile;

        RunnableX reader = () -> {
            int expectedFirstLogIndex = 0;

            int relativeFileOrdinal = 0;

            for (int logIndex = 0; logIndex < totalLogEntries; logIndex++) {
                IndexFileMeta indexFileMeta = groupMeta.indexMeta(logIndex);

                int nextFirstLogIndex = expectedFirstLogIndex + logEntriesPerFile - overlap;

                // Last file is special, as it doesn't have an overlap.
                if (logIndex >= nextFirstLogIndex && relativeFileOrdinal != totalIndexFiles - 1) {
                    expectedFirstLogIndex = nextFirstLogIndex;
                    relativeFileOrdinal++;
                }

                if (indexFileMeta != null) {
                    int expectedFileOrdinal = startFileOrdinal + relativeFileOrdinal;

                    var expectedMetaWithOverlap = new IndexFileMeta(
                            expectedFirstLogIndex,
                            expectedFirstLogIndex + logEntriesPerFile - 1,
                            0,
                            new FileProperties(expectedFileOrdinal)
                    );

                    if (expectedFirstLogIndex == 0) {
                        assertThat(logIndex + " -> " + indexFileMeta, indexFileMeta, is(expectedMetaWithOverlap));
                    } else {
                        var expectedMetaWithoutOverlap = new IndexFileMeta(
                                expectedFirstLogIndex + overlap - logEntriesPerFile,
                                expectedFirstLogIndex + overlap - 1,
                                0,
                                new FileProperties(expectedFileOrdinal - 1)
                        );

                        // We can possibly be reading from two different metas - from the newer one (that overlaps the older one) or
                        // the older one.
                        assertThat(
                                logIndex + " -> " + indexFileMeta,
                                indexFileMeta, either(is(expectedMetaWithOverlap)).or(is(expectedMetaWithoutOverlap))
                        );
                    }
                }
            }
        };

        runRace(writer, reader, reader, reader);
    }

    @Test
    void testTruncatePrefix() {
        var meta1 = new IndexFileMeta(1, 100, 0, new FileProperties(0));
        var meta2 = new IndexFileMeta(42, 100, 42, new FileProperties(1));
        var meta3 = new IndexFileMeta(100, 120, 66, new FileProperties(2));
        var meta4 = new IndexFileMeta(110, 200, 95, new FileProperties(3));

        var groupMeta = new GroupIndexMeta(meta1);

        groupMeta.addIndexMeta(meta2);
        groupMeta.addIndexMeta(meta3);
        groupMeta.addIndexMeta(meta4);

        assertThat(groupMeta.firstLogIndexInclusive(), is(1L));
        assertThat(groupMeta.lastLogIndexExclusive(), is(200L));

        assertThat(groupMeta.indexMeta(10), is(meta1));
        assertThat(groupMeta.indexMeta(42), is(meta2));
        assertThat(groupMeta.indexMeta(100), is(meta3));
        assertThat(groupMeta.indexMeta(110), is(meta4));

        groupMeta.truncatePrefix(43);

        assertThat(groupMeta.indexMeta(10), is(nullValue()));
        assertThat(groupMeta.indexMeta(42), is(nullValue()));

        // Payload offset is shifted 4 bytes in order to skip the truncated entry.
        var trimmedMeta = new IndexFileMeta(43, 100, 46, new FileProperties(1));

        assertThat(groupMeta.indexMeta(43), is(trimmedMeta));
        assertThat(groupMeta.indexMeta(100), is(meta3));
        assertThat(groupMeta.indexMeta(110), is(meta4));

        groupMeta.truncatePrefix(110);

        assertThat(groupMeta.indexMeta(43), is(nullValue()));
        assertThat(groupMeta.indexMeta(100), is(nullValue()));
        assertThat(groupMeta.indexMeta(110), is(meta4));
    }

    @Test
    void testTruncatePrefixRemovesAllEntriesWhenKeptAfterLast() {
        var meta1 = new IndexFileMeta(1, 10, 0, new FileProperties(0));
        var meta2 = new IndexFileMeta(10, 20, 100, new FileProperties(1));

        var groupMeta = new GroupIndexMeta(meta1);
        groupMeta.addIndexMeta(meta2);

        // Truncate to the end of last meta - everything should be removed.
        groupMeta.truncatePrefix(20);

        assertThat(groupMeta.indexMeta(0), is(nullValue()));
        assertThat(groupMeta.indexMeta(19), is(nullValue()));
        assertThat(groupMeta.firstLogIndexInclusive(), is(-1L));
    }
}
