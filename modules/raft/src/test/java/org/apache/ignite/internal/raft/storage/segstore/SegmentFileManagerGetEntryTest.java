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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.lenient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentFileManagerGetEntryTest extends IgniteAbstractTest {
    private static final String NODE_NAME = "test";

    private static final int STRIPES = 10;

    private static final int FILE_SIZE = 1000;

    private SegmentFileManager fileManager;

    @Mock
    private LogEntryEncoder encoder;

    @Mock
    private LogEntryDecoder decoder;

    @BeforeEach
    void setUp() throws IOException {
        fileManager = new SegmentFileManager(NODE_NAME, workDir, FILE_SIZE, STRIPES, new NoOpFailureManager());

        fileManager.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(fileManager);
    }

    @Test
    void testGetEntryFromCurrentFile() throws IOException {
        long groupId = 1;

        int logIndex = 1;

        MockEntry entry = new MockEntry(logIndex, 5);

        fileManager.appendEntry(groupId, entry.logEntry, encoder);

        assertThat(fileManager.getEntry(groupId, logIndex, decoder), is(entry.logEntry));

        assertThat(fileManager.getEntry(groupId + 1, logIndex, decoder), is(nullValue()));
        assertThat(fileManager.getEntry(groupId, logIndex + 1, decoder), is(nullValue()));
    }

    @Test
    void testGetEntryFromRolloverFiles() throws IOException {
        int numEntries = 50;

        int entrySize = FILE_SIZE / 4;

        List<MockEntry> entries = IntStream.range(0, numEntries)
                .mapToObj(logIndex -> new MockEntry(logIndex, entrySize))
                .collect(toList());

        for (MockEntry e : entries) {
            fileManager.appendEntry(0, e.logEntry, encoder);
        }

        for (MockEntry e : entries) {
            LogEntry actualEntry = fileManager.getEntry(0, e.logIndex(), decoder);

            assertThat(actualEntry, is(e.logEntry));
        }
    }

    @RepeatedTest(5)
    void getEntryMultithreadedTest() throws IOException {
        int numGroups = STRIPES;

        Map<Long, List<MockEntry>> entriesByGroupId = generateEntries(numGroups, 100, 10);

        var tasks = new ArrayList<RunnableX>();

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<MockEntry> entries = entriesByGroupId.get(groupId);

            tasks.add(() -> {
                for (MockEntry e : entries) {
                    fileManager.appendEntry(groupId, e.logEntry, encoder);
                }
            });
        }

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<MockEntry> entries = entriesByGroupId.get(groupId);

            RunnableX reader = () -> {
                for (MockEntry e : entries) {
                    LogEntry actualEntry = fileManager.getEntry(groupId, e.logIndex(), decoder);

                    if (actualEntry != null) {
                        assertThat(actualEntry, is(e.logEntry));
                    }
                }
            };

            // Two readers per every group.
            tasks.add(reader);
            tasks.add(reader);
        }

        runRace(tasks.toArray(RunnableX[]::new));

        // Validate that data was actually inserted.
        for (Map.Entry<Long, List<MockEntry>> e : entriesByGroupId.entrySet()) {
            long groupId = e.getKey();

            for (MockEntry entry : e.getValue()) {
                LogEntry actualEntry = fileManager.getEntry(groupId, entry.logIndex(), decoder);

                assertThat(actualEntry, is(entry.logEntry));
            }
        }
    }

    @Test
    void testGetEntryWithSuffixTruncate() throws IOException {
        Map<Long, List<MockEntry>> entriesByGroupId = generateEntries(2, 3, 10);

        for (Map.Entry<Long, List<MockEntry>> e : entriesByGroupId.entrySet()) {
            long groupId = e.getKey();

            for (MockEntry entry : e.getValue()) {
                fileManager.appendEntry(groupId, entry.logEntry, encoder);
            }
        }

        fileManager.truncateSuffix(0, 1);
        fileManager.truncateSuffix(1, 2);

        assertThat(
                fileManager.getEntry(0, 1, decoder),
                is(entriesByGroupId.get(0L).get(1).logEntry)
        );

        assertThat(
                fileManager.getEntry(1, 1, decoder),
                is(entriesByGroupId.get(1L).get(1).logEntry)
        );

        assertThat(
                fileManager.getEntry(0, 2, decoder),
                is(nullValue())
        );

        assertThat(
                fileManager.getEntry(1, 2, decoder),
                is(entriesByGroupId.get(1L).get(2).logEntry)
        );
    }

    @RepeatedTest(5)
    void truncateSuffixMultithreadedTest() throws IOException {
        int numGroups = STRIPES;

        int entrySize = 10;

        int numEntriesPerGroup = 100;

        Map<Long, List<MockEntry>> entriesByGroupId = generateEntries(numGroups, numEntriesPerGroup, entrySize);

        // Entries that will be used to replace truncated entries.
        var replacementEntriesByGroupId = new HashMap<Long, List<MockEntry>>();

        int numReplacementEntriesPerGroup = numEntriesPerGroup / 5;

        for (long groupId = 0; groupId < numGroups; groupId++) {
            var entries = new ArrayList<MockEntry>(numReplacementEntriesPerGroup);

            for (int i = 0; i < numReplacementEntriesPerGroup; i++) {
                entries.add(new MockEntry(i * 5, entrySize));
            }

            replacementEntriesByGroupId.put(groupId, entries);
        }

        var tasks = new ArrayList<RunnableX>();

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<MockEntry> entries = entriesByGroupId.get(groupId);

            List<MockEntry> replacementEntries = replacementEntriesByGroupId.get(groupId);

            tasks.add(() -> {
                for (int entryIndex = 0; entryIndex < entries.size(); entryIndex++) {
                    MockEntry entry = entries.get(entryIndex);

                    fileManager.appendEntry(groupId, entry.logEntry, encoder);

                    // Truncate every 5th entry.
                    if (entryIndex % 5 == 0) {
                        fileManager.truncateSuffix(groupId, entryIndex - 1);

                        MockEntry replacementEntry = replacementEntries.get(entryIndex / 5);

                        fileManager.appendEntry(groupId, replacementEntry.logEntry, encoder);
                    }
                }
            });
        }

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<MockEntry> entries = entriesByGroupId.get(groupId);

            List<MockEntry> replacementEntries = replacementEntriesByGroupId.get(groupId);

            RunnableX reader = () -> {
                for (int logIndex = 0; logIndex < entries.size(); logIndex++) {
                    LogEntry actualEntry = fileManager.getEntry(groupId, logIndex, decoder);

                    if (actualEntry == null) {
                        continue;
                    }

                    MockEntry expectedEntry = entries.get(logIndex);

                    if (logIndex % 5 == 0) {
                        // Here we can read both the truncated and the replacement entry.
                        MockEntry replacementEntry = replacementEntries.get(logIndex / 5);

                        assertThat(actualEntry, either(sameInstance(replacementEntry.logEntry)).or(sameInstance(expectedEntry.logEntry)));
                    } else {
                        assertThat(actualEntry, is(sameInstance(expectedEntry.logEntry)));
                    }
                }
            };

            // Two readers per every group.
            tasks.add(reader);
            tasks.add(reader);
        }

        runRace(tasks.toArray(RunnableX[]::new));

        // Validate that data was actually inserted.
        for (long groupId = 0; groupId < numGroups; groupId++) {
            List<MockEntry> entries = entriesByGroupId.get(groupId);

            List<MockEntry> replacementEntries = replacementEntriesByGroupId.get(groupId);

            for (int logIndex = 0; logIndex < entries.size(); logIndex++) {
                MockEntry expectedEntry = logIndex % 5 == 0 ? replacementEntries.get(logIndex / 5) : entries.get(logIndex);

                LogEntry actualEntry = fileManager.getEntry(groupId, logIndex, decoder);

                assertThat(actualEntry, is(sameInstance(expectedEntry.logEntry)));
            }
        }
    }

    private Map<Long, List<MockEntry>> generateEntries(int numGroups, int numEntriesPerGroup, int entrySize) {
        Map<Long, List<MockEntry>> entriesByGroupId = newHashMap(numGroups);

        for (long groupId = 0; groupId < numGroups; groupId++) {
            var entries = new ArrayList<MockEntry>(numEntriesPerGroup);

            for (int i = 0; i < numEntriesPerGroup; i++) {
                entries.add(new MockEntry(i, entrySize));
            }

            entriesByGroupId.put(groupId, entries);
        }

        return entriesByGroupId;
    }

    private class MockEntry {
        private final LogEntry logEntry = new LogEntry();

        MockEntry(long logIndex, int entrySize) {
            logEntry.setId(new LogId(logIndex, 0));

            byte[] bytes = randomBytes(ThreadLocalRandom.current(), entrySize);

            lenient().doAnswer(invocationOnMock -> {
                ByteBuffer buffer = invocationOnMock.getArgument(0);

                buffer.put(bytes);

                return null;
            }).when(encoder).encode(any(), same(logEntry));

            lenient().when(encoder.size(same(logEntry))).thenReturn(entrySize);

            lenient().when(decoder.decode(bytes)).thenReturn(logEntry);
        }

        long logIndex() {
            return logEntry.getId().getIndex();
        }
    }
}
