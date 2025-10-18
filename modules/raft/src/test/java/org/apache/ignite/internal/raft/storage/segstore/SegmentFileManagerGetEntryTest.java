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
import static org.hamcrest.Matchers.notNullValue;
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

        LogEntry entry = createLogEntry(logIndex, 5);

        fileManager.appendEntry(groupId, entry, encoder);

        assertThat(fileManager.getEntry(groupId, logIndex, decoder), is(entry));

        assertThat(fileManager.getEntry(groupId + 1, logIndex, decoder), is(nullValue()));
        assertThat(fileManager.getEntry(groupId, logIndex + 1, decoder), is(nullValue()));
    }

    @Test
    void testGetEntryFromRolloverFiles() throws IOException {
        int numEntries = 50;

        int entrySize = FILE_SIZE / 4;

        List<LogEntry> entries = IntStream.range(0, numEntries)
                .mapToObj(logIndex -> createLogEntry(logIndex, entrySize))
                .collect(toList());

        for (LogEntry e : entries) {
            fileManager.appendEntry(0, e, encoder);
        }

        for (LogEntry e : entries) {
            LogEntry actualEntry = fileManager.getEntry(0, e.getId().getIndex(), decoder);

            assertThat(actualEntry, is(e));
        }
    }

    @RepeatedTest(5)
    void getEntryMultithreadedTest() throws IOException {
        int numGroups = STRIPES;

        Map<Long, List<LogEntry>> entriesByGroupId = generateEntries(numGroups, 100, 10);

        var tasks = new ArrayList<RunnableX>();

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<LogEntry> entries = entriesByGroupId.get(groupId);

            tasks.add(() -> {
                for (LogEntry e : entries) {
                    fileManager.appendEntry(groupId, e, encoder);
                }
            });
        }

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<LogEntry> entries = entriesByGroupId.get(groupId);

            RunnableX reader = () -> {
                for (LogEntry e : entries) {
                    LogEntry actualEntry = fileManager.getEntry(groupId, e.getId().getIndex(), decoder);

                    if (actualEntry != null) {
                        assertThat(actualEntry, is(e));
                    }
                }
            };

            // Two readers per every group.
            tasks.add(reader);
            tasks.add(reader);
        }

        runRace(tasks.toArray(RunnableX[]::new));

        // Validate that data was actually inserted.
        for (Map.Entry<Long, List<LogEntry>> e : entriesByGroupId.entrySet()) {
            long groupId = e.getKey();

            for (LogEntry entry : e.getValue()) {
                LogEntry actualEntry = fileManager.getEntry(groupId, entry.getId().getIndex(), decoder);

                assertThat(actualEntry, is(entry));
            }
        }
    }

    @Test
    void testGetEntryWithSuffixTruncate() throws IOException {
        int entrySize = FILE_SIZE / 10;

        int numEntries = 100;

        long curLogIndex = 0;

        for (int i = 0; i < numEntries; i++) {
            var entry = createLogEntry(curLogIndex, entrySize);

            fileManager.appendEntry(0, entry, encoder);

            if (i > 0 && i % 10 == 0) {
                curLogIndex -= 4;

                fileManager.truncateSuffix(0, curLogIndex);

                // Check that the "lastIndexKept" entry is accessible, while the truncated one is not.
                assertThat(fileManager.getEntry(0, curLogIndex, decoder), is(notNullValue()));

                assertThat(fileManager.getEntry(0, curLogIndex + 1, decoder), is(nullValue()));
            }

            curLogIndex++;
        }
    }

    @RepeatedTest(5)
    void truncateSuffixMultithreadedTest() throws IOException {
        int numGroups = STRIPES;

        int entrySize = 10;

        int numEntriesPerGroup = 100;

        Map<Long, List<LogEntry>> entriesByGroupId = generateEntries(numGroups, numEntriesPerGroup, entrySize);

        // Entries that will be used to replace truncated entries.
        var replacementEntriesByGroupId = new HashMap<Long, List<LogEntry>>();

        int numReplacementEntriesPerGroup = numEntriesPerGroup / 5;

        for (long groupId = 0; groupId < numGroups; groupId++) {
            var entries = new ArrayList<LogEntry>(numReplacementEntriesPerGroup);

            for (int i = 0; i < numReplacementEntriesPerGroup; i++) {
                entries.add(createLogEntry(i * 5, entrySize));
            }

            replacementEntriesByGroupId.put(groupId, entries);
        }

        var tasks = new ArrayList<RunnableX>();

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<LogEntry> entries = entriesByGroupId.get(groupId);

            List<LogEntry> replacementEntries = replacementEntriesByGroupId.get(groupId);

            tasks.add(() -> {
                for (int entryIndex = 0; entryIndex < entries.size(); entryIndex++) {
                    LogEntry entry = entries.get(entryIndex);

                    fileManager.appendEntry(groupId, entry, encoder);

                    // Truncate every 5th entry.
                    if (entryIndex % 5 == 0) {
                        fileManager.truncateSuffix(groupId, entryIndex - 1);

                        LogEntry replacementEntry = replacementEntries.get(entryIndex / 5);

                        fileManager.appendEntry(groupId, replacementEntry, encoder);
                    }
                }
            });
        }

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<LogEntry> entries = entriesByGroupId.get(groupId);

            List<LogEntry> replacementEntries = replacementEntriesByGroupId.get(groupId);

            RunnableX reader = () -> {
                for (int logIndex = 0; logIndex < entries.size(); logIndex++) {
                    LogEntry actualEntry = fileManager.getEntry(groupId, logIndex, decoder);

                    if (actualEntry == null) {
                        continue;
                    }

                    LogEntry expectedEntry = entries.get(logIndex);

                    if (logIndex % 5 == 0) {
                        // Here we can read both the truncated and the replacement entry.
                        LogEntry replacementEntry = replacementEntries.get(logIndex / 5);

                        assertThat(actualEntry, either(sameInstance(replacementEntry)).or(sameInstance(expectedEntry)));
                    } else {
                        assertThat(actualEntry, is(sameInstance(expectedEntry)));
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
            List<LogEntry> entries = entriesByGroupId.get(groupId);

            List<LogEntry> replacementEntries = replacementEntriesByGroupId.get(groupId);

            for (int logIndex = 0; logIndex < entries.size(); logIndex++) {
                LogEntry expectedEntry = logIndex % 5 == 0 ? replacementEntries.get(logIndex / 5) : entries.get(logIndex);

                LogEntry actualEntry = fileManager.getEntry(groupId, logIndex, decoder);

                assertThat(actualEntry, is(sameInstance(expectedEntry)));
            }
        }
    }

    private Map<Long, List<LogEntry>> generateEntries(int numGroups, int numEntriesPerGroup, int entrySize) {
        Map<Long, List<LogEntry>> entriesByGroupId = newHashMap(numGroups);

        for (long groupId = 0; groupId < numGroups; groupId++) {
            var entries = new ArrayList<LogEntry>(numEntriesPerGroup);

            for (int i = 0; i < numEntriesPerGroup; i++) {
                entries.add(createLogEntry(i, entrySize));
            }

            entriesByGroupId.put(groupId, entries);
        }

        return entriesByGroupId;
    }

    private LogEntry createLogEntry(long logIndex, int entrySize) {
        LogEntry logEntry = new LogEntry();

        logEntry.setId(new LogId(logIndex, 0));

        byte[] bytes = randomBytes(ThreadLocalRandom.current(), entrySize);

        lenient().doAnswer(invocationOnMock -> {
            ByteBuffer buffer = invocationOnMock.getArgument(0);

            buffer.put(bytes);

            return null;
        }).when(encoder).encode(any(), same(logEntry));

        lenient().when(encoder.size(same(logEntry))).thenReturn(entrySize);

        lenient().when(decoder.decode(bytes)).thenReturn(logEntry);

        return logEntry;
    }
}
