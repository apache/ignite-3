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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SegmentFileManagerGetEntryTest extends IgniteAbstractTest {
    private static final String NODE_NAME = "test";

    private static final int STRIPES = 10;

    private static final int FILE_SIZE = 1000;

    private SegmentFileManager fileManager;

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

        fileManager.appendEntry(groupId, entry.logEntry, entry.encoder);

        assertThat(fileManager.getEntry(groupId, logIndex, entry.decoder), is(entry.logEntry));

        assertThat(fileManager.getEntry(groupId + 1, logIndex, entry.decoder), is(nullValue()));
        assertThat(fileManager.getEntry(groupId, logIndex + 1, entry.decoder), is(nullValue()));
    }

    @Test
    void testGetEntryFromRolloverFiles() throws IOException {
        int numEntries = 50;

        int entrySize = FILE_SIZE / 4;

        List<MockEntry> entries = IntStream.range(0, numEntries)
                .mapToObj(logIndex -> new MockEntry(logIndex, entrySize))
                .collect(toList());

        for (MockEntry e : entries) {
            fileManager.appendEntry(0, e.logEntry, e.encoder);
        }

        for (MockEntry e : entries) {
            LogEntry actualEntry = fileManager.getEntry(0, e.logIndex(), e.decoder);

            assertThat(actualEntry, is(e.logEntry));
        }
    }

    @RepeatedTest(10)
    void getEntryMultithreadedTest() throws IOException {
        int numGroups = STRIPES;

        Map<Long, List<MockEntry>> entriesByGroupId = generateEntries(numGroups, 100, 10);

        var tasks = new ArrayList<RunnableX>();

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<MockEntry> entries = entriesByGroupId.get(groupId);

            tasks.add(() -> {
                for (MockEntry e : entries) {
                    fileManager.appendEntry(groupId, e.logEntry, e.encoder);
                }
            });
        }

        for (int i = 0; i < numGroups; i++) {
            long groupId = i;

            List<MockEntry> entries = entriesByGroupId.get(groupId);

            RunnableX reader = () -> {
                for (MockEntry e : entries) {
                    LogEntry actualEntry = fileManager.getEntry(groupId, e.logIndex(), e.decoder);

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
                LogEntry actualEntry = fileManager.getEntry(groupId, entry.logIndex(), entry.decoder);

                assertThat(actualEntry, is(entry.logEntry));
            }
        }
    }

    private static Map<Long, List<MockEntry>> generateEntries(int numGroups, int numEntriesPerGroup, int entrySize) {
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

    private static class MockEntry {
        private final LogEntry logEntry = mock(LogEntry.class);

        private final LogEntryEncoder encoder;

        private final LogEntryDecoder decoder;

        MockEntry(long logIndex, int entrySize) {
            when(logEntry.getId()).thenReturn(new LogId(logIndex, 0));

            byte[] bytes = randomBytes(ThreadLocalRandom.current(), entrySize);

            encoder = new LogEntryEncoder() {
                @Override
                public byte[] encode(LogEntry log) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void encode(ByteBuffer buffer, LogEntry log) {
                    buffer.put(bytes);
                }

                @Override
                public int size(LogEntry logEntry) {
                    return entrySize;
                }
            };

            decoder = new LogEntryDecoder() {
                @Override
                public LogEntry decode(byte[] bs) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public LogEntry decode(ByteBuffer bs) {
                    return logEntry;
                }
            };
        }

        long logIndex() {
            return logEntry.getId().getIndex();
        }
    }
}
