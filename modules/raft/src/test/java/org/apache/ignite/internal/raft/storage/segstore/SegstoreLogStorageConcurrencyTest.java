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
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.List;
import java.util.function.IntFunction;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SegstoreLogStorageConcurrencyTest extends IgniteAbstractTest {
    private static final int SEGMENT_SIZE = 100;

    private static final String NODE_NAME = "test";

    private SegmentFileManager segmentFileManager;

    @BeforeEach
    void setUp() throws IOException {
        segmentFileManager = new SegmentFileManager(NODE_NAME, workDir, SEGMENT_SIZE, 1, new NoOpFailureManager());

        segmentFileManager.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(segmentFileManager);
    }

    protected SegstoreLogStorage newLogStorage(long groupId) {
        var logStorage = new SegstoreLogStorage(groupId, segmentFileManager);

        var opts = new LogStorageOptions();
        opts.setConfigurationManager(new ConfigurationManager());
        opts.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());

        logStorage.init(opts);

        return logStorage;
    }

    @Test
    void testFirstAndLastIndexOnAppend() {
        List<LogEntry> entries = TestUtils.mockEntries();

        IntFunction<RunnableX> writerTaskFactory = groupId -> () -> {
            SegstoreLogStorage logStorage = newLogStorage(groupId);

            try {
                assertThat(logStorage.getFirstLogIndex(), is(1L));
                assertThat(logStorage.getLastLogIndex(), is(0L));

                for (int i = 0; i < entries.size(); i++) {
                    logStorage.appendEntry(entries.get(i));

                    assertThat(logStorage.getFirstLogIndex(), is(0L));
                    assertThat(logStorage.getLastLogIndex(), is((long) i));
                }

                assertThat(logStorage.getFirstLogIndex(), is(0L));
                assertThat(logStorage.getLastLogIndex(), is((long) entries.size() - 1));
            } finally {
                logStorage.shutdown();
            }
        };

        runRace(writerTaskFactory.apply(1), writerTaskFactory.apply(2));
    }

    @Test
    void testLastIndexAfterTruncateSuffix() {
        List<LogEntry> entries = TestUtils.mockEntries();

        IntFunction<RunnableX> writerTaskFactory = groupId -> () -> {
            SegstoreLogStorage logStorage = newLogStorage(groupId);

            try {
                assertThat(logStorage.getFirstLogIndex(), is(1L));
                assertThat(logStorage.getLastLogIndex(), is(0L));

                long curLogIndex = 0;

                for (int i = 0; i < entries.size(); i++) {
                    logStorage.appendEntry(entries.get(i));

                    if (i > 0 && i % 10 == 0) {
                        curLogIndex -= 4;

                        logStorage.truncateSuffix(curLogIndex);
                    }

                    assertThat(logStorage.getFirstLogIndex(), is(0L));
                    assertThat(logStorage.getLastLogIndex(), is(curLogIndex));

                    curLogIndex++;
                }

                assertThat(logStorage.getFirstLogIndex(), is(0L));
                assertThat(logStorage.getLastLogIndex(), is(curLogIndex - 1));
            } finally {
                logStorage.shutdown();
            }
        };

        runRace(writerTaskFactory.apply(1), writerTaskFactory.apply(2));
    }

    @Test
    void testFirstIndexAfterTruncatePrefix() {
        List<LogEntry> entries = TestUtils.mockEntries();

        IntFunction<RunnableX> writerTaskFactory = groupId -> () -> {
            SegstoreLogStorage logStorage = newLogStorage(groupId);

            try {
                assertThat(logStorage.getFirstLogIndex(), is(1L));
                assertThat(logStorage.getLastLogIndex(), is(0L));

                long firstLogIndex = 0;

                for (int i = 0; i < entries.size(); i++) {
                    LogEntry entry = entries.get(i);

                    logStorage.appendEntry(entry);

                    long logIndex = entry.getId().getIndex();

                    if (i > 0 && i % 10 == 0) {
                        logStorage.truncatePrefix(logIndex);

                        firstLogIndex = logIndex;
                    }

                    assertThat(logStorage.getFirstLogIndex(), is(firstLogIndex));
                    assertThat(logStorage.getLastLogIndex(), is(logIndex));
                }

                assertThat(logStorage.getFirstLogIndex(), is(firstLogIndex));
                assertThat(logStorage.getLastLogIndex(), is((long) entries.size() - 1));
            } finally {
                logStorage.shutdown();
            }
        };

        runRace(writerTaskFactory.apply(1), writerTaskFactory.apply(2));
    }
}
