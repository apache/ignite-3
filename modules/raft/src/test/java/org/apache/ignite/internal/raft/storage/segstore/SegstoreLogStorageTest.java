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

import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.BaseLogStorageTest;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(ConfigurationExtension.class)
class SegstoreLogStorageTest extends BaseLogStorageTest {
    private static final int SEGMENT_SIZE = 512 * 1024; // Same as in JRaft tests.

    private static final long GROUP_ID = 1000;

    private static final String NODE_NAME = "test";

    private SegmentFileManager segmentFileManager;

    @InjectConfiguration("mock.segmentFileSizeBytes=" + SEGMENT_SIZE)
    private LogStorageConfiguration storageConfiguration;

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(segmentFileManager);
    }

    @Override
    protected LogStorage newLogStorage() {
        try {
            segmentFileManager = new SegmentFileManager(
                    NODE_NAME,
                    path,
                    1,
                    new NoOpFailureManager(),
                    storageConfiguration
            );

            segmentFileManager.start();

            logStorage = new SegstoreLogStorage(GROUP_ID, segmentFileManager);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return logStorage;
    }

    @ParameterizedTest
    // Number of entries is chosen to test scenarios with zero index files and with multiple index files.
    @ValueSource(ints = { 15, 100_000 })
    public void firstAndLastLogIndexAfterRestart(int numEntries) throws Exception {
        logStorage.appendEntries(TestUtils.mockEntries(numEntries));

        logStorage.shutdown();
        segmentFileManager.close();

        logStorage = newLogStorage();
        logStorage.init(newLogStorageOptions());

        assertThat(logStorage.getFirstLogIndex(), is(0L));
        assertThat(logStorage.getLastLogIndex(), is((long) numEntries - 1));
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 100_000 })
    public void firstAndLastLogIndexAfterSuffixTruncateAndRestart(int numEntries) throws Exception {
        logStorage.appendEntries(TestUtils.mockEntries(numEntries));

        long lastIndexKept = numEntries / 2;

        logStorage.truncateSuffix(lastIndexKept);

        assertThat(logStorage.getFirstLogIndex(), is(0L));
        assertThat(logStorage.getLastLogIndex(), is(lastIndexKept));

        logStorage.shutdown();
        segmentFileManager.close();

        logStorage = newLogStorage();
        logStorage.init(newLogStorageOptions());

        assertThat(logStorage.getFirstLogIndex(), is(0L));
        assertThat(logStorage.getLastLogIndex(), is(lastIndexKept));
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 100_000 })
    public void firstAndLastLogIndexAfterPrefixTruncateAndRestart(int numEntries) throws Exception {
        logStorage.appendEntries(TestUtils.mockEntries(numEntries));

        long firstIndexKept = numEntries / 2;

        logStorage.truncatePrefix(firstIndexKept);

        assertThat(logStorage.getFirstLogIndex(), is(firstIndexKept));
        assertThat(logStorage.getLastLogIndex(), is((long) numEntries - 1));

        logStorage.shutdown();
        segmentFileManager.close();

        logStorage = newLogStorage();
        logStorage.init(newLogStorageOptions());

        assertThat(logStorage.getFirstLogIndex(), is(firstIndexKept));
        assertThat(logStorage.getLastLogIndex(), is((long) numEntries - 1));
    }

    @ParameterizedTest
    @ValueSource(ints = { 15, 100_000 })
    public void firstAndLastLogIndexAfterResetAndRestart(int numEntries) throws Exception {
        logStorage.appendEntries(TestUtils.mockEntries(numEntries));

        long nextLogIndex = numEntries / 2;

        logStorage.reset(nextLogIndex);

        assertThat(logStorage.getFirstLogIndex(), is(nextLogIndex));
        assertThat(logStorage.getLastLogIndex(), is(nextLogIndex));

        logStorage.shutdown();
        segmentFileManager.close();

        logStorage = newLogStorage();
        logStorage.init(newLogStorageOptions());

        assertThat(logStorage.getFirstLogIndex(), is(nextLogIndex));
        assertThat(logStorage.getLastLogIndex(), is(nextLogIndex));
    }
}
