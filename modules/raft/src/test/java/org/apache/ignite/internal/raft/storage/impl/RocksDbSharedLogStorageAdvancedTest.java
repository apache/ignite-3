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

package org.apache.ignite.internal.raft.storage.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for implementation specifics of the shared storage.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class RocksDbSharedLogStorageAdvancedTest extends BaseIgniteAbstractTest {
    @WorkDirectory
    private Path path;

    private DefaultLogStorageFactory logStorageProvider;

    private ConfigurationManager confManager;

    private LogEntryCodecFactory logEntryCodecFactory;

    private LogStorageOptions logStorageOptions;

    @BeforeEach
    public void setUp() {
        logStorageProvider = new DefaultLogStorageFactory(this.path);

        logStorageProvider.start();

        this.confManager = new ConfigurationManager();
        this.logEntryCodecFactory = LogEntryV1CodecFactory.getInstance();
        this.logStorageOptions = newLogStorageOptions();
    }

    @AfterEach
    public void tearDown() {
        logStorageProvider.close();
    }

    @Test
    public void raftGroupsWithPrefixCollision() {
        LogStorage abcdStorage = logStorageProvider.createLogStorage("abcd", new RaftOptions());
        abcdStorage.init(logStorageOptions);

        LogStorage abStorage = logStorageProvider.createLogStorage("ab", new RaftOptions());
        abStorage.init(logStorageOptions);

        int count = 100;

        for (int i = 0; i < count; i++) {
            LogEntry abcdEntry = TestUtils.mockEntry(i, i, 1);
            abcdStorage.appendEntry(abcdEntry);

            LogEntry ab = TestUtils.mockEntry(i, i + 1000, 2);
            abStorage.appendEntry(ab);
        }

        assertEquals(0, abcdStorage.getFirstLogIndex());
        assertEquals(count - 1, abcdStorage.getLastLogIndex());

        assertEquals(0, abStorage.getFirstLogIndex());
        assertEquals(count - 1, abStorage.getLastLogIndex());

        for (int i = 0; i < count; i++) {
            LogEntry abcdEntry = abcdStorage.getEntry(i);

            assertEquals(new LogId(i, i), abcdEntry.getId());

            LogEntry abEntry = abStorage.getEntry(i);

            assertEquals(new LogId(i, i + 1000), abEntry.getId());
        }

        abStorage.reset(1);

        assertEquals(0, abcdStorage.getFirstLogIndex());
        assertEquals(count - 1, abcdStorage.getLastLogIndex());

        for (int i = 0; i < count; i++) {
            LogEntry entry = abcdStorage.getEntry(i);

            assertEquals(new LogId(i, i), entry.getId());
        }

        abcdStorage.shutdown();
        abStorage.shutdown();
    }

    @Test
    public void testCollisionWithEmptyAndNotEmptyStorage() {
        LogStorage testStorage1 = logStorageProvider.createLogStorage("test1", new RaftOptions());
        testStorage1.init(logStorageOptions);

        testStorage1.appendEntry(TestUtils.mockEntry(1, 1, 1));

        LogStorage testStorage2 = logStorageProvider.createLogStorage("test2", new RaftOptions());
        testStorage2.init(logStorageOptions);

        assertEquals(0, testStorage2.getLastLogIndex());

        testStorage1.shutdown();
        testStorage2.shutdown();
    }

    @Test
    public void testIncorrectRaftGroupName() {
        assertThrows(
                IllegalArgumentException.class,
                () -> logStorageProvider.createLogStorage("name" + ((char) 0), new RaftOptions())
        );

        assertThrows(
                IllegalArgumentException.class,
                () -> logStorageProvider.createLogStorage("name" + ((char) 1), new RaftOptions())
        );
    }

    private LogStorageOptions newLogStorageOptions() {
        LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(this.confManager);
        opts.setLogEntryCodecFactory(this.logEntryCodecFactory);
        return opts;
    }
}
