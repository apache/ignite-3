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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Random;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.EntryCountBudgetConfigurationSchema;
import org.apache.ignite.internal.raft.configuration.EntryCountBudgetView;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.DefaultLogEntryCodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDBException;

@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
class VolatileLogStorageManagerCreatorTest extends BaseIgniteAbstractTest {
    @WorkDirectory
    private Path workDir;

    @Mock
    private EntryCountBudgetView budgetCausingSpillOut;

    private final Peer peer = new Peer("127.0.0.1");

    private final LogStorageOptions logStorageOptions = new LogStorageOptions();

    private VolatileLogStorageManagerCreator managerCreator;

    @BeforeEach
    void prepare() {
        logStorageOptions.setConfigurationManager(new ConfigurationManager());
        logStorageOptions.setLogEntryCodecFactory(DefaultLogEntryCodecFactory.getInstance());

        managerCreator = new VolatileLogStorageManagerCreator("test", workDir);
        assertThat(managerCreator.startAsync(new ComponentContext()), willCompleteSuccessfully());

        when(budgetCausingSpillOut.name()).thenReturn(EntryCountBudgetConfigurationSchema.NAME);
        // No budget for storing entries in memory -> spillout will happen.
        when(budgetCausingSpillOut.entriesCountLimit()).thenReturn(0L);
    }

    @AfterEach
    void cleanup() {
        assertThat(managerCreator.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void totalBytesOnDiskReportsSize() throws Exception {
        int entrySize = 1000;

        long originalSize = managerCreator.totalBytesOnDisk();

        LogStorage logStorage = createAndInitLogStorage(new ZonePartitionId(1, 0));
        logStorage.appendEntry(dataLogEntry(1, randomBytes(new Random(), entrySize)));

        // Make sure SST files are accounted for. WAL is ignored as it is disabled in this implementation.
        flushSstFiles();
        assertThat(managerCreator.totalBytesOnDisk(), is(greaterThanOrEqualTo(originalSize + entrySize)));
    }

    private LogStorage createAndInitLogStorage(ZonePartitionId groupId) {
        LogStorageManager logStorageManager = managerCreator.manager(budgetCausingSpillOut);

        LogStorage logStorage = logStorageManager.createLogStorage(nodeIdStringForStorage(groupId), new RaftOptions());
        logStorage.init(logStorageOptions);

        return logStorage;
    }

    private String nodeIdStringForStorage(ZonePartitionId groupId) {
        return new RaftNodeId(groupId, peer).nodeIdStringForStorage();
    }

    private static LogEntry dataLogEntry(int index, byte[] content) {
        LogEntry logEntry = new LogEntry();

        logEntry.setId(new LogId(index, 1));
        logEntry.setType(EntryType.ENTRY_TYPE_DATA);
        logEntry.setData(ByteBuffer.wrap(content));

        return logEntry;
    }

    private void flushSstFiles() throws RocksDBException {
        try (var flushOptions = new FlushOptions().setWaitForFlush(true)) {
            //noinspection resource
            managerCreator.db().flush(flushOptions);
        }
    }
}
