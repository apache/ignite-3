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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.codec.DefaultLogEntryCodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class DefaultLogStorageManagerTest {
    @WorkDirectory
    private Path workDir;

    private DefaultLogStorageManager logStorageManager;

    private final LogStorageOptions logStorageOptions = new LogStorageOptions();

    private final Peer peer = new Peer("127.0.0.1");

    @BeforeEach
    void setUp() {
        logStorageOptions.setConfigurationManager(new ConfigurationManager());
        logStorageOptions.setLogEntryCodecFactory(DefaultLogEntryCodecFactory.getInstance());

        logStorageManager = new DefaultLogStorageManager(workDir);

        startFactory();
    }

    private void startFactory() {
        assertThat(logStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        if (logStorageManager != null) {
            stopFactory();
        }
    }

    private void stopFactory() {
        assertThat(logStorageManager.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void metadataMigrationFindsGroupsHavingOnlyConfigurationEntries() throws Exception {
        ZonePartitionId groupId1 = new ZonePartitionId(1, 0);
        ZonePartitionId groupId3 = new ZonePartitionId(3, 2);
        LogStorage logStorage1 = createAndInitLogStorage(groupId1);
        LogStorage logStorage3 = createAndInitLogStorage(groupId3);

        logStorage1.appendEntry(configLogEntry(1));
        logStorage3.appendEntry(configLogEntry(10));

        Set<String> ids = logStorageManager.metadataMigration().raftNodeStorageIdsOnDisk();

        assertThat(
                ids,
                containsInAnyOrder(
                        nodeIdStringForStorage(groupId1),
                        nodeIdStringForStorage(groupId3)
                )
        );
    }

    private LogStorage createAndInitLogStorage(ZonePartitionId groupId) {
        LogStorage logStorage = logStorageManager.createLogStorage(nodeIdStringForStorage(groupId), new RaftOptions());
        logStorage.init(logStorageOptions);
        return logStorage;
    }

    private String nodeIdStringForStorage(ZonePartitionId groupId) {
        return new RaftNodeId(groupId, peer).nodeIdStringForStorage();
    }

    private static LogEntry configLogEntry(int index) {
        LogEntry logEntry = new LogEntry();

        logEntry.setId(new LogId(index, 1));
        logEntry.setType(EntryType.ENTRY_TYPE_CONFIGURATION);
        logEntry.setPeers(List.of(new PeerId("a")));

        return logEntry;
    }

    @Test
    void metadataMigrationFindsGroupsHavingOnlyDataEntries() throws Exception {
        ZonePartitionId groupId1 = new ZonePartitionId(1, 0);
        ZonePartitionId groupId3 = new ZonePartitionId(3, 2);
        LogStorage logStorage1 = createAndInitLogStorage(groupId1);
        LogStorage logStorage3 = createAndInitLogStorage(groupId3);

        logStorage1.appendEntry(dataLogEntry(1));
        logStorage3.appendEntry(dataLogEntry(10));

        Set<String> ids = logStorageManager.metadataMigration().raftNodeStorageIdsOnDisk();

        assertThat(
                ids,
                containsInAnyOrder(
                        nodeIdStringForStorage(groupId1),
                        nodeIdStringForStorage(groupId3)
                )
        );
    }

    private static LogEntry dataLogEntry(int index) {
        LogEntry logEntry = new LogEntry();

        logEntry.setId(new LogId(index, 1));
        logEntry.setType(EntryType.ENTRY_TYPE_DATA);
        logEntry.setData(ByteBuffer.wrap(new byte[0]));

        return logEntry;
    }

    @Test
    void metadataMigrationFindsGroupsHavingBothConfigurationAndDataEntries() throws Exception {
        ZonePartitionId groupId1 = new ZonePartitionId(1, 0);
        ZonePartitionId groupId3 = new ZonePartitionId(3, 2);
        LogStorage logStorage1 = createAndInitLogStorage(groupId1);
        LogStorage logStorage3 = createAndInitLogStorage(groupId3);

        logStorage1.appendEntry(configLogEntry(1));
        logStorage1.appendEntry(dataLogEntry(2));
        logStorage3.appendEntry(configLogEntry(10));
        logStorage3.appendEntry(dataLogEntry(11));

        Set<String> ids = logStorageManager.metadataMigration().raftNodeStorageIdsOnDisk();

        assertThat(
                ids,
                containsInAnyOrder(
                        nodeIdStringForStorage(groupId1),
                        nodeIdStringForStorage(groupId3)
                )
        );
    }

    @Test
    void metadataMigrationSavesStorageCreatedFlagInMetaColumnFamily() throws Exception {
        ZonePartitionId groupId1 = new ZonePartitionId(1, 0);
        ZonePartitionId groupId3 = new ZonePartitionId(3, 2);
        LogStorage logStorage1 = createAndInitLogStorage(groupId1);
        LogStorage logStorage3 = createAndInitLogStorage(groupId3);

        logStorage1.appendEntry(configLogEntry(10));
        logStorage1.appendEntry(dataLogEntry(11));
        logStorage3.appendEntry(configLogEntry(100));
        logStorage3.appendEntry(dataLogEntry(101));

        logStorageManager.db().dropColumnFamily(logStorageManager.metaColumnFamilyHandle());
        logStorageManager.db().destroyColumnFamilyHandle(logStorageManager.metaColumnFamilyHandle());

        // Restart causes a migration.
        stopFactory();
        startFactory();

        assertThat(
                logStorageManager.db().get(logStorageManager.metaColumnFamilyHandle(), storageCreatedKey(groupId1)),
                is(new byte[0])
        );
        assertThat(
                logStorageManager.db().get(logStorageManager.metaColumnFamilyHandle(), storageCreatedKey(groupId3)),
                is(new byte[0])
        );
    }

    private byte[] storageCreatedKey(ZonePartitionId groupId) {
        return ("\u0001" + nodeIdStringForStorage(groupId)).getBytes(UTF_8);
    }

    private byte[] entryKey(ZonePartitionId groupId, long index) {
        return RocksDbSharedLogStorage.createKey(
                (nodeIdStringForStorage(groupId) + "\0").getBytes(UTF_8),
                ByteUtils.longToBytes(index)
        );
    }

    @Test
    void storageInitSavesStorageCreatedFlagInMetaColumnFamily() throws Exception {
        ZonePartitionId groupId1 = new ZonePartitionId(1, 0);
        ZonePartitionId groupId3 = new ZonePartitionId(3, 2);
        createAndInitLogStorage(groupId1);
        createAndInitLogStorage(groupId3);

        assertThat(
                logStorageManager.db().get(logStorageManager.metaColumnFamilyHandle(), storageCreatedKey(groupId1)),
                is(new byte[0])
        );
        assertThat(
                logStorageManager.db().get(logStorageManager.metaColumnFamilyHandle(), storageCreatedKey(groupId3)),
                is(new byte[0])
        );
    }

    @Test
    void storageDestructionRemovesAllItsKes() throws Exception {
        ZonePartitionId groupId = new ZonePartitionId(3, 2);
        LogStorage logStorage = createAndInitLogStorage(groupId);

        logStorage.appendEntry(configLogEntry(100));

        assertThat(
                logStorageManager.db().get(logStorageManager.metaColumnFamilyHandle(), storageCreatedKey(groupId)),
                is(new byte[0])
        );
        assertThat(
                logStorageManager.db().get(logStorageManager.confColumnFamilyHandle(), entryKey(groupId, 100)),
                is(notNullValue())
        );
        assertThat(
                logStorageManager.db().get(logStorageManager.dataColumnFamilyHandle(), entryKey(groupId, 100)),
                is(notNullValue())
        );

        logStorageManager.destroyLogStorage(nodeIdStringForStorage(groupId));

        assertThat(
                logStorageManager.db().get(logStorageManager.metaColumnFamilyHandle(), storageCreatedKey(groupId)),
                is(nullValue())
        );
        assertThat(
                logStorageManager.db().get(logStorageManager.confColumnFamilyHandle(), entryKey(groupId, 100)),
                is(nullValue())
        );
        assertThat(
                logStorageManager.db().get(logStorageManager.dataColumnFamilyHandle(), entryKey(groupId, 100)),
                is(nullValue())
        );
    }

    @Test
    void groupsScanFindsGroups() {
        ZonePartitionId groupId1 = new ZonePartitionId(1, 0);
        ZonePartitionId groupId3 = new ZonePartitionId(3, 2);
        createAndInitLogStorage(groupId1);
        createAndInitLogStorage(groupId3);

        Set<String> ids = logStorageManager.raftNodeStorageIdsOnDisk();

        assertThat(
                ids,
                containsInAnyOrder(
                        new RaftNodeId(groupId1, peer).nodeIdStringForStorage(),
                        new RaftNodeId(groupId3, peer).nodeIdStringForStorage()
                )
        );
    }
}
