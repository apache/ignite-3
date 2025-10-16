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

package org.apache.ignite.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.function.Predicate;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageFactory;
import org.apache.ignite.internal.raft.storage.impl.IgniteJraftServiceFactory;
import org.apache.ignite.internal.replicator.PartitionGroupId;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.rocksdb.DBOptions;
import org.rocksdb.WriteOptions;

class ItRaftFsyncOptionTest extends ClusterPerTestIntegrationTest {
    @Override
    protected int initialNodes() {
        return 0;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void fsyncOptionOnlyAffectsPartitions(boolean fsyncInConfig) {
        cluster.startAndInit(1, "ignite.raft.fsync = " + fsyncInConfig, paramsBuilder -> {});

        node(0).sql().executeScript("CREATE TABLE TEST (id INT PRIMARY KEY, val VARCHAR)");

        RaftNodeId cmgNodeId = findRaftNodeId(id -> id.groupId() == CmgGroupId.INSTANCE);
        assertFsyncIsAsExpectedOn(cmgNodeId, true);

        RaftNodeId metastorageNodeId = findRaftNodeId(id -> id.groupId() == MetastorageGroupId.INSTANCE);
        assertFsyncIsAsExpectedOn(metastorageNodeId, true);

        RaftNodeId partitionNodeId = findRaftNodeId(id -> id.groupId() instanceof PartitionGroupId);
        assertFsyncIsAsExpectedOn(partitionNodeId, fsyncInConfig);
    }

    private RaftNodeId findRaftNodeId(Predicate<RaftNodeId> raftNodeIdMatches) {
        return raftServer().localNodes().stream()
                .filter(raftNodeIdMatches)
                .findAny()
                .orElseThrow();
    }

    private JraftServerImpl raftServer() {
        return (JraftServerImpl) igniteImpl(0).raftManager().server();
    }

    private void assertFsyncIsAsExpectedOn(RaftNodeId raftNodeId, boolean expectedFsync) {
        IgniteJraftServiceFactory cmgServiceFactory = (IgniteJraftServiceFactory) raftServer()
                .raftGroupService(raftNodeId)
                .getNodeOptions()
                .getServiceFactory();
        DefaultLogStorageFactory logStorageFactory = (DefaultLogStorageFactory) cmgServiceFactory.logStorageFactory();

        DBOptions dbOptions = logStorageFactory.dbOptions();
        assertNotNull(dbOptions);
        assertEquals(expectedFsync, dbOptions.useFsync());

        WriteOptions writeOptions = logStorageFactory.writeOptions();
        assertNotNull(writeOptions);
        assertEquals(expectedFsync, writeOptions.sync());
    }
}
