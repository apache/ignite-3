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

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.parser.ConfigDocumentFactory;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.junit.jupiter.api.Test;

/** Class for testing various raft log truncation and rebalancing scenarios. */
public class ItTruncateRaftLogAndRebalanceTest extends BaseTruncateRaftLogAbstractTest {
    private static final int RAFT_SNAPSHOT_INTERVAL_SECS = 5;

    @Override
    protected int initialNodes() {
        return 3;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return ConfigDocumentFactory.parseString(super.getNodeBootstrapConfigTemplate())
                .withValueText("ignite.system.properties.raftSnapshotIntervalSecs", Integer.toString(RAFT_SNAPSHOT_INTERVAL_SECS))
                .render();
    }

    @Test
    void test() throws Exception {
        createZoneAndTablePerson(ZONE_NAME, TABLE_NAME, 3, 1);

        ReplicationGroupId replicationGroupId = cluster.solePartitionId(ZONE_NAME, TABLE_NAME);

        cluster.transferLeadershipTo(0, replicationGroupId);

        insertPeopleAndAwaitTruncateRaftLogOnAllNodes(replicationGroupId);
    }

    private void insertPeopleAndAwaitTruncateRaftLogOnAllNodes(ReplicationGroupId replicationGroupId) {
        long[] beforeInsertPeopleRaftFirstLogIndexes = collectRaftFirstLogIndexes(replicationGroupId);
        assertEquals(initialNodes(), beforeInsertPeopleRaftFirstLogIndexes.length);

        insertPeople(TABLE_NAME, generatePeople(1_000));

        await().atMost(RAFT_SNAPSHOT_INTERVAL_SECS * 2, TimeUnit.SECONDS).until(() -> {
            long[] raftFirstLogIndexes = collectRaftFirstLogIndexes(replicationGroupId);
            assertEquals(initialNodes(), raftFirstLogIndexes.length);

            return !Arrays.equals(beforeInsertPeopleRaftFirstLogIndexes, raftFirstLogIndexes);
        });
    }

    private long[] collectRaftFirstLogIndexes(ReplicationGroupId replicationGroupId) {
        return cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .map(igniteImpl -> raftNodeImpl(igniteImpl, replicationGroupId))
                .mapToLong(raftNodeImpl -> raftNodeImpl.logStorage().getFirstLogIndex())
                .toArray();
    }
}
