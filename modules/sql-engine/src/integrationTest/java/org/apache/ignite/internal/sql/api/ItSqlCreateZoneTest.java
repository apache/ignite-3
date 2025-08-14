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

package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.cluster.management.network.messages.ClusterStateMessage;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommandImpl;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.server.WatchListenerInhibitor;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.message.InvokeRequest;
import org.apache.ignite.internal.network.message.ScaleCubeMessage;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.table.distributed.schema.ThreadLocalPartitionCommandsMarshaller;
import org.apache.ignite.raft.jraft.rpc.AppendEntriesRequestImpl;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequestImpl;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

//@Timeout(20)
class ItSqlCreateZoneTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_MANE = "test_zone";
    private static final String NOT_EXISTED_PROFILE_NAME = "not-existed-profile";
    private static final String EXTRA_PROFILE_NAME = "extra-profile";
    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_TEST_PROFILE_NAME + ".engine: test, "
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + EXTRA_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnSameNode() {
        assertDoesNotThrow(() -> createZoneQuery(0, "default"));
    }

    Set<Class<? extends NetworkMessage>> allMessages = ConcurrentHashMap.newKeySet();

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnDifferentNode() {
        cluster.startNode(1, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE);
        assertDoesNotThrow(() -> createZoneQuery(0, EXTRA_PROFILE_NAME));
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnDifferentNodeWithDistributedLogicalTopologyUpdate() throws InterruptedException {
        // Node 0 is CMG leader and Node 1 is a laggy query executor
        IgniteImpl node0 = unwrapIgniteImpl(node(0));
        IgniteImpl node1 = unwrapIgniteImpl(cluster.startNode(1));

        Marshaller raftMarshaller = new ThreadLocalPartitionCommandsMarshaller(node0.clusterService().serializationRegistry());

        assertTrue(waitForCondition(
                () -> node1.logicalTopologyService().localLogicalTopology().nodes().size() == 2,
                10_000
        ));

        cluster.transferLeadershipTo(0, CmgGroupId.INSTANCE);
        cluster.transferLeadershipTo(0, MetastorageGroupId.INSTANCE);

        assertThrowsWithCause(
                () -> createZoneQuery(1, EXTRA_PROFILE_NAME),
                SqlException.class,
                "Some storage profiles don't exist [missedProfileNames=[" + EXTRA_PROFILE_NAME + "]]."
        );

        // Node 1 won't see Node 2 joined with extra profile
        node0.dropMessages((recipient, msg) -> {
            if (node1.name().equals(recipient)) {
            }

            if (msg instanceof WriteActionRequestImpl) {
                var waReq = (WriteActionRequestImpl) msg;

                var writeCommand = waReq.deserializedCommand();

                allMessages.add(writeCommand.getClass());


                if (writeCommand instanceof JoinReadyCommandImpl) {
                    var joinCommand = (JoinReadyCommandImpl) writeCommand;

                    return true;
                }
            }

            return node1.name().equals(recipient) && !(msg instanceof ScaleCubeMessage);
        });

        node1.dropIncomingMessages((sender, msg) -> {
            if (node1.name().equals(sender)) {
                allMessages.add(msg.getClass());
            }

            if (msg instanceof WriteActionRequestImpl) {
                var waReq = (WriteActionRequestImpl) msg;

                var writeCommand = waReq.deserializedCommand();

                allMessages.add(writeCommand.getClass());


                if (writeCommand instanceof JoinReadyCommandImpl) {
                    var joinCommand = (JoinReadyCommandImpl) writeCommand;

                    return true;
                }
            }

            if (msg instanceof InvokeRequest) {
                var innerMsg = ((InvokeRequest) msg).message();
                allMessages.add(innerMsg.getClass());
                if (innerMsg instanceof AppendEntriesRequestImpl) {
                    var list = ((AppendEntriesRequestImpl) innerMsg).entriesList();
                }
            }

            if (msg instanceof AppendEntriesRequestImpl) {
                var list = ((AppendEntriesRequestImpl) msg).entriesList();
            }

            return !(msg instanceof ScaleCubeMessage);
        });



        cluster.startNode(2, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE);

        assertTrue(waitForCondition(
                () -> unwrapIgniteImpl(node(0)).logicalTopologyService().localLogicalTopology().nodes().size() == 3,
                10_000
        ));

        assertTrue(waitForCondition(
                () -> unwrapIgniteImpl(node(2)).logicalTopologyService().localLogicalTopology().nodes().size() == 3,
                10_000
        ));

        assertEquals(2, node1.logicalTopologyService().localLogicalTopology().nodes().size());


        assertThrowsWithCause(
                () -> createZoneQuery(1, EXTRA_PROFILE_NAME),
                SqlException.class,
                "Storage profile " + EXTRA_PROFILE_NAME + " doesn't exist in local topology snapshot with profiles"
        );

        assertTrue(waitForCondition(
             () -> node1.logicalTopologyService().localLogicalTopology().nodes().size() == 3,
             10_000
        ));

         assertDoesNotThrow(() -> createZoneQuery(1, EXTRA_PROFILE_NAME));
    }

    @AfterEach
    void log() {
        Loggers.forClass(ItSqlCreateZoneTest.class).info("!! All messages: {}", allMessages);
    }

    @Test
    void testCreateZoneFailedWithoutCorrectStorageProfileInCluster() {
        assertThrowsWithCode(
                SqlException.class,
                STMT_VALIDATION_ERR,
                () -> createZoneQuery(0, NOT_EXISTED_PROFILE_NAME),
                "Some storage profiles don't exist [missedProfileNames=[" + NOT_EXISTED_PROFILE_NAME + "]]."
        );
    }

    private  List<List<Object>> createZoneQuery(int nodeIdx, String storageProfile) {
        return executeSql(nodeIdx, format("CREATE ZONE IF NOT EXISTS {} STORAGE PROFILES ['{}']", ZONE_MANE, storageProfile));
    }
}
