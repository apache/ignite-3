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
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.apache.ignite.table.QualifiedName.DEFAULT_SCHEMA_NAME;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
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
        assertDoesNotThrow(() -> createZoneQuery(0, DEFAULT_STORAGE_PROFILE));
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnDifferentNode() {
        cluster.startNode(1, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE);
        assertDoesNotThrow(() -> createZoneQuery(0, EXTRA_PROFILE_NAME));
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnDifferentNodeWithDistributedLogicalTopologyUpdate() throws InterruptedException {
        // Node 0 is CMG leader and Node 1 is a laggy query executor.
        IgniteImpl node0 = unwrapIgniteImpl(node(0));
        IgniteImpl node1 = unwrapIgniteImpl(cluster.startNode(1));

        assertTrue(waitForCondition(
                () -> node1.logicalTopologyService().localLogicalTopology().nodes().size() == 2,
                10_000
        ));

        // Assert that we can't create the zone without a node with extra profile.
        assertThrowsWithCause(
                () -> createZoneQuery(1, EXTRA_PROFILE_NAME),
                SqlException.class,
                "Some storage profiles don't exist [missedProfileNames=[" + EXTRA_PROFILE_NAME + "]]."
        );

        // Node 1 won't see node 2 joined with extra profile because node 0 is CMG leader and all CMG-related RAFT-replicated messages to
        // node 1 will be dropped after the code below.
        node0.dropMessages((recipient, msg) -> msg instanceof AppendEntriesRequest
                && ((AppendEntriesRequest) msg).groupId().equals(CmgGroupId.INSTANCE.toString())
                && node1.name().equals(recipient));

        // Then start node 2 with the desired extra profile.
        cluster.startNode(2, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE);

        // Check that Node 0 and 2 will see all three nodes in local logical topologies.
        assertTrue(waitForCondition(
                () -> unwrapIgniteImpl(node(0)).logicalTopologyService().localLogicalTopology().nodes().size() == 3,
                10_000
        ));

        assertTrue(waitForCondition(
                () -> unwrapIgniteImpl(node(2)).logicalTopologyService().localLogicalTopology().nodes().size() == 3,
                10_000
        ));

        // And we expect that node 1 won't see node 2 in its local logical topology.
        assertEquals(2, node1.logicalTopologyService().localLogicalTopology().nodes().size());

        // But still we're able to create zone with extra profile on node 2 because node 1 will try to ask CMG leader (node 0) directly over
        // the network for its up-to-date leader's local logical topology and check this snapshot's storage profiles that should
        // extra profile because 2nd node was accepted to cluster by node 0 because it's the only CMG group voting member.
        assertDoesNotThrow(() -> createZoneQuery(1, EXTRA_PROFILE_NAME));
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

    @Test
    void testCreateZoneFailsIfFilterIsInvalid() {
        String filter = "$[?(@.region == \"non-existing\")]";

        assertThrowsWithCode(
                SqlException.class,
                STMT_VALIDATION_ERR,
                () -> createZoneQueryWithFilter(0, filter),
                format("Node filter does not match any node in the cluster [filter='{}'].", filter)
        );
    }

    @Test
    void testCreateDefaultZoneLazily() {
        IgniteImpl node = unwrapIgniteImpl(node(0));

        CatalogManager catalogManager = node.catalogManager();
        assertNull(catalogManager.catalog(catalogManager.latestCatalogVersion()).defaultZone());

        assertDoesNotThrow(() -> createZoneQuery(0, DEFAULT_STORAGE_PROFILE));

        assertDoesNotThrow(() -> createTableQuery(0, "test_table", ZONE_MANE));

        String testTableWithoutZoneName = "test_table_without_zone";
        assertDoesNotThrow(() -> createTableWithoutZoneQuery(0, testTableWithoutZoneName));

        CatalogZoneDescriptor defaultZoneDesc = catalogManager.catalog(catalogManager.latestCatalogVersion()).defaultZone();
        assertNotNull(defaultZoneDesc);

        CatalogTableDescriptor tableWithDefaultZoneDescriptor = catalogManager
                .catalog(catalogManager.latestCatalogVersion())
                .table(DEFAULT_SCHEMA_NAME, testTableWithoutZoneName);
        assertNotNull(tableWithDefaultZoneDescriptor);
        assertEquals(defaultZoneDesc.id(), tableWithDefaultZoneDescriptor.zoneId());
    }

    private List<List<Object>> createZoneQuery(int nodeIdx, String storageProfile) {
        return executeSql(nodeIdx, format("CREATE ZONE IF NOT EXISTS {} STORAGE PROFILES ['{}']", ZONE_MANE, storageProfile));
    }

    private List<List<Object>> createZoneQueryWithFilter(int nodeIdx, String filter) {
        return executeSql(nodeIdx, format("CREATE ZONE IF NOT EXISTS {} (NODES FILTER '{}') "
                + "STORAGE PROFILES ['{}']", ZONE_MANE, filter, DEFAULT_STORAGE_PROFILE));
    }

    private List<List<Object>> createTableQuery(int nodeIdx, String tableName, String  zoneName) {
        return executeSql(
                nodeIdx,
                format("CREATE TABLE IF NOT EXISTS \"{}\" (key int primary key, val varchar) ZONE {}", tableName, zoneName)
        );
    }

    private List<List<Object>> createTableWithoutZoneQuery(int nodeIdx, String tableName) {
        return executeSql(nodeIdx, format("CREATE TABLE IF NOT EXISTS \"{}\" (key int primary key, val varchar)", tableName));
    }
}
