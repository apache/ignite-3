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

package org.apache.ignite.internal.distributionzones;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestRebalanceUtil.pendingPartitionAssignmentsKey;
import static org.apache.ignite.internal.TestRebalanceUtil.stablePartitionAssignmentsKey;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.assertValueInStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.deserializeLatestDataNodesHistoryEntry;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.distributionzones.DataNodesHistory.DataNodesHistorySerializer;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.SqlException;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

/**
 * Integration test for data nodes' filters functionality.
 */
public class ItDistributionZonesFiltersTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "table1";

    @Language("HOCON")
    private static final String NODE_ATTRIBUTES = "{region = US, storage = SSD}";

    private static final String STORAGE_PROFILES =
            String.format("'%s', '%s'", DEFAULT_ROCKSDB_PROFILE_NAME, DEFAULT_AIPERSIST_PROFILE_NAME);

    @Language("HOCON")
    private static final String STORAGE_PROFILES_CONFIGS = String.format(
            "{%s.engine = rocksdb, %s.engine = aipersist}",
            DEFAULT_ROCKSDB_PROFILE_NAME,
            DEFAULT_AIPERSIST_PROFILE_NAME
    );

    private static final String COLUMN_KEY = "key";

    private static final String COLUMN_VAL = "val";

    private static final int TIMEOUT_MILLIS = 10_000;

    @Language("HOCON")
    private static String createStartConfig(@Language("HOCON") String nodeAttributes, @Language("HOCON") String storageProfiles) {
        return "ignite {\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder.netClusterNodes: [ {} ]\n"
                + "  },"
                + "  nodeAttributes.nodeAttributes: " + nodeAttributes + ",\n"
                + "  storage.profiles: " + storageProfiles + ",\n"
                + "  clientConnector.port: {},\n"
                + "  rest.port: {},\n"
                + "  failureHandler.dumpThreadsOnFailure: false\n"
                + "}";
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return createStartConfig(NODE_ATTRIBUTES, STORAGE_PROFILES_CONFIGS);
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    /**
     * Tests the scenario when filter was applied to data nodes and stable key for rebalance was changed to that filtered value.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21387")
    @ParameterizedTest
    @EnumSource(ConsistencyMode.class)
    void testFilteredDataNodesPropagatedToStable(ConsistencyMode consistencyMode) throws Exception {
        String filter = "$[?(@.region == \"US\" && @.storage == \"SSD\")]";

        // This node do not pass the filter
        @Language("HOCON") String firstNodeAttributes = "{region: EU, storage: SSD}";

        IgniteImpl node = unwrapIgniteImpl(startNode(1, createStartConfig(firstNodeAttributes, STORAGE_PROFILES_CONFIGS)));

        node.sql().execute(
                null,
                createZoneSql(2, 3, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, filter, STORAGE_PROFILES, consistencyMode)
        );

        node.sql().execute(null, createTableSql());

        MetaStorageManager metaStorageManager = node.metaStorageManager();

        TableViewInternal table = unwrapTableImpl(node.distributedTableManager().table(TABLE_NAME));

        ZonePartitionId partId = new ZonePartitionId(table.zoneId(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartitionAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes().size(),
                1,
                TIMEOUT_MILLIS
        );

        @Language("HOCON") String secondNodeAttributes = "{region: US, storage: SSD}";

        // This node pass the filter but storage profiles of a node do not match zone's storage profiles.
        // TODO: https://issues.apache.org/jira/browse/IGNITE-21387 recovery of this node is failing,
        // TODO: because there are no appropriate storage profile on the node
        @Language("HOCON") String notMatchingProfiles = "{dummy:{engine:\"dummy\"},another_dummy:{engine:\"dummy\"}}";
        startNode(2, createStartConfig(secondNodeAttributes, notMatchingProfiles));

        // This node pass the filter and storage profiles of a node match zone's storage profiles.
        startNode(3, createStartConfig(secondNodeAttributes, STORAGE_PROFILES_CONFIGS));

        int zoneId = getZoneId(node);

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesHistoryKey(zoneId),
                DataNodesHistorySerializer::deserialize,
                4,
                TIMEOUT_MILLIS
        );

        Entry dataNodesEntry1 = metaStorageManager.get(zoneDataNodesHistoryKey(zoneId)).get(5_000, MILLISECONDS);

        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= dataNodesEntry1.revision(), TIMEOUT_MILLIS));

        // We check that two nodes that pass the filter and storage profiles are presented in the stable key.
        assertValueInStorage(
                metaStorageManager,
                stablePartitionAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name(), node(3).name()),
                TIMEOUT_MILLIS * 2
        );
    }

    /**
     * Tests the scenario when altering filter triggers immediate scale up so data nodes
     * and stable key for rebalance is changed to the new value even if scale up timer is big enough.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource(ConsistencyMode.class)
    void testAlteringFiltersPropagatedDataNodesToStableImmediately(ConsistencyMode consistencyMode) throws Exception {
        String filter = "$[?(@.region == \"US\" && @.storage == \"SSD\")]";

        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        node0.sql().execute(
                null,
                createZoneSql(2, 3, 10_000, 10_000, filter, STORAGE_PROFILES, consistencyMode)
        );

        node0.sql().execute(null, createTableSql());

        MetaStorageManager metaStorageManager = node0.metaStorageManager();

        TableViewInternal table = unwrapTableViewInternal(node0.distributedTableManager().table(TABLE_NAME));

        ZonePartitionId partId = new ZonePartitionId(table.zoneId(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartitionAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name()),
                TIMEOUT_MILLIS
        );

        @Language("HOCON") String firstNodeAttributes = "{region: US, storage: SSD}";

        // This node pass the filter
        startNode(1, createStartConfig(firstNodeAttributes, STORAGE_PROFILES_CONFIGS));

        // Expected size is 1 because we have timers equals to 10000, so no scale up will be propagated.
        waitDataNodeAndListenersAreHandled(metaStorageManager, 1, getZoneId(node0));

        node0.sql().execute(null, alterZoneSql(DEFAULT_FILTER));

        // We check that all nodes that pass the filter are presented in the stable key because altering filter triggers immediate scale up.
        assertValueInStorage(
                metaStorageManager,
                stablePartitionAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name(), node(1).name()),
                TIMEOUT_MILLIS * 2
        );
    }

    /**
     * Tests the scenario when empty data nodes are not propagated to stable after filter is altered, because there are no node that
     * matches the new filter.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource(ConsistencyMode.class)
    void testEmptyDataNodesDoNotPropagatedToStableAfterAlteringFilter(ConsistencyMode consistencyMode) throws Exception {
        String filter = "$[?(@.region == \"US\" && @.storage == \"SSD\")]";

        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        node0.sql().execute(
                null,
                createZoneSql(2, 3, 10_000, 10_000, filter, STORAGE_PROFILES, consistencyMode)
        );

        node0.sql().execute(null, createTableSql());

        MetaStorageManager metaStorageManager = node0.metaStorageManager();

        TableViewInternal table = unwrapTableViewInternal(node0.distributedTableManager().table(TABLE_NAME));

        ZonePartitionId partId = new ZonePartitionId(table.zoneId(), 0);

        assertValueInStorage(
                metaStorageManager,
                stablePartitionAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name()),
                TIMEOUT_MILLIS
        );

        @Language("HOCON") String firstNodeAttributes = "{region: US, storage: SSD}";

        // This node pass the filter
        startNode(1, createStartConfig(firstNodeAttributes, STORAGE_PROFILES_CONFIGS));

        int zoneId = getZoneId(node0);

        // Expected size is 2 because we have timers equals to 10000, so no scale up will be propagated.
        waitDataNodeAndListenersAreHandled(metaStorageManager, 1, zoneId);

        // There is no node that match the filter
        String newFilter = "$[?(@.region == \"FOO\" && @.storage == \"BAR\")]";

        assertThrowsWithCode(SqlException.class, STMT_VALIDATION_ERR, () -> node0.sql().execute(null, alterZoneSql(newFilter)), null);

        assertValueInStorage(
                metaStorageManager,
                stablePartitionAssignmentsKey(partId),
                (v) -> Assignments.fromBytes(v).nodes()
                        .stream().map(Assignment::consistentId).collect(Collectors.toSet()),
                Set.of(node(0).name()),
                TIMEOUT_MILLIS
        );
    }

    /**
     * Tests the scenario when removal node from the logical topology leads to empty data nodes, but this empty data nodes do not trigger
     * rebalance. Data nodes become empty after applying corresponding filter.
     *
     * @throws Exception If failed.
     */
    @ParameterizedTest
    @EnumSource(ConsistencyMode.class)
    void testFilteredEmptyDataNodesDoNotTriggerRebalance(ConsistencyMode consistencyMode) throws Exception {
        String filter = "$[?(@.region == \"EU\" && @.storage == \"HDD\")]";

        // This node do not pass the filter.
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        // This node passes the filter
        @Language("HOCON") String firstNodeAttributes = "{region: EU, storage: HDD}";

        Ignite node1 = startNode(1, createStartConfig(firstNodeAttributes, STORAGE_PROFILES_CONFIGS));

        node1.sql().execute(
                null,
                createZoneSql(1, 1, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, filter, STORAGE_PROFILES, consistencyMode)
        );

        MetaStorageManager metaStorageManager = IgniteTestUtils.getFieldValue(
                node0,
                IgniteImpl.class,
                "metaStorageMgr"
        );

        int zoneId = getZoneId(node0);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 1, zoneId);

        node1.sql().execute(null, createTableSql());

        TableViewInternal table = unwrapTableViewInternal(node0.distributedTableManager().table(TABLE_NAME));

        ZonePartitionId partId = new ZonePartitionId(table.zoneId(), 0);

        // Table was created after both nodes was up, so there wasn't any rebalance.
        assertPendingAssignmentsNeverExisted(metaStorageManager, partId);

        // Stop node, that was only one, that passed the filter, so data nodes after filtering will be empty.
        stopNode(1);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 0, zoneId);

        // Check that pending are null, so there wasn't any rebalance.
        assertPendingAssignmentsNeverExisted(metaStorageManager, partId);
    }

    @ParameterizedTest
    @EnumSource(ConsistencyMode.class)
    void testFilteredEmptyDataNodesDoNotTriggerRebalanceOnReplicaUpdate(ConsistencyMode consistencyMode) throws Exception {
        String filter = "$[?(@.region == \"EU\" && @.storage == \"HDD\")]";

        // This node do not pass the filter.
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        // This node passes the filter
        @Language("HOCON") String firstNodeAttributes = "{region: EU, storage: HDD}";

        startNode(1, createStartConfig(firstNodeAttributes, STORAGE_PROFILES_CONFIGS));

        node0.sql().execute(
                null,
                createZoneSql(1, 1, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, filter, STORAGE_PROFILES, consistencyMode)
        );

        MetaStorageManager metaStorageManager = IgniteTestUtils.getFieldValue(
                unwrapIgniteImpl(node0),
                IgniteImpl.class,
                "metaStorageMgr"
        );

        int zoneId = getZoneId(node0);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 1, zoneId);

        node0.sql().execute(null, createTableSql());

        TableViewInternal table = unwrapTableViewInternal(node0.distributedTableManager().table(TABLE_NAME));

        ZonePartitionId partId = new ZonePartitionId(table.zoneId(), 0);

        // Table was created after both nodes was up, so there wasn't any rebalance.
        assertPendingAssignmentsNeverExisted(metaStorageManager, partId);

        // Stop node, that was only one, that passed the filter, so data nodes after filtering will be empty.
        stopNode(1);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 0, zoneId);

        // Check that stable and pending are null, so there wasn't any rebalance.
        assertPendingAssignmentsNeverExisted(metaStorageManager, partId);

        node0.sql().execute(null, alterZoneSql(2));

        CountDownLatch latch = new CountDownLatch(1);

        // We need to be sure, that the first asynchronous catalog change of replica was handled,
        // so we create a listener with a latch, and change replica again and wait for latch, so we can be sure that the first
        // replica was handled.
        unwrapIgniteImpl(node0).catalogManager().listen(CatalogEvent.ZONE_ALTER, parameters -> {
            latch.countDown();

            return falseCompletedFuture();
        });

        node0.sql().execute(null, alterZoneSql(3));

        assertTrue(latch.await(10_000, MILLISECONDS));

        // Check that stable and pending are null, so there wasn't any rebalance.
        assertPendingAssignmentsNeverExisted(metaStorageManager, partId);
    }

    @CartesianTest
    @CartesianTest.MethodFactory("testEmptyDataNodesAlterZoneParamFactory")
    public void testAlterZoneWhenDataNodesAreAlreadyEmpty(ConsistencyMode consistencyMode, String alterZoneSet) throws Exception {
        String filter = "$[?(@.region == \"EU\" && @.storage == \"HDD\")]";

        // This node do not pass the filter.
        IgniteImpl node0 = unwrapIgniteImpl(node(0));

        // This node passes the filter
        startNode(1, createStartConfig("{region: EU, storage: HDD}", STORAGE_PROFILES_CONFIGS));

        executeSql(node0, createZoneSql(1, 5, IMMEDIATE_TIMER_VALUE, IMMEDIATE_TIMER_VALUE, filter, STORAGE_PROFILES, consistencyMode));
        executeSql(createTableSql());

        MetaStorageManager metaStorageManager = unwrapIgniteImpl(node0).metaStorageManager();
        int zoneId = getZoneId(node0);
        TableViewInternal table = unwrapTableViewInternal(node0.distributedTableManager().table(TABLE_NAME));
        ZonePartitionId partId = new ZonePartitionId(table.zoneId(), 0);

        stopNode(1);

        waitDataNodeAndListenersAreHandled(metaStorageManager, 0, zoneId);
        assertPendingAssignmentsNeverExisted(metaStorageManager, partId);

        CountDownLatch latch = new CountDownLatch(1);

        // We need to be sure, that the first asynchronous catalog change of replica was handled,
        // so we create a listener with a latch, and change replica again and wait for latch, so we can be sure that the first
        // replica was handled.
        node0.catalogManager().listen(CatalogEvent.ZONE_ALTER, parameters -> {
            latch.countDown();

            return falseCompletedFuture();
        });

        executeSql(node0, alterZoneSetSql(alterZoneSet));

        assertTrue(latch.await(10_000, MILLISECONDS));

        assertPendingAssignmentsNeverExisted(metaStorageManager, partId);
        assertStableAssignmentsAreNotEmpty(metaStorageManager, partId);
    }

    @SuppressWarnings("unused")
    private static ArgumentSets testEmptyDataNodesAlterZoneParamFactory() {
        return ArgumentSets.argumentsForFirstParameter(ConsistencyMode.STRONG_CONSISTENCY, ConsistencyMode.HIGH_AVAILABILITY)
                .argumentsForNextParameter(
                        "REPLICAS 1",
                        "QUORUM SIZE 3",
                        "AUTO SCALE UP 10000",
                        "AUTO SCALE DOWN 10000"
                );
    }

    private static void executeSql(IgniteImpl node, String sql) {
        node.sql().execute(null, sql);
    }

    private static void waitDataNodeAndListenersAreHandled(
            MetaStorageManager metaStorageManager,
            int expectedDataNodesSize,
            int zoneId
    ) throws Exception {
        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesHistoryKey(zoneId),
                (v) -> deserializeLatestDataNodesHistoryEntry(v).size(),
                expectedDataNodesSize,
                TIMEOUT_MILLIS
        );

        ByteArray fakeKey = new ByteArray("Foo");

        metaStorageManager.put(fakeKey, toBytes("Bar")).get(5_000, MILLISECONDS);

        Entry fakeEntry = metaStorageManager.get(fakeKey).get(5_000, MILLISECONDS);

        // We wait for all data nodes listeners are triggered and all their meta storage activity is done.
        assertTrue(waitForCondition(() -> metaStorageManager.appliedRevision() >= fakeEntry.revision(), 5_000));

        assertValueInStorage(
                metaStorageManager,
                zoneDataNodesHistoryKey(zoneId),
                (v) -> deserializeLatestDataNodesHistoryEntry(v).size(),
                expectedDataNodesSize,
                TIMEOUT_MILLIS
        );
    }

    private static void assertPendingAssignmentsNeverExisted(
            MetaStorageManager metaStorageManager,
            ZonePartitionId partId
    ) throws InterruptedException, ExecutionException {
        assertTrue(metaStorageManager.get(pendingPartitionAssignmentsKey(partId)).get().empty());
    }

    private static void assertStableAssignmentsAreNotEmpty(
            MetaStorageManager metaStorageManager,
            ZonePartitionId partId
    ) throws ExecutionException, InterruptedException {
        assertFalse(metaStorageManager.get(stablePartitionAssignmentsKey(partId)).get().empty());
    }

    private static String createZoneSql(
            int partitions,
            int replicas,
            int scaleUp,
            int scaleDown,
            String filter,
            String storageProfiles,
            ConsistencyMode consistencyMode
    ) {
        String sqlFormat = "CREATE ZONE %s ("
                + "REPLICAS %s, "
                + "PARTITIONS %s, "
                + "NODES FILTER '%s', "
                + "AUTO SCALE UP %s, "
                + "AUTO SCALE DOWN %s, "
                + "CONSISTENCY MODE '%s') "
                + "STORAGE PROFILES [%s]";

        return String.format(sqlFormat, ZONE_NAME, replicas, partitions, filter, scaleUp, scaleDown, consistencyMode, storageProfiles);
    }

    private static String alterZoneSql(String filter) {
        return String.format("ALTER ZONE \"%s\" SET (NODES FILTER '%s')", ZONE_NAME, filter);
    }

    private static String alterZoneSql(int replicas) {
        return String.format("ALTER ZONE \"%s\" SET (REPLICAS %s)", ZONE_NAME, replicas);
    }

    private static String alterZoneSetSql(String set) {
        return String.format("ALTER ZONE \"%s\" SET (%s)", ZONE_NAME, set);
    }

    private static String createTableSql() {
        return String.format(
                "CREATE TABLE %s(%s INT PRIMARY KEY, %s VARCHAR) ZONE %s STORAGE PROFILE '%s'",
                TABLE_NAME, COLUMN_KEY, COLUMN_VAL, ZONE_NAME, DEFAULT_AIPERSIST_PROFILE_NAME
        );
    }

    private static int getZoneId(Ignite node) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);
        return DistributionZonesTestUtil.getZoneIdStrict(igniteImpl.catalogManager(), ZONE_NAME, igniteImpl.clock().nowLong());
    }
}
