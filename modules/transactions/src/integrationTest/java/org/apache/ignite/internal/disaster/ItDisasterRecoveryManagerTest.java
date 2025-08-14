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

package org.apache.ignite.internal.disaster;

import static java.util.Collections.emptySet;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.createZone;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getDefaultZone;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryManager;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum;
import org.apache.ignite.internal.table.distributed.disaster.GlobalTablePartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateByNode;
import org.apache.ignite.internal.table.distributed.disaster.LocalTablePartitionState;
import org.apache.ignite.internal.table.distributed.disaster.LocalTablePartitionStateByNode;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/** For {@link DisasterRecoveryManager} integration testing. */
// TODO https://issues.apache.org/jira/browse/IGNITE-22332 Add test cases.
public class ItDisasterRecoveryManagerTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    public static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = "ZONE_" + TABLE_NAME;

    private static final int INITIAL_NODES = 1;

    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("id", NativeTypes.INT32, false)},
            new Column[]{
                    new Column("valInt", NativeTypes.INT32, false),
            }
    );

    @Override
    protected int initialNodes() {
        return INITIAL_NODES;
    }

    @BeforeEach
    void setUp(TestInfo testInfo) {
        Method testMethod = testInfo.getTestMethod().orElseThrow();

        ZoneParams zoneParams = testMethod.getAnnotation(ZoneParams.class);

        if (zoneParams != null) {
            IntStream.range(INITIAL_NODES, zoneParams.nodes()).forEach(i -> cluster.startNode(i));
        }

        int replicas = zoneParams != null ? zoneParams.replicas() : initialNodes();

        int partitions = zoneParams != null ? zoneParams.partitions() : initialNodes();

        executeSql(String.format(
                "CREATE ZONE %s (REPLICAS %s, PARTITIONS %s) STORAGE PROFILES ['%s']",
                ZONE_NAME, replicas, partitions, DEFAULT_STORAGE_PROFILE
        ));

        executeSql(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, valInt INT) ZONE %s",
                TABLE_NAME,
                ZONE_NAME
        ));
    }

    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "false")
    @Test
    void testRestartTablePartitions() {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        int partitionId = 0;

        CompletableFuture<Void> restartPartitionsFuture = node.disasterRecoveryManager().restartTablePartitions(
                Set.of(node.name()),
                ZONE_NAME,
                SqlCommon.DEFAULT_SCHEMA_NAME,
                TABLE_NAME,
                Set.of(partitionId)
        );

        assertThat(restartPartitionsFuture, willCompleteSuccessfully());
        assertThat(awaitPrimaryReplicaForNow(node, new TablePartitionId(tableId(node), partitionId)), willCompleteSuccessfully());

        insert(2, 2);
        insert(3, 3);

        assertThat(selectAll(), hasSize(4));
    }

    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "false")
    @Test
    void testRestartTablePartitionsWithCleanUpFails() {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        int partitionId = 0;

        CompletableFuture<Void> restartPartitionsWithCleanupFuture = node.disasterRecoveryManager().restartTablePartitionsWithCleanup(
                Set.of(node.name()),
                ZONE_NAME,
                SqlCommon.DEFAULT_SCHEMA_NAME,
                TABLE_NAME,
                Set.of(partitionId)
        );

        ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> restartPartitionsWithCleanupFuture.get(10_000, MILLISECONDS)
        );

        assertInstanceOf(DisasterRecoveryException.class, exception.getCause());

        assertThat(exception.getCause().getMessage(), is("Not enough alive node to perform reset with clean up."));
    }

    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "false")
    @Test
    void testRestartTablePartitionsWithCleanUp() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());
        IgniteImpl node1 = unwrapIgniteImpl(cluster.startNode(1));
        IgniteImpl node2 = unwrapIgniteImpl(cluster.startNode(2));

        String testZone = "TEST_ZONE";

        createZone(node.catalogManager(), testZone, 1, 3);

        String tableName2 = "TABLE_NAME_2";

        node.sql().executeScript(String.format(
                "CREATE TABLE %s (id INT PRIMARY KEY, valInt INT) ZONE TEST_ZONE",
                tableName2
        ));

        insert(0, 0, tableName2);

        assertValueOnSpecificNodes(tableName2, Set.of(node, node1, node2), 0, 0);

        int partitionId = 0;

        CompletableFuture<Void> restartPartitionsWithCleanupFuture = node.disasterRecoveryManager().restartTablePartitionsWithCleanup(
                Set.of(node.name()),
                testZone,
                SqlCommon.DEFAULT_SCHEMA_NAME,
                tableName2,
                Set.of(partitionId)
        );

        assertThat(restartPartitionsWithCleanupFuture, willCompleteSuccessfully());
        assertThat(awaitPrimaryReplicaForNow(node, new TablePartitionId(tableId(node), partitionId)), willCompleteSuccessfully());

        insert(1, 1, tableName2);

        assertValueOnSpecificNodes(tableName2, Set.of(node, node1, node2), 0, 0);
        assertValueOnSpecificNodes(tableName2, Set.of(node, node1, node2), 1, 1);
    }

    private static void assertValueOnSpecificNodes(String tableName, Set<IgniteImpl> nodes, int id, int val) throws Exception {
        for (IgniteImpl node : nodes) {
            assertValueOnSpecificNode(tableName, node, id, val);
        }
    }

    private static void assertValueOnSpecificNode(String tableName, IgniteImpl node, int id, int val) throws Exception {
        InternalTable internalTable = unwrapTableViewInternal(node.tables().table(tableName)).internalTable();

        Row keyValueRow0 = createKeyValueRow(id, val);
        Row keyRow0 = createKeyRow(id);

        CompletableFuture<BinaryRow> getFut = internalTable.get(keyRow0, node.clock().now(), node.node());

        assertThat(getFut, willCompleteSuccessfully());

        assertTrue(compareRows(getFut.get(), keyValueRow0));
    }

    @Test
    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "true")
    void testRestartZonePartitions() {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        int partitionId = 0;

        CompletableFuture<Void> restartPartitionsFuture = node.disasterRecoveryManager().restartPartitions(
                Set.of(node.name()),
                ZONE_NAME,
                Set.of(partitionId)
        );

        assertThat(restartPartitionsFuture, willCompleteSuccessfully());
        assertThat(awaitPrimaryReplicaForNow(node, new ZonePartitionId(zoneId(node), partitionId)), willCompleteSuccessfully());

        insert(2, 2);
        insert(3, 3);

        assertThat(selectAll(), hasSize(4));
    }

    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "false")
    @Test
    @ZoneParams(nodes = 2, replicas = 1, partitions = 2)
    void testEstimatedRowsTable() throws Exception {
        validateEstimatedRows();
    }

    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "true")
    @Test
    @ZoneParams(nodes = 2, replicas = 1, partitions = 2)
    void testEstimatedRowsTableZone() throws Exception {
        validateEstimatedRows();
    }

    private void validateEstimatedRows() throws InterruptedException {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        // Wait for replication to finish.
        assertTrue(waitForCondition(() -> {
                    CompletableFuture<Map<TablePartitionId, LocalTablePartitionStateByNode>> localStateTableFuture =
                            node.disasterRecoveryManager().localTablePartitionStates(emptySet(), emptySet(), emptySet());

                    assertThat(localStateTableFuture, willCompleteSuccessfully());
                    Map<TablePartitionId, LocalTablePartitionStateByNode> localState;
                    try {
                        localState = localStateTableFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                    Set<Long> size = localState.values().stream()
                            .flatMap(localTablePartitionStateByNode -> localTablePartitionStateByNode.values().stream())
                            .map(state -> state.estimatedRows)
                            .collect(Collectors.toSet());
                    // There are 2 nodes, 2 partitions and 1 replica, so we should have 2 entries in localState (one for each partition),
                    // LocalTablePartitionStateByNode should have a entry for either the first or the second node with 1 row.
                    return size.size() == 1 && size.contains(1L) && localState.size() == 2;
                },
                20_000
        ));
    }

    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "true")
    @Test
    @ZoneParams(nodes = 2, replicas = 1, partitions = 2)
    void testEstimatedRowsZone() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        // Wait for replication to finish.
        assertTrue(waitForCondition(() -> {
                    CompletableFuture<Map<ZonePartitionId, LocalPartitionStateByNode>> localStateTableFuture =
                            node.disasterRecoveryManager().localPartitionStates(Set.of(ZONE_NAME), emptySet(), emptySet());

                    assertThat(localStateTableFuture, willCompleteSuccessfully());

                    Map<ZonePartitionId, LocalPartitionStateByNode> localState;
                    try {
                        localState = localStateTableFuture.get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new RuntimeException(e);
                    }

                    Set<Long> size = localState.values().stream()
                            .flatMap(localTablePartitionStateByNode -> localTablePartitionStateByNode.values().stream())
                            .map(state -> state.estimatedRows)
                            .collect(Collectors.toSet());
                    // There are 2 nodes, 2 partitions and 1 replica, so we should have 2 entries in localState (one for each partition),
                    // LocalTablePartitionStateByNode should have a entry for either the first or the second node with 1 row.
                    return size.size() == 1 && size.contains(1L) && localState.size() == 2;
                },
                20_000
        ));
    }

    @Test
    @ZoneParams(nodes = 2, replicas = 2, partitions = 2)
    void testLocalPartitionStateTable() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        CompletableFuture<Map<TablePartitionId, LocalTablePartitionStateByNode>> localStateTableFuture =
                node.disasterRecoveryManager().localTablePartitionStates(emptySet(), emptySet(), emptySet());

        assertThat(localStateTableFuture, willCompleteSuccessfully());
        Map<TablePartitionId, LocalTablePartitionStateByNode> localState = localStateTableFuture.get();

        // 2 partitions.
        assertThat(localState, aMapWithSize(2));

        int tableId = tableId(node);

        // Partitions size is 2.
        for (int partitionId = 0; partitionId < 2; partitionId++) {
            LocalTablePartitionStateByNode partitionStateByNode = localState.get(new TablePartitionId(tableId, partitionId));
            // 2 nodes.
            assertThat(partitionStateByNode.values(), hasSize(2));

            for (LocalTablePartitionState state : partitionStateByNode.values()) {
                assertThat(state.tableId, is(tableId));
                assertThat(state.tableName, is(TABLE_NAME));
                assertThat(state.schemaName, is(SqlCommon.DEFAULT_SCHEMA_NAME));
                assertThat(state.partitionId, is(partitionId));
                assertThat(state.zoneName, is(ZONE_NAME));
                assertThat(state.state, is(LocalPartitionStateEnum.HEALTHY));
            }
        }
    }

    @Test
    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "true")
    @ZoneParams(nodes = 2, replicas = 2, partitions = 2)
    void testLocalPartitionStateZone() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        if (colocationEnabled()) {
            // Generally it's required to await default zone dataNodesAutoAdjustScaleUp timeout in order to treat zone as ready one.
            // In order to eliminate awaiting interval, default zone scaleUp is altered to be immediate.
            setDefaultZoneAutoAdjustScaleUpTimeoutToImmediate();
        }

        insert(0, 0);
        insert(1, 1);

        CompletableFuture<Map<ZonePartitionId, LocalPartitionStateByNode>> localStateTableFuture =
                node.disasterRecoveryManager().localPartitionStates(emptySet(), emptySet(), emptySet());

        assertThat(localStateTableFuture, willCompleteSuccessfully());
        Map<ZonePartitionId, LocalPartitionStateByNode> localState = localStateTableFuture.get();

        // A default zone and a custom zone, which was created in `BeforeEach`.
        // 27 partitions = CatalogUtils.DEFAULT_PARTITION_COUNT (=25) + 2.
        assertThat(localState, aMapWithSize(27));

        int zoneId = zoneId(node);

        // Partitions size is 2.
        for (int partitionId = 0; partitionId < 2; partitionId++) {
            LocalPartitionStateByNode partitionStateByNode = localState.get(new ZonePartitionId(zoneId, partitionId));
            // 2 nodes.
            assertThat(partitionStateByNode.values(), hasSize(2));

            for (LocalPartitionState state : partitionStateByNode.values()) {
                assertThat(state.zoneId, is(zoneId));
                assertThat(state.zoneName, is(ZONE_NAME));
                assertThat(state.partitionId, is(partitionId));
                assertThat(state.state, is(LocalPartitionStateEnum.HEALTHY));
            }
        }
    }

    @Test
    @ZoneParams(nodes = 2, replicas = 2, partitions = 2)
    void testGlobalPartitionStateTable() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        insert(0, 0);
        insert(1, 1);

        CompletableFuture<Map<TablePartitionId, GlobalTablePartitionState>> globalStatesFuture =
                node.disasterRecoveryManager().globalTablePartitionStates(emptySet(), emptySet());

        assertThat(globalStatesFuture, willCompleteSuccessfully());
        Map<TablePartitionId, GlobalTablePartitionState> globalState = globalStatesFuture.get();

        // 2 partitions.
        assertThat(globalState, aMapWithSize(2));

        int tableId = tableId(node);

        // Partitions size is 2.
        for (int partitionId = 0; partitionId < 2; partitionId++) {
            GlobalTablePartitionState state = globalState.get(new TablePartitionId(tableId, partitionId));
            assertThat(state.tableId, is(tableId));
            assertThat(state.tableName, is(TABLE_NAME));
            assertThat(state.schemaName, is(SqlCommon.DEFAULT_SCHEMA_NAME));
            assertThat(state.partitionId, is(partitionId));
            assertThat(state.zoneName, is(ZONE_NAME));
            assertThat(state.state, is(GlobalPartitionStateEnum.AVAILABLE));
        }
    }

    @Test
    @WithSystemProperty(key = IgniteSystemProperties.COLOCATION_FEATURE_FLAG, value = "true")
    @ZoneParams(nodes = 2, replicas = 2, partitions = 2)
    void testGlobalPartitionStateZone() throws Exception {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        if (colocationEnabled()) {
            // Generally it's required to await default zone dataNodesAutoAdjustScaleUp timeout in order to treat zone as ready one.
            // In order to eliminate awaiting interval, default zone scaleUp is altered to be immediate.
            setDefaultZoneAutoAdjustScaleUpTimeoutToImmediate();
        }

        insert(0, 0);
        insert(1, 1);

        CompletableFuture<Map<ZonePartitionId, GlobalPartitionState>> globalStatesFuture =
                node.disasterRecoveryManager().globalPartitionStates(emptySet(), emptySet());

        assertThat(globalStatesFuture, willCompleteSuccessfully());
        Map<ZonePartitionId, GlobalPartitionState> globalState = globalStatesFuture.get();

        // A default zone and a custom zone, which was created in `BeforeEach`.
        // 27 partitions = CatalogUtils.DEFAULT_PARTITION_COUNT (=25) + 2.
        assertThat(globalState, aMapWithSize(27));

        int zoneId = zoneId(node);

        // Partitions size is 2.
        for (int partitionId = 0; partitionId < 2; partitionId++) {
            GlobalPartitionState state = globalState.get(new ZonePartitionId(zoneId, partitionId));
            assertThat(state.zoneId, is(zoneId));
            assertThat(state.zoneName, is(ZONE_NAME));
            assertThat(state.partitionId, is(partitionId));
            assertThat(state.state, is(GlobalPartitionStateEnum.AVAILABLE));
        }
    }

    private void insert(int id, int val) {
        insert(id, val, TABLE_NAME);
    }

    private void insert(int id, int val, String tableName) {
        executeSql(String.format(
                "INSERT INTO %s (id, valInt) VALUES (%s, %s)",
                tableName, id, val
        ));
    }

    private List<List<Object>> selectAll() {
        return selectAll(TABLE_NAME);
    }

    private List<List<Object>> selectAll(String tableName) {
        return executeSql(String.format(
                "SELECT * FROM %s",
                tableName
        ));
    }

    private static int tableId(IgniteImpl node) {
        return ((Wrapper) node.tables().table(TABLE_NAME)).unwrap(TableImpl.class).tableId();
    }

    private static int zoneId(IgniteImpl node) {
        return node.catalogManager().catalog(node.catalogManager().latestCatalogVersion()).zone(ZONE_NAME).id();
    }

    private static CompletableFuture<ReplicaMeta> awaitPrimaryReplicaForNow(IgniteImpl node, ReplicationGroupId replicationGroupId) {
        return node.placementDriver().awaitPrimaryReplica(replicationGroupId, node.clock().now(), 60, SECONDS);
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface ZoneParams {
        int replicas() default INITIAL_NODES;

        int partitions() default INITIAL_NODES;

        int nodes() default INITIAL_NODES;
    }

    private void setDefaultZoneAutoAdjustScaleUpTimeoutToImmediate() {
        IgniteImpl node = unwrapIgniteImpl(node(0));
        CatalogManager catalogManager = node.catalogManager();
        CatalogZoneDescriptor defaultZone = getDefaultZone(catalogManager, node.clock().nowLong());

        node(0).sql().executeScript(String.format("ALTER ZONE \"%s\"SET (AUTO SCALE UP 0)", defaultZone.name()));
    }

    private static Row createKeyRow(int id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA.version(), SCHEMA.keyColumns(), -1);

        rowBuilder.appendInt(id);

        return Row.wrapKeyOnlyBinaryRow(SCHEMA, rowBuilder.build());
    }

    private static Row createKeyValueRow(int id, int value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, -1);

        rowBuilder.appendInt(id);
        rowBuilder.appendInt(value);

        return Row.wrapBinaryRow(SCHEMA, rowBuilder.build());
    }

    private static boolean compareRows(BinaryRow row1, BinaryRow row2) {
        return row1.schemaVersion() == row2.schemaVersion() && row1.tupleSlice().equals(row2.tupleSlice());
    }
}
