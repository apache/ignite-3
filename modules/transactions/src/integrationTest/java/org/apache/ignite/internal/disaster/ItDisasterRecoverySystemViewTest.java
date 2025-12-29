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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.disaster.ItDisasterRecoveryZonePartitionsStatesSystemViewTest.estimatedSize;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.AVAILABLE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.PublicApiThreadingTable;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** For integration testing of disaster recovery system views. */
public class ItDisasterRecoverySystemViewTest extends BaseSqlIntegrationTest {
    /** Table name. */
    public static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = "ZONE_" + TABLE_NAME;

    @Override
    protected int initialNodes() {
        return 2;
    }

    @AfterEach
    void tearDown() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);
    }

    @Test
    public void globalPatitionStatesMetadata() {
        assertQuery("SELECT * FROM SYSTEM.GLOBAL_PARTITION_STATES")
                .columnMetadata(
                        new MetadataMatcher().name("ZONE_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TABLE_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("SCHEMA_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TABLE_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("PARTITION_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("PARTITION_STATE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("ZONE_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("SCHEMA_ID").type(ColumnType.INT32).nullable(true),
                        // Legacy columns.
                        new MetadataMatcher().name("STATE").type(ColumnType.STRING).nullable(true)
                )
                .check();
    }

    @Test
    public void localPatitionStatesMetadata() {
        assertQuery("SELECT * FROM SYSTEM.LOCAL_PARTITION_STATES")
                .columnMetadata(
                        new MetadataMatcher().name("NODE_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("ZONE_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TABLE_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("SCHEMA_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("TABLE_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("PARTITION_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("PARTITION_STATE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("ESTIMATED_ROWS").type(ColumnType.INT64).nullable(true),
                        new MetadataMatcher().name("ZONE_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("SCHEMA_ID").type(ColumnType.INT32).nullable(true),
                        // Legacy columns.
                        new MetadataMatcher().name("STATE").type(ColumnType.STRING).nullable(true)
                )
                .check();
    }

    @Test
    void testNoZonesAndTables() {
        assertQuery(globalPartitionStatesSystemViewSql()).returnNothing().check();
        assertQuery(localPartitionStatesSystemViewSql()).returnNothing().check();
    }

    @Test
    void testGlobalPartitionStatesSystemView() {
        int partitionsCount = 2;

        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), partitionsCount);

        if (colocationEnabled()) {
            ItDisasterRecoveryZonePartitionsStatesSystemViewTest.waitLeaderOnAllPartitions(ZONE_NAME, partitionsCount);
        } else {
            waitLeaderOnAllPartitions(TABLE_NAME, partitionsCount);
        }

        int tableId = getTableId(DEFAULT_SCHEMA_NAME, TABLE_NAME);

        assertQuery(globalPartitionStatesSystemViewSql())
                .returns(ZONE_NAME, tableId, DEFAULT_SCHEMA_NAME, TABLE_NAME, 0, AVAILABLE.name())
                .returns(ZONE_NAME, tableId, DEFAULT_SCHEMA_NAME, TABLE_NAME, 1, AVAILABLE.name())
                .check();
    }

    @Test
    void testLocalPartitionStatesSystemView() {
        assertEquals(2, initialNodes());

        int partitionsCount = 2;

        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), partitionsCount);

        if (colocationEnabled()) {
            ItDisasterRecoveryZonePartitionsStatesSystemViewTest.waitLeaderOnAllPartitions(ZONE_NAME, partitionsCount);
        } else {
            waitLeaderOnAllPartitions(TABLE_NAME, partitionsCount);
        }

        List<String> nodeNames = CLUSTER.runningNodes().map(Ignite::name).sorted().collect(toList());

        String nodeName0 = nodeNames.get(0);
        String nodeName1 = nodeNames.get(1);

        int tableId = getTableId(DEFAULT_SCHEMA_NAME, TABLE_NAME);

        assertQuery(localPartitionStatesSystemViewSql())
                .returns(nodeName0, ZONE_NAME, tableId, DEFAULT_SCHEMA_NAME, TABLE_NAME, 0, HEALTHY.name(), 0L)
                .returns(nodeName0, ZONE_NAME, tableId, DEFAULT_SCHEMA_NAME, TABLE_NAME, 1, HEALTHY.name(), 0L)
                .returns(nodeName1, ZONE_NAME, tableId, DEFAULT_SCHEMA_NAME, TABLE_NAME, 0, HEALTHY.name(), 0L)
                .returns(nodeName1, ZONE_NAME, tableId, DEFAULT_SCHEMA_NAME, TABLE_NAME, 1, HEALTHY.name(), 0L)
                .check();
    }

    @Test
    void testLocalPartitionStatesSystemViewWithUpdatedEstimatedRows() throws Exception {
        assertEquals(2, initialNodes());

        int partitionsCount = 1;

        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), partitionsCount);

        if (colocationEnabled()) {
            ItDisasterRecoveryZonePartitionsStatesSystemViewTest.waitLeaderOnAllPartitions(ZONE_NAME, partitionsCount);
        } else {
            waitLeaderOnAllPartitions(TABLE_NAME, partitionsCount);
        }

        insertPeople(
                TABLE_NAME,
                new Person(1, "foo_name", 100.0),
                new Person(2, "bar_name", 200.0)
        );

        List<String> nodeNames = CLUSTER.runningNodes().map(Ignite::name).sorted().collect(toList());

        int tableId = getTableId(DEFAULT_SCHEMA_NAME, TABLE_NAME);

        // Small wait is specially added so that the follower can execute the replicated "insert" command and the counter is honestly
        // increased.
        assertTrue(waitForCondition(
                () -> nodeNames.stream().allMatch(nodeName -> estimatedSize(nodeName, TABLE_NAME, 0, CLUSTER) >= 2L),
                10,
                1_000
        ));

        assertQuery(localPartitionStatesSystemViewSql())
                .returns(nodeNames.get(0), ZONE_NAME, tableId, DEFAULT_SCHEMA_NAME, TABLE_NAME, 0, HEALTHY.name(), 2L)
                .returns(nodeNames.get(1), ZONE_NAME, tableId, DEFAULT_SCHEMA_NAME, TABLE_NAME, 0, HEALTHY.name(), 2L)
                .check();
    }

    /**
     * waiting a leader for all partitions because later we expect that partitions will be in AVAILABLE state. Without it there won't be
     * log updating (see {@code LocalPartitionStateEnumWithLogIndex#of}) and then in SYSTEM.*_PARTITION_STATES we will get UNAVAILABLE state
     * instead of the desired one. That's why in {@link #testGlobalPartitionStatesSystemView()} and
     * {@link #testLocalPartitionStatesSystemView()} we must manually trigger {@link RaftGroupService#refreshLeader()} that will lead
     * partitions to the proper states.
     *
     * @param tableName A table whose partitions will do a leader refresh.
     * @param partitionsCount Expected the table partitions count for iterating over them.
     */
    private static void waitLeaderOnAllPartitions(String tableName, int partitionsCount) {
        IgniteImpl node = unwrapIgniteImpl(CLUSTER.node(0));

        TableImpl table = ((PublicApiThreadingTable) node.tables().table(tableName)).unwrap(TableImpl.class);

        int tableId = table.tableId();

        IntStream.range(0, partitionsCount).forEach(partId -> assertThat(
                node.replicaManager()
                        .replica(new TablePartitionId(tableId, partId))
                        .thenCompose(replica -> replica.raftClient().refreshLeader()),
                willCompleteSuccessfully()
        ));
    }

    private static String globalPartitionStatesSystemViewSql() {
        return "SELECT ZONE_NAME, TABLE_ID, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATE FROM SYSTEM.GLOBAL_PARTITION_STATES";
    }

    private static String localPartitionStatesSystemViewSql() {
        return "SELECT NODE_NAME, ZONE_NAME, TABLE_ID, SCHEMA_NAME, TABLE_NAME, PARTITION_ID, STATE, ESTIMATED_ROWS"
                + " FROM SYSTEM.LOCAL_PARTITION_STATES";
    }

    private static int getTableId(String schemaName, String tableName) {
        return unwrapIgniteImpl(CLUSTER.aliveNode()).catalogManager()
                .latestCatalog()
                .table(schemaName, tableName)
                .id();
    }
}
