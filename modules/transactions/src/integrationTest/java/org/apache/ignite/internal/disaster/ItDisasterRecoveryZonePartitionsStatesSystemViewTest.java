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
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.AVAILABLE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.MetadataMatcher;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.sql.ColumnType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** For integration testing of disaster recovery system views. */
public class ItDisasterRecoveryZonePartitionsStatesSystemViewTest extends BaseSqlIntegrationTest {
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
        assertQuery("SELECT * FROM SYSTEM.GLOBAL_ZONE_PARTITION_STATES")
                .columnMetadata(
                        new MetadataMatcher().name("ZONE_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("PARTITION_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("PARTITION_STATE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("ZONE_ID").type(ColumnType.INT32).nullable(true)
                )
                .check();
    }

    @Test
    public void localPatitionStatesMetadata() {
        assertQuery("SELECT * FROM SYSTEM.LOCAL_ZONE_PARTITION_STATES")
                .columnMetadata(
                        new MetadataMatcher().name("NODE_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("ZONE_NAME").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("PARTITION_ID").type(ColumnType.INT32).nullable(true),
                        new MetadataMatcher().name("PARTITION_STATE").type(ColumnType.STRING).nullable(true),
                        new MetadataMatcher().name("ESTIMATED_ROWS").type(ColumnType.INT64).nullable(true),
                        new MetadataMatcher().name("ZONE_ID").type(ColumnType.INT32).nullable(true)
                )
                .check();
    }

    @Test
    void testGlobalPartitionStatesSystemView() {
        int partitionsCount = 2;

        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), partitionsCount);

        waitLeaderOnAllPartitions(ZONE_NAME, partitionsCount);

        int zoneId = getZoneId(ZONE_NAME);

        assertQuery(globalZonePartitionStatesSystemViewSql())
                .returns(ZONE_NAME, 0, AVAILABLE.name(), zoneId)
                .returns(ZONE_NAME, 1, AVAILABLE.name(), zoneId)
                .check();
    }

    @Test
    void testLocalPartitionStatesSystemView() {
        assertEquals(2, initialNodes());

        int partitionsCount = 2;

        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), partitionsCount);

        waitLeaderOnAllPartitions(ZONE_NAME, partitionsCount);

        List<String> nodeNames = CLUSTER.runningNodes().map(Ignite::name).sorted().collect(toList());

        String nodeName0 = nodeNames.get(0);
        String nodeName1 = nodeNames.get(1);

        int zoneId = getZoneId(ZONE_NAME);

        assertQuery(localZonePartitionStatesSystemViewSql())
                .returns(nodeName0, ZONE_NAME, 0, HEALTHY.name(), 0L, zoneId)
                .returns(nodeName0, ZONE_NAME, 1, HEALTHY.name(), 0L, zoneId)
                .returns(nodeName1, ZONE_NAME, 0, HEALTHY.name(), 0L, zoneId)
                .returns(nodeName1, ZONE_NAME, 1, HEALTHY.name(), 0L, zoneId)
                .check();
    }

    @Test
    void testLocalPartitionStatesSystemViewWithUpdatedEstimatedRows() throws Exception {
        assertEquals(2, initialNodes());

        int partitionsCount = 1;

        createZoneAndTable(ZONE_NAME, TABLE_NAME, initialNodes(), partitionsCount);

        waitLeaderOnAllPartitions(ZONE_NAME, partitionsCount);

        insertPeople(
                TABLE_NAME,
                new Person(1, "foo_name", 100.0),
                new Person(2, "bar_name", 200.0)
        );

        List<String> nodeNames = CLUSTER.runningNodes().map(Ignite::name).sorted().collect(toList());

        // Small wait is specially added so that the follower can execute the replicated "insert" command and the counter is honestly
        // increased.
        assertTrue(waitForCondition(
                () -> nodeNames.stream().allMatch(nodeName -> estimatedSize(nodeName, TABLE_NAME, 0, CLUSTER) >= 2L),
                10,
                1_000
        ));

        int zoneId = getZoneId(ZONE_NAME);

        assertQuery(localZonePartitionStatesSystemViewSql())
                .returns(nodeNames.get(0), ZONE_NAME, 0, HEALTHY.name(), 2L, zoneId)
                .returns(nodeNames.get(1), ZONE_NAME, 0, HEALTHY.name(), 2L, zoneId)
                .check();
    }

    /**
     * Waiting a leader for all partitions because later we expect that partitions will be in AVAILABLE state. Without it there won't be
     * log updating (see {@code LocalPartitionStateEnumWithLogIndex#of}) and then in SYSTEM.*_PARTITION_STATES we will get UNAVAILABLE state
     * instead of the desired one. That's why in {@link #testGlobalPartitionStatesSystemView()} and
     * {@link #testLocalPartitionStatesSystemView()} we must manually trigger {@link RaftGroupService#refreshLeader()} that will lead
     * partitions to the proper states.
     *
     * @param zoneName A zone whose partitions will do a leader refresh.
     * @param partitionsCount Expected the table partitions count for iterating over them.
     */
    @SuppressWarnings("removal")
    static void waitLeaderOnAllPartitions(String zoneName, int partitionsCount) {
        IgniteImpl node = unwrapIgniteImpl(CLUSTER.node(0));

        int zoneId = getZoneId(zoneName);

        IntStream.range(0, partitionsCount).forEach(partId -> assertThat(
                node.replicaManager()
                        .replica(new ZonePartitionId(zoneId, partId))
                        .thenCompose(replica -> replica.raftClient().refreshLeader()),
                willCompleteSuccessfully()
        ));
    }

    private static String globalZonePartitionStatesSystemViewSql() {
        return "SELECT ZONE_NAME, PARTITION_ID, PARTITION_STATE, ZONE_ID FROM SYSTEM.GLOBAL_ZONE_PARTITION_STATES "
                + "WHERE ZONE_NAME != 'Default'";
    }

    private static String localZonePartitionStatesSystemViewSql() {
        return "SELECT NODE_NAME, ZONE_NAME, PARTITION_ID, PARTITION_STATE, ESTIMATED_ROWS, ZONE_ID"
                + " FROM SYSTEM.LOCAL_ZONE_PARTITION_STATES WHERE ZONE_NAME != 'Default'";
    }

    private static int getZoneId(String zoneName) {
        return unwrapIgniteImpl(CLUSTER.aliveNode()).catalogManager()
                .latestCatalog()
                .zone(zoneName)
                .id();
    }

    static long estimatedSize(String nodeName, String tableName, int partitionId, Cluster cluster) {
        return cluster.runningNodes()
                .filter(ignite -> nodeName.equals(ignite.name()))
                .map(ignite -> {
                    TableImpl table = unwrapTableImpl(ignite.tables().table(tableName));

                    return table.internalTable().storage().getMvPartition(partitionId);
                })
                .filter(Objects::nonNull)
                .map(MvPartitionStorage::estimatedSize)
                .findAny()
                .orElse(-1L);
    }
}
