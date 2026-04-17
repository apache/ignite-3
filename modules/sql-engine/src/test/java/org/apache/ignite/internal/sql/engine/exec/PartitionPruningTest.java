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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.exec.TransactionEnlistTest.blackhole;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.toSqlType;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.toInternal;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.planner.datatypes.utils.Types;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.util.QueryCheckerExtension;
import org.apache.ignite.internal.sql.engine.util.SqlTestUtils;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

/**
 * Various tests regarding partition pruning.
 */
@ExtendWith(QueryCheckerExtension.class)
public class PartitionPruningTest extends BaseIgniteAbstractTest {
    private static final List<String> DATA_NODES = List.of("DATA_1", "DATA_2");
    private static final String GATEWAY_NODE_NAME = "gateway";
    private static final int PARTITIONS_COUNT = 5;

    private TestCluster cluster;

    @BeforeAll
    static void warmUpCluster() throws Exception {
        TestBuilders.warmupTestCluster();
    }

    @BeforeEach
    void startCluster() {
        cluster = TestBuilders.cluster()
                .nodes(GATEWAY_NODE_NAME, DATA_NODES.toArray(new String[0]))
                .build();

        cluster.start();
    }

    @AfterEach
    void stopCluster() throws Exception {
        cluster.stop();
    }

    private static Stream<Arguments> temporalTypes() {
        return Stream.of(Types.TIME_0, Types.TIME_3, Types.TIMESTAMP_0, Types.TIMESTAMP_3, Types.TIMESTAMP_WLTZ_0, Types.TIMESTAMP_WLTZ_3)
                .map(Arguments::of);
    }

    @ParameterizedTest
    @MethodSource("temporalTypes")
    void testPartitionPruningForScanOnlyWithTemporalTypes(NativeType type) {
        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);
        String tableName = prepareTemporalTable(type);
        Object key = Objects.requireNonNull(SqlTestUtils.generateValueByType(type));

        int expectedPartition = expectedPartition(key, type);

        // Override data provider to throw an exception whenever unexpected partition is requested.
        cluster.setDataProvider(tableName, TestBuilders.tableScan((nodeName, partId) -> {
            if (expectedPartition != partId) {
                throw new RuntimeException("Requested unexpected partition [expectedPartition="
                        + expectedPartition + ", requestedPartition=" + partId + "]");
            }

            return Collections.singleton(new Object[]{partId, toInternal(key, type.spec()), nodeName});
        }));

        // Expect no exception to be thrown.
        await(gatewayNode.executeQuery(
                "SELECT node FROM " + tableName + " WHERE id_ts = CAST(? AS " + toSqlType(type) + ")", key
        ).requestNextAsync(128));
    }

    @ParameterizedTest
    @MethodSource("temporalTypes")
    void testPartitionPruningForUpdateWithTemporalTypes(NativeType type) {
        TestNode gatewayNode = cluster.node(GATEWAY_NODE_NAME);
        String tableName = prepareTemporalTable(type);
        NoOpTransaction tx = Mockito.spy(NoOpTransaction.readWrite("t1", false));
        Object key = Objects.requireNonNull(SqlTestUtils.generateValueByType(type));

        await(gatewayNode.executeQuery(
                tx,
                "UPDATE " + tableName + " /*+ no_index */ SET node = UPPER(node) WHERE id_ts = CAST(? AS " + toSqlType(type) + ")",
                key
        ).requestNextAsync(1));

        int expectedPartition = expectedPartition(key, type);
        {
            ArgumentMatcher<ZonePartitionId> partitionIdMatch =
                    zonePartitionId -> zonePartitionId.partitionId() == expectedPartition;
            // We expect commit partitions to be assigned once for given transaction.
            Mockito.verify(tx, times(1))
                    .assignCommitPartition(argThat(partitionIdMatch));
            // Individual partition on the other hand will be enlisted for every source.
            // In this particular case -- first time for scan and second for Modify node.
            Mockito.verify(tx, times(2))
                    .enlist(argThat(partitionIdMatch), anyInt(), any(), anyLong());
        }

        {
            // Due to partition pruning we don't expect any more enlistment.
            // We should not try to assign other partition as commit partition as well.
            ArgumentMatcher<ZonePartitionId> partitionIdMismatch =
                    zonePartitionId -> zonePartitionId.partitionId() != expectedPartition;
            Mockito.verify(tx, never())
                    .assignCommitPartition(argThat(partitionIdMismatch));
            Mockito.verify(tx, never())
                    .enlist(argThat(partitionIdMismatch), anyInt(), any(), anyLong());
        }
    }

    private static int expectedPartition(Object key, NativeType type) {
        var calculator = new PartitionCalculator(PARTITIONS_COUNT, new NativeType[] {type});
        calculator.append(key);
        return calculator.partition();
    }

    private String prepareTemporalTable(NativeType pkColumnType) {
        String tableName = (pkColumnType.spec() + "_table").toUpperCase(Locale.ROOT);

        //noinspection ConcatenationWithEmptyString
        cluster.node(GATEWAY_NODE_NAME).initSchema(""
                + "CREATE ZONE my_zone (partitions " + PARTITIONS_COUNT + ") STORAGE PROFILES ['default'];"
                + format(
                        "CREATE TABLE {} (id int, id_ts {}, node VARCHAR(128), PRIMARY KEY (id, id_ts))",
                        tableName, toSqlType(pkColumnType)
                )
                + " COLOCATE BY (id_ts) ZONE my_zone;");

        cluster.setAssignmentsProvider(tableName, (partitionCount, b) -> IntStream.range(0, partitionCount)
                .mapToObj(i -> DATA_NODES)
                .collect(Collectors.toList()));
        cluster.setDataProvider(tableName, TestBuilders.tableScan(DataProvider.fromCollection(List.of())));
        cluster.setUpdatableTable(tableName, blackhole());

        return tableName;
    }
}
