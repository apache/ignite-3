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

package org.apache.ignite.internal.sql.engine.prepare.partitionawareness;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.ExplainablePlan;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.table.QualifiedName;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link PartitionAwarenessMetadata} in query plans.
 */
public class PartitionAwarenessMetadataTest extends BaseIgniteAbstractTest {

    private static final String NODE_NAME = "N1";

    private static final TestCluster CLUSTER = TestBuilders.cluster()
            .nodes(NODE_NAME)
            .build();

    private final TestNode node = CLUSTER.node(NODE_NAME);

    @BeforeAll
    static void start() {
        CLUSTER.start();
    }

    @AfterAll
    static void stop() throws Exception {
        CLUSTER.stop();
    }

    @AfterEach
    void clearCatalog() {
        Commons.resetFastQueryOptimizationFlag();

        List<CatalogCommand> commands = new ArrayList<>();
        for (CatalogTableDescriptor table : CLUSTER.catalogManager().latestCatalog().tables()) {
            commands.add(
                    DropTableCommand.builder()
                            .schemaName(SqlCommon.DEFAULT_SCHEMA_NAME)
                            .tableName(table.name())
                            .build()
            );
        }

        await(CLUSTER.catalogManager().execute(commands));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SELECT 1",
            "KILL QUERY '1'",
            "KILL TRANSACTION '1'",
            "CREATE TABLE x (a INT, b INT, PRIMARY KEY (a))",
            "SELECT * FROM table(system_range(1, 10)) WHERE x = 1",
            "SELECT (SELECT MIN(c1) FROM t WHERE c1=k1) FROM t2 WHERE k1=?",
            "SELECT (SELECT COUNT(*) FROM t2 WHERE k2=c2) FROM t WHERE t.c1=1",
    })
    public void noMetadata(String query) {
        node.initSchema("CREATE TABLE t (c1 INT PRIMARY KEY, c2 INT)");
        node.initSchema("CREATE TABLE t2 (k1 INT PRIMARY KEY, k2 INT)");

        QueryPlan plan = node.prepare(query);
        PartitionAwarenessMetadata metadata = plan.partitionAwarenessMetadata();
        assertNull(metadata);
    }

    @ParameterizedTest
    @MethodSource("simpleKeyMetadata")
    public void simpleKey(String query, PartitionAwarenessMetadata expected) {
        node.initSchema("CREATE TABLE t (c1 INT PRIMARY KEY, c2 INT, c3 INT)");

        QueryPlan plan = node.prepare(query);
        expectMetadata(plan, expected);
    }

    private static Stream<Arguments> simpleKeyMetadata() {
        return Stream.of(
                // KV GET
                Arguments.of("SELECT * FROM t WHERE c1=?", meta(0)),
                Arguments.of("SELECT * FROM t WHERE c2=? and c1=?", meta(1)),
                Arguments.of("SELECT * FROM t WHERE c1=" + Long.MAX_VALUE, null),
                Arguments.of("SELECT * FROM t WHERE c1=1", meta(new int[]{-1}, new int[]{1})),
                Arguments.of("SELECT * FROM t WHERE c1=1+1", meta(new int[]{-1}, new int[]{2})),
                // the first condition goes into key lookup other into a post-lookup filter.
                Arguments.of("SELECT * FROM t WHERE c1=? and c1=?", meta(0)),
                Arguments.of("SELECT * FROM t WHERE c2=? and c1=? and c1=?", meta(1)),

                // KV PUT
                Arguments.of("INSERT INTO t VALUES(?, ?, 0)", metaTrackingRequired(0)),
                Arguments.of("INSERT INTO t VALUES(1, ?, 0)", metaTrackingRequired(new int[]{-1}, new int[]{1})),
                Arguments.of("INSERT INTO t VALUES(1+1, ?, 0)", null),
                Arguments.of("INSERT INTO t(c2, c1) VALUES(?, ?)", metaTrackingRequired(1)),
                Arguments.of("INSERT INTO t(c2, c1) VALUES(1, ?)", metaTrackingRequired(0)),
                Arguments.of("INSERT INTO t(c2, c1) VALUES(?, 1)", metaTrackingRequired(new int[]{-1}, new int[]{1})),

                // KV DELETE
                Arguments.of("DELETE FROM t WHERE c1=?", metaTrackingRequired(0)),
                Arguments.of("DELETE FROM t WHERE c1=1", metaTrackingRequired(new int[]{-1}, new int[]{1})),
                Arguments.of("DELETE FROM t WHERE c1=1+1", metaTrackingRequired(new int[]{-1}, new int[]{2})),

                // SELECT 
                Arguments.of("SELECT * FROM t WHERE c1=?", meta(0)),
                Arguments.of("SELECT * FROM t WHERE c1=? AND c2=?", meta(0)),
                Arguments.of("SELECT c1, c2 FROM t WHERE c1=? UNION SELECT 1, x FROM SYSTEM_RANGE(1, 100)", meta(0)),
                Arguments.of("SELECT c1, c2 FROM t JOIN (SELECT x FROM SYSTEM_RANGE(1, 100)) r ON r.x=c2 WHERE c1 = ? ", meta(0)),
                Arguments.of("SELECT c1, c2 FROM t JOIN (SELECT x FROM SYSTEM_RANGE(1, 100)) r ON r.x=c2 WHERE c1 = 1 ", 
                        meta(new int[]{-1}, new int[]{1})),

                // Can refer multiple partitions - skip
                Arguments.of("SELECT * FROM t WHERE c1=1 or c1=2", null),
                Arguments.of("SELECT * FROM t WHERE c1 IN (?, 2)", null),
                Arguments.of("SELECT * FROM t WHERE c1=? AND c2 IN (SELECT * FROM SYSTEM_RANGE(1, 100))", meta(0)),

                // UPDATE
                Arguments.of("UPDATE t SET c2=1 WHERE c1=?", meta(new int[]{0}, new int[0], DirectTxMode.NOT_SUPPORTED)),
                Arguments.of("UPDATE t SET c2=? WHERE c1=1", meta(new int[]{-1}, new int[]{1}, DirectTxMode.NOT_SUPPORTED)),
                Arguments.of("UPDATE t SET c2=? WHERE c1=1 AND c3 IN (SELECT * FROM SYSTEM_RANGE(1, 100))",
                        meta(new int[]{-1}, new int[]{1}, DirectTxMode.NOT_SUPPORTED)),

                // Can refer multiple partitions - skip
                Arguments.of("UPDATE t SET c2=1 WHERE c1=1 or c1=2", null),
                Arguments.of("UPDATE t SET c2=? WHERE c1 IN (?, 2)", null),

                // INSERT  
                // TODO https://issues.apache.org/jira/browse/IGNITE-26203 No partition pruning metadata for INSERT INTO ... SELECT
                // Arguments.of("INSERT INTO t SELECT 1 as c1, 2 as c2, x as c3 FROM SYSTEM_RANGE(1, 100)",
                //        meta(new int[]{-1}, new int[]{1}), DirectTxMode.NOT_SUPPORTED),

                // DELETE
                Arguments.of("DELETE FROM t WHERE c1=1 AND c2 IN (SELECT * FROM SYSTEM_RANGE(1, 100)) ",
                        meta(new int[]{-1}, new int[]{1}, DirectTxMode.NOT_SUPPORTED)),
                // Can refer multiple partitions - skip
                Arguments.of("DELETE FROM t WHERE c1=1 or c1=2", null)
        );
    }

    @ParameterizedTest
    @MethodSource("tsKeyMetadata")
    public void tsCompoundKey(String query, PartitionAwarenessMetadata expected) {
        node.initSchema("CREATE TABLE t (c1 INT, c2 INT, c3 TIMESTAMP WITH LOCAL TIME ZONE, PRIMARY KEY (c3, c2))");

        QueryPlan plan = node.prepare(query);
        expectMetadata(plan, expected);
    }

    private static Stream<Arguments> tsKeyMetadata() {
        return Stream.of(
                // KV GET
                Arguments.of("SELECT * FROM t WHERE c3=? and c2=?", meta(0, 1)),
                Arguments.of("SELECT * FROM t WHERE c2=? and c3 = TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 00:00:00'", null),

                // KV PUT
                Arguments.of("INSERT INTO t VALUES (?, ?, ?)", metaTrackingRequired(2, 1)),
                Arguments.of("INSERT INTO t (c1, c2, c3) VALUES (?, ?, TIMESTAMP WITH LOCAL TIME ZONE '1970-01-01 00:00:00')", null)
        );
    }

    @ParameterizedTest
    @MethodSource("shortKeyMetadata")
    public void shortKey(String query, PartitionAwarenessMetadata expected) {
        node.initSchema("CREATE TABLE t (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c2)) COLOCATE BY (c3)");

        QueryPlan plan = node.prepare(query);
        expectMetadata(plan, expected);
    }

    private static Stream<Arguments> shortKeyMetadata() {
        return Stream.of(
                // KV GET
                Arguments.of("SELECT * FROM t WHERE c3=? and c2=?", meta(0)),
                Arguments.of("SELECT * FROM t WHERE c2=? and c3=?", meta(1)),
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=?", meta(2)),
                Arguments.of("SELECT * FROM t WHERE c3=? and c1=? and c2=?", meta(0)),
                Arguments.of("SELECT * FROM t WHERE c3=? and c2=1", meta(0)),
                Arguments.of("SELECT * FROM t WHERE c1=1 and c2=? and c3=?", meta(1)),

                Arguments.of("SELECT * FROM t WHERE c3=3", meta(new int[]{-1}, new int[]{3})),
                Arguments.of("SELECT * FROM t WHERE c2=? and c3=3", meta(new int[]{-1}, new int[]{3})),
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=3", meta(new int[]{-1}, new int[]{3})),

                // KV PUT
                Arguments.of("INSERT INTO t VALUES (?, ?, ?)", metaTrackingRequired(2)),
                Arguments.of("INSERT INTO t (c1, c2, c3) VALUES (?, ?, ?)", metaTrackingRequired(2)),
                Arguments.of("INSERT INTO t (c3, c1, c2) VALUES (?, ?, ?)", metaTrackingRequired(0)),

                Arguments.of("INSERT INTO t (c1, c2, c3) VALUES (?, ?, 3)", metaTrackingRequired(new int[]{-1}, new int[]{3})),
                Arguments.of("INSERT INTO t (c1, c3, c2) VALUES (?, 3, ?)", metaTrackingRequired(new int[]{-1}, new int[]{3})),
                Arguments.of("INSERT INTO t (c3, c1, c2) VALUES (3, ?, ?)", metaTrackingRequired(new int[]{-1}, new int[]{3})),

                // KV DELETE
                Arguments.of("DELETE FROM t WHERE c3=? and c2=?", metaTrackingRequired(0)),
                Arguments.of("DELETE FROM t WHERE c2=? and c3=?", metaTrackingRequired(1)),
                Arguments.of("DELETE FROM t WHERE c3=? and c2=1", metaTrackingRequired(0)),
                Arguments.of("DELETE FROM t WHERE c2=? and c3=3",
                        meta(new int[]{-1}, new int[]{3}, DirectTxMode.SUPPORTED_TRACKING_REQUIRED)),

                // SELECT
                Arguments.of("SELECT * FROM t WHERE c3=3 AND c1 IN (SELECT * FROM SYSTEM_RANGE(1, 100))",
                        meta(new int[]{-1}, new int[]{3})
                ),

                // UPDATE
                Arguments.of("UPDATE t SET c1=? WHERE c3=? and c2=?",
                        meta(new int[]{1}, new int[0], DirectTxMode.NOT_SUPPORTED)
                ),
                Arguments.of("UPDATE t SET c1=? WHERE c3=1 and c2=?",
                        meta(new int[]{-1}, new int[]{1}, DirectTxMode.NOT_SUPPORTED)
                ),
                Arguments.of("UPDATE t SET c1=? WHERE c3=1 and c2 IN (SELECT * FROM SYSTEM_RANGE(1, 100))",
                        meta(new int[]{-1}, new int[]{1}, DirectTxMode.NOT_SUPPORTED)
                ),

                // INSERT
                // TODO https://issues.apache.org/jira/browse/IGNITE-26203 No partition pruning metadata for INSERT INTO ... SELECT
                // Arguments.of("INSERT INTO t SELECT 1 as c1, 2 as c2, 3 as c3 FROM SYSTEM_RANGE(1, 100)",
                //        meta(new int[]{-1}, new int[]{3}, DirectTxMode.NOT_SUPPORTED)
                // ),

                // DELETE
                Arguments.of("DELETE FROM t WHERE c3=3", meta(new int[]{-1}, new int[]{3}, DirectTxMode.NOT_SUPPORTED)),
                Arguments.of("DELETE FROM t WHERE c3=3 and c2 IN (SELECT * FROM SYSTEM_RANGE(1, 100))",
                        meta(new int[]{-1}, new int[]{3}, DirectTxMode.NOT_SUPPORTED))
        );
    }

    @ParameterizedTest
    @MethodSource("compoundKeyMetadata")
    public void compoundKey(String query, PartitionAwarenessMetadata expected) {
        node.initSchema("CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, PRIMARY KEY(c1, c2, c3)) COLOCATE BY (c3, c1, c2)");

        QueryPlan plan = node.prepare(query);
        expectMetadata(plan, expected);
    }

    private static Stream<Arguments> compoundKeyMetadata() {
        return Stream.of(
                // KV GET
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=?", meta(2, 0, 1)),
                Arguments.of("SELECT * FROM t WHERE c3=? and c1=? and c2=?", meta(0, 1, 2)),
                Arguments.of("SELECT * FROM t WHERE c3=? and c2=? and c1=?", meta(0, 2, 1)),

                Arguments.of("SELECT * FROM t WHERE c3=1 and c1=? and c2=?", meta(new int[]{-1, 0, 1}, new int[]{1})),
                Arguments.of("SELECT * FROM t WHERE c3=? and c1=1 and c2=?", meta(new int[]{0, -1, 1}, new int[]{1})),
                Arguments.of("SELECT * FROM t WHERE c3=? and c1=? and c2=1", meta(new int[]{0, 1, -1}, new int[]{1})),

                Arguments.of("SELECT * FROM t WHERE c3=1 and c1=1 and c2=?", meta(new int[]{-1, -2, 0}, new int[]{1, 1})),
                Arguments.of("SELECT * FROM t WHERE c3=1 and c1=1 and c2=1", meta(new int[]{-1, -2, -3}, new int[]{1, 1, 1})),

                Arguments.of("SELECT * FROM t WHERE c4=? and c1=? and c2=? and 1=? and c3=?", meta(4, 1, 2)),
                // duplicate condition goes to a post lookup filter.
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=? and c2=?", meta(2, 0, 1)),

                Arguments.of("SELECT * FROM t WHERE c1=1 and c2=? and c3=?", meta(new int[]{1, -1, 0}, new int[]{1})),
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=2 and c3=?", meta(new int[]{1, 0, -1}, new int[]{2})),
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=3", meta(new int[]{-1, 0, 1}, new int[]{3})),
                Arguments.of("SELECT * FROM t WHERE c1=1 and c2=2 and c3=3", meta(new int[]{-1, -2, -3}, new int[]{3, 1, 2})),

                // KV PUT
                Arguments.of("INSERT INTO t VALUES (1, ?, ?, ?)", metaTrackingRequired(new int[]{1, -1, 0}, new int[]{1})),
                Arguments.of("INSERT INTO t (c3, c2, c4, c1) VALUES (?, ?, ?, ?)", metaTrackingRequired(0, 3, 1)),
                Arguments.of("INSERT INTO t (c3, c2, c4, c1) VALUES (?, ?, 1, ?)", metaTrackingRequired(0, 2, 1)),
                Arguments.of("INSERT INTO t VALUES (1, 2, 3, 4)", metaTrackingRequired(new int[]{-1, -2, -3}, new int[]{3, 1, 2})),

                // KV DELETE
                Arguments.of("DELETE FROM t WHERE c1=? and c2=? and c3=?", metaTrackingRequired(2, 0, 1)),
                Arguments.of("DELETE FROM t WHERE c3=? and c1=? and c2=?", metaTrackingRequired(0, 1, 2)),
                Arguments.of("DELETE FROM t WHERE c3=? and c2=? and c1=?", metaTrackingRequired(0, 2, 1)),

                Arguments.of("DELETE FROM t WHERE c1=1 and c2=? and c3=?", metaTrackingRequired(new int[]{1, -1, 0}, new int[]{1})),
                Arguments.of("DELETE FROM t WHERE c1=? and c2=2 and c3=?", metaTrackingRequired(new int[]{1, 0, -1}, new int[]{2})),
                Arguments.of("DELETE FROM t WHERE c1=? and c2=? and c3=3", metaTrackingRequired(new int[]{-1, 0, 1}, new int[]{3})),

                // SELECT 
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=?",
                        meta(new int[]{2, 0, 1}, new int[]{}, DirectTxMode.SUPPORTED)
                ),
                Arguments.of("SELECT * FROM t WHERE c1=1 and c2=? and c3=?",
                        meta(new int[]{1, -1, 0}, new int[]{1}, DirectTxMode.SUPPORTED)
                ),
                Arguments.of("SELECT * FROM t WHERE c1=1 and c2=2 and c3=?",
                        meta(new int[]{0, -1, -2}, new int[]{1, 2}, DirectTxMode.SUPPORTED)
                ),
                Arguments.of("SELECT * FROM t WHERE c1=1 and c2=2 and c3=3",
                        meta(new int[]{-1, -2, -3}, new int[]{3, 1, 2}, DirectTxMode.SUPPORTED)
                ),
                // Multiple partitions - skip
                Arguments.of("SELECT * FROM t WHERE (c1=? and c2=? and c3=?) OR (c1=1 and c2=2 and c3=3)", null),
                // Not keys specified - no partition pruning metadata
                Arguments.of("SELECT * FROM t WHERE c2=? and c3=?", null),

                // INSERT
                // TODO https://issues.apache.org/jira/browse/IGNITE-26203 No partition pruning metadata for INSERT INTO ... SELECT
                // Arguments.of("INSERT INTO t SELECT * FROM t WHERE c1=1 and c2=2 and c3=3", null)

                // UPDATE
                Arguments.of("UPDATE t SET c4=? WHERE c1=? and c2=? and c3=?",
                        meta(new int[]{3, 1, 2}, new int[]{}, DirectTxMode.NOT_SUPPORTED)
                ),
                Arguments.of("UPDATE t SET c4=? WHERE c2=? and c3=3", null)
        );
    }

    private static PartitionAwarenessMetadata meta(int... dynamicParams) {
        return meta(dynamicParams, new int[0], DirectTxMode.SUPPORTED);
    }

    private static PartitionAwarenessMetadata meta(int[] dynamicParams, int[] toHash) {
        return meta(dynamicParams, toHash, DirectTxMode.SUPPORTED);
    }

    private static PartitionAwarenessMetadata meta(int[] dynamicParams, int[] toHash, DirectTxMode mode) {
        int[] hashes = new int[toHash.length];
        for (int i = 0; i < toHash.length; ++i) {
            hashes[i] = ColocationUtils.hash(toHash[i], NativeTypes.INT32);
        }
        return new PartitionAwarenessMetadata(1, dynamicParams, hashes, mode, QualifiedName.parse("PUBLIC.TBL"));
    }

    private static PartitionAwarenessMetadata metaTrackingRequired(int[] dynamicParams, int[] toHash) {
        return meta(dynamicParams, toHash, DirectTxMode.SUPPORTED_TRACKING_REQUIRED);
    }

    private static PartitionAwarenessMetadata metaTrackingRequired(int... dynamicParams) {
        return meta(dynamicParams, new int[0], DirectTxMode.SUPPORTED_TRACKING_REQUIRED);
    }

    private void expectMetadata(QueryPlan plan, PartitionAwarenessMetadata expected) {
        PartitionAwarenessMetadata metadata = plan.partitionAwarenessMetadata();

        if (plan instanceof ExplainablePlan) {
            ExplainablePlan explainablePlan = (ExplainablePlan) plan;
            log.info("Plan:\n{}", explainablePlan.explain());
        } else {
            log.info("Plan:{}", plan.getClass().getName());
        }

        expectMetadata(expected, metadata);
    }

    private static void expectMetadata(PartitionAwarenessMetadata expected, @Nullable PartitionAwarenessMetadata actual) {
        if (expected == null) {
            assertNull(actual, "Metadata should not be present");
        } else {
            assertNotNull(actual, "Metadata not found");

            CatalogTableDescriptor table = CLUSTER.catalogManager().latestCatalog().table("PUBLIC", "T");
            assertNotNull(table, "table");

            assertEquals(table.id(), actual.tableId(), "metadata tableId");
            assertEquals(expected.directTxMode(), actual.directTxMode(), "direct transaction support");
            assertEquals(
                    Arrays.stream(expected.indexes()).boxed().collect(Collectors.toList()),
                    Arrays.stream(actual.indexes()).boxed().collect(Collectors.toList()),
                    "indexes"
            );

            assertEquals(
                    Arrays.stream(expected.hash()).boxed().collect(Collectors.toList()),
                    Arrays.stream(actual.hash()).boxed().collect(Collectors.toList()),
                    "hashes"
            );
        }
    }
}
