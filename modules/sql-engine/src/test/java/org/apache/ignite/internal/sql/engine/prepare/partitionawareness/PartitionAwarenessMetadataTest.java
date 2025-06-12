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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.commands.DropTableCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.framework.TestCluster;
import org.apache.ignite.internal.sql.engine.framework.TestNode;
import org.apache.ignite.internal.sql.engine.prepare.QueryPlan;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests forr {@link PartitionAwarenessMetadata} in query plans.
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

        int version = CLUSTER.catalogManager().latestCatalogVersion();

        List<CatalogCommand> commands = new ArrayList<>();
        for (CatalogTableDescriptor table : CLUSTER.catalogManager().catalog(version).tables()) {
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
            "SELECT count(*) FROM t WHERE c1=?",
            "UPDATE t SET c2=1 WHERE c1=?",
            "DELETE FROM t WHERE c1=?",
    })
    public void noMetadata(String query) {
        node.initSchema("CREATE TABLE t (c1 INT PRIMARY KEY, c2 INT)");

        QueryPlan plan = node.prepare(query);
        PartitionAwarenessMetadata metadata = plan.partitionAwarenessMetadata();
        assertNull(metadata);
    }

    @ParameterizedTest
    @MethodSource("simpleKeyMetadata")
    public void simpleKey(String query, List<Integer> dynamicParams) {
        node.initSchema("CREATE TABLE t (c1 INT PRIMARY KEY, c2 INT)");

        QueryPlan plan = node.prepare(query);
        PartitionAwarenessMetadata metadata = plan.partitionAwarenessMetadata();

        if (dynamicParams == null) {
            assertNull(metadata);
        } else {
            assertNotNull(metadata, "Expected no metadata");
            assertEquals(dynamicParams, IntStream.of(metadata.indexes()).boxed().collect(Collectors.toList()));
        }
    }

    private static Stream<Arguments> simpleKeyMetadata() {
        return Stream.of(
                // KV GET
                Arguments.of("SELECT * FROM t WHERE c1=?", List.of(0)),
                Arguments.of("SELECT * FROM t WHERE c2=? and c1=?", List.of(1)),
                Arguments.of("SELECT * FROM t WHERE c1=1", null),
                Arguments.of("SELECT * FROM t WHERE c1=1+1", null),
                // the first condition goes into key lookup other into a post-lookup filter.
                Arguments.of("SELECT * FROM t WHERE c1=? and c1=?", List.of(0)),
                Arguments.of("SELECT * FROM t WHERE c2=? and c1=? and c1=?", List.of(1)),

                // KV PUT
                Arguments.of("INSERT INTO t VALUES(?, ?)", List.of(0)),
                Arguments.of("INSERT INTO t VALUES(1, ?)", null),
                Arguments.of("INSERT INTO t VALUES(1+1, ?)", null),
                Arguments.of("INSERT INTO t(c2, c1) VALUES(?, ?)", List.of(1)),
                Arguments.of("INSERT INTO t(c2, c1) VALUES(1, ?)", List.of(0)),
                Arguments.of("INSERT INTO t(c2, c1) VALUES(?, 1)", null)
        );
    }

    @ParameterizedTest
    @MethodSource("compoundKeyMetadata")
    public void compoundKey(String query, List<Integer> dynamicParams) {
        node.initSchema("CREATE TABLE t (c1 INT, c2 INT, c3 INT, c4 INT, PRIMARY KEY(c1, c2, c3)) COLOCATE BY (c3, c1, c2)");

        QueryPlan plan = node.prepare(query);
        PartitionAwarenessMetadata metadata = plan.partitionAwarenessMetadata();

        if (dynamicParams == null) {
            assertNull(metadata);
        } else {
            assertNotNull(metadata, "Expected no metadata");
            assertEquals(dynamicParams, IntStream.of(metadata.indexes()).boxed().collect(Collectors.toList()));
        }
    }

    private static Stream<Arguments> compoundKeyMetadata() {
        return Stream.of(
                // KV GET
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=?", List.of(2, 0, 1)),
                Arguments.of("SELECT * FROM t WHERE c3=? and c1=? and c2=?", List.of(0, 1, 2)),
                Arguments.of("SELECT * FROM t WHERE c3=? and c2=? and c1=?", List.of(0, 2, 1)),
                Arguments.of("SELECT * FROM t WHERE c4=? and c1=? and c2=? and 1=? and c3=?", List.of(4, 1, 2)),
                // duplicate condition goes to a post lookup filter.
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=? and c2=?", List.of(2, 0, 1)),

                Arguments.of("SELECT * FROM t WHERE c1=1 and c2=? and c3=?", null),
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=2 and c3=?", null),
                Arguments.of("SELECT * FROM t WHERE c1=? and c2=? and c3=3", null),
                Arguments.of("SELECT * FROM t WHERE c1=1 and c2=2 and c3=3", null),

                // KV PUT
                Arguments.of("INSERT INTO t VALUES (?, ?, ?, ?)",  List.of(2, 0, 1)),
                Arguments.of("INSERT INTO t (c3, c2, c4, c1) VALUES (?, ?, ?, ?)", List.of(0, 3, 1)),
                Arguments.of("INSERT INTO t (c3, c2, c4, c1) VALUES (?, ?, 1, ?)", List.of(0, 2, 1))
        );
    }
}
