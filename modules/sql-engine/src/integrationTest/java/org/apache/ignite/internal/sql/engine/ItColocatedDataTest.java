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

package org.apache.ignite.internal.sql.engine;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.engine.util.QueryChecker;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Tests colocated data. */
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "true")
public class ItColocatedDataTest extends BaseSqlIntegrationTest {
    @BeforeAll
    public static void beforeTestsStarted() throws InterruptedException {
        waitForDefaultZoneAssignments();
    }

    @ParameterizedTest
    @EnumSource(DisabledJoinRules.class)
    public void joinColocatedImplicitly(DisabledJoinRules rules) {
        try {
            sql("CREATE TABLE T1 (id INT PRIMARY KEY, c1 INT)");
            sql("CREATE TABLE T2 (id INT PRIMARY KEY, c1 INT)");

            assertQuery("SELECT * FROM T1 JOIN T2 USING (id)", rules.disabledRules)
                    .matches(QueryChecker.matchesOnce("Exchange"))
                    .matches(QueryChecker.matches("^Exchange.*Join.*"))
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS T1");
            sql("DROP TABLE IF EXISTS T2");
        }
    }

    @ParameterizedTest
    @EnumSource(DisabledJoinRules.class)
    public void joinColocatedExplicitly(DisabledJoinRules rules) {
        try {
            sql("CREATE TABLE T1 (id INT, c1 INT, PRIMARY KEY(id, c1)) COLOCATE BY (c1)");
            sql("CREATE TABLE T2 (id INT, c2 INT, PRIMARY KEY(id, c2)) COLOCATE BY (c2)");

            assertQuery("SELECT * FROM T1 JOIN T2 ON T1.c1 = T2.c2", rules.disabledRules)
                    .matches(QueryChecker.matchesOnce("Exchange"))
                    .matches(QueryChecker.matches("^Exchange.*Join.*"))
                    .check();

            assertQuery("SELECT * FROM T1 JOIN T2 ON T1.c1 = T2.c2 AND T1.id = 1", rules.disabledRules)
                    .matches(QueryChecker.matchesOnce("Exchange"))
                    .matches(QueryChecker.matches("^Exchange.*Join.*"))
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS T1");
            sql("DROP TABLE IF EXISTS T2");
        }
    }

    @ParameterizedTest
    @EnumSource(DisabledJoinRules.class)
    public void joinNonColocated(DisabledJoinRules rules) {
        try {
            sql("CREATE TABLE T1 (id INT, c1 INT, PRIMARY KEY(id, c1)) COLOCATE BY (c1)");
            sql("CREATE ZONE IF NOT EXISTS ZONE_TEST (PARTITIONS 1, REPLICAS 1) STORAGE PROFILES ['default']");
            sql("CREATE TABLE T2 (id INT, c2 INT, PRIMARY KEY(id, c2)) COLOCATE BY (c2) ZONE ZONE_TEST");

            assertQuery("SELECT * FROM T1 JOIN T2 ON T1.c1 = T2.c2", rules.disabledRules)
                    .matches(not(QueryChecker.matchesOnce("Exchange")))
                    .matches(QueryChecker.matches(".*Join.*Exchange.*"))
                    .check();

            assertQuery("SELECT * FROM T1 JOIN T2 ON T1.c1 = T2.id", rules.disabledRules)
                    .matches(not(QueryChecker.matchesOnce("Exchange")))
                    .matches(QueryChecker.matches(".*Join.*Exchange.*"))
                    .check();
        } finally {
            sql("DROP TABLE IF EXISTS T1");
            sql("DROP TABLE IF EXISTS T2");
            sql("DROP ZONE IF EXISTS ZONE_TEST");
        }
    }

    /**
     * Join type.
     */
    public enum DisabledJoinRules {
        NESTED_LOOP(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "MergeJoinConverter",
                "HashJoinConverter"
        ),

        MERGE(
                "CorrelatedNestedLoopJoin",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "HashJoinConverter"
        ),

        HASH(
                "MergeJoinConverter",
                "JoinCommuteRule",
                "NestedLoopJoinConverter",
                "CorrelatedNestedLoopJoin"
        );

        private final String[] disabledRules;

        DisabledJoinRules(String... disabledRules) {
            this.disabledRules = disabledRules;
        }
    }

    /**
     * Waits for initial default zone assignments to appear.
     */
    // TODO: remove this method after https://issues.apache.org/jira/browse/IGNITE-25283 has been fixed.
    private static void waitForDefaultZoneAssignments() throws InterruptedException {
        if (!enabledColocation()) {
            return;
        }

        IgniteImpl nodeImpl = unwrapIgniteImpl(CLUSTER.aliveNode());

        Catalog catalog = nodeImpl.catalogManager().catalog(nodeImpl.catalogManager().latestCatalogVersion());

        CatalogZoneDescriptor defaultZone = catalog.defaultZone();

        List<ZonePartitionId> partitionIds = IntStream.range(0, defaultZone.partitions())
                .mapToObj(partId -> new ZonePartitionId(defaultZone.id(), partId))
                .collect(toList());

        assertTrue(waitForCondition(() -> {
            HybridTimestamp now = nodeImpl.clock().now();

            CompletableFuture<List<TokenizedAssignments>> assignmentsFuture = nodeImpl.placementDriver().getAssignments(partitionIds, now);

            assertThat(assignmentsFuture, willCompleteSuccessfully());

            return assignmentsFuture.join().stream().noneMatch(assignments -> assignments.nodes().isEmpty());
        }, 15_000));
    }
}
