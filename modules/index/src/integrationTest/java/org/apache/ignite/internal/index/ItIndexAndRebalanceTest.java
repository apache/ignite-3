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

package org.apache.ignite.internal.index;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.sql.engine.util.QueryChecker.containsIndexScan;
import static org.apache.ignite.internal.table.TableTestUtils.getTableIdStrict;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.sql.SqlCommon;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

/** Integration test to check the work with indexes on rebalancing. */
public class ItIndexAndRebalanceTest extends BaseSqlIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final String ZONE_NAME = zoneName(TABLE_NAME);

    private static final String INDEX_NAME = "TEST_INDEX";

    private static final String COLUMN_NAME = "SALARY";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @BeforeEach
    void setUp() {
        sql("DROP TABLE IF EXISTS " + TABLE_NAME);
        sql("DROP ZONE IF EXISTS " + ZONE_NAME);
    }

    @RepeatedTest(100)
    void testChangeReplicaCountWithoutRestartNodes() throws Exception {
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 2, 1);

        insertPeople(TABLE_NAME, new Person(0, "0", 10.0));

        createIndex(TABLE_NAME, INDEX_NAME, COLUMN_NAME);

        changeZoneReplicas(ZONE_NAME, 1);
        waitForStableAssignmentsChangeInMetastore(TABLE_NAME, 1, 0);
        insertPeople(TABLE_NAME, new Person(1, "1", 11.0));

        changeZoneReplicas(ZONE_NAME, 2);
        waitForStableAssignmentsChangeInMetastore(TABLE_NAME, 2, 0);
        insertPeople(TABLE_NAME, new Person(2, "2", 12.0));

        for (IgniteImpl node : CLUSTER.runningNodes().collect(toList())) {
            // TODO: IGNITE-21710 Understand why the check fails and returns 2 rows instead of 3 from the rebalancing node
            assertQuery(node, format("SELECT * FROM {} WHERE {} > 0.0", TABLE_NAME, COLUMN_NAME))
                    .matches(containsIndexScan(SqlCommon.DEFAULT_SCHEMA_NAME, TABLE_NAME, INDEX_NAME))
                    .returnRowCount(3)
                    .check();
        }
    }

    private static void changeZoneReplicas(String zoneName, int replicas) {
        sql(format("ALTER ZONE {} SET REPLICAS={}", zoneName, replicas));
    }

    private static void waitForStableAssignmentsChangeInMetastore(
            String tableName,
            int expReplicaCount,
            int partitionId
    ) throws Exception {
        IgniteImpl node = CLUSTER.aliveNode();

        int tableId = getTableIdStrict(node.catalogManager(), tableName, node.clock().nowLong());

        Set<Assignment>[] actualAssignmentsHolder = new Set[]{Set.of()};

        assertTrue(waitForCondition(() -> {
            CompletableFuture<Set<Assignment>> partitionAssignmentsFuture = partitionAssignments(
                    node.metaStorageManager(),
                    tableId,
                    partitionId
            );

            assertThat(partitionAssignmentsFuture, willCompleteSuccessfully());

            Set<Assignment> assignments = partitionAssignmentsFuture.join();

            actualAssignmentsHolder[0] = assignments;

            return assignments.size() == expReplicaCount;
        }, 3_000), format("Expected replica count {}, actual assignments {}", expReplicaCount, actualAssignmentsHolder));
    }
}
