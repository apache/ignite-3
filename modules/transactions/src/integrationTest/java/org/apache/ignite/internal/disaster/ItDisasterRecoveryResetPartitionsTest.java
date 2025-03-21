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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogManagerImpl.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.STABLE_ASSIGNMENTS_PREFIX;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.COLOCATION_FEATURE_FLAG;
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;

/** Tests for disaster recovery manager reset partition command. */
// TODO https://issues.apache.org/jira/browse/IGNITE-24332
@WithSystemProperty(key = COLOCATION_FEATURE_FLAG, value = "false")
public class ItDisasterRecoveryResetPartitionsTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "PERSON";

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void shouldResetWithLostPartitions() throws InterruptedException {
        String selectSql = "select * from " + TABLE_NAME;
        executeSql("CREATE TABLE " + TABLE_NAME + "(ID INTEGER PRIMARY KEY, NAME VARCHAR(100));");
        executeSql("INSERT INTO " + TABLE_NAME + "(ID, NAME) VALUES (1, 'Ed'), (2, 'Ann'), (3, 'Emma');");

        assertDoesNotThrow(() -> executeSql(selectSql));

        String nodeToStop = cluster.node(1).name();

        stopNode(nodeToStop);

        assertThrows(SqlException.class, () -> executeSql(selectSql), "Mandatory nodes was excluded from mapping:");

        CompletableFuture<Void> resetFuture = unwrapIgniteImpl(cluster.aliveNode()).disasterRecoveryManager()
                .resetPartitions(DEFAULT_ZONE_NAME, DEFAULT_SCHEMA_NAME, TABLE_NAME, Set.of());

        assertThat(resetFuture, willCompleteSuccessfully());

        assertTrue(waitForCondition(() -> !hasAssignmentsForNode(TABLE_NAME, nodeToStop), 10000));

        assertDoesNotThrow(() -> executeSql(selectSql));
    }

    private boolean hasAssignmentsForNode(String tableName, String nodeName) {
        IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());

        int tableId = unwrapTableViewInternal(ignite.tables().table(tableName)).tableId();

        Publisher<Entry> publisher = ignite.metaStorageManager().prefix(new ByteArray(STABLE_ASSIGNMENTS_PREFIX + tableId));

        CompletableFuture<List<Entry>> stableAssignmentsFuture = subscribeToList(publisher);

        assertThat(stableAssignmentsFuture, willCompleteSuccessfully());

        return stableAssignmentsFuture.join().stream()
                .map(entry -> Assignments.fromBytes(entry.value()).nodes())
                .flatMap(Collection::stream)
                .anyMatch(assignment -> nodeName.equals(assignment.consistentId()));
    }
}
