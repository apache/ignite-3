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

package org.apache.ignite.internal.tx.readonly;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.getDefaultZone;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.enabledColocation;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests about read-only transactions in the past.
 */
@SuppressWarnings("resource")
class ItReadOnlyTxInPastTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "test";

    @Override
    protected int initialNodes() {
        return 0;
    }

    @BeforeEach
    void prepareCluster() {
        // Setting idleSafeTimePropagationDuration to 1 second so that an RO tx has a potential to look before a table was created.
        cluster.startAndInit(1, builder -> builder.clusterConfiguration("ignite.replication.idleSafeTimePropagationDurationMillis: 1000"));

        cluster.doInSession(0, session -> {
            executeUpdate("CREATE TABLE " + TABLE_NAME + " (id int PRIMARY KEY, val varchar)", session);
        });
    }

    /**
     * Make sure that an explicit RO transaction does not look too far in the past (where the corresponding
     * table did not yet exist) even when the 'look in the past' optimization is enabled.
     */
    @Test
    void explicitReadOnlyTxDoesNotLookBeforeTableCreation() throws Exception {
        Ignite node = cluster.node(0);

        if (enabledColocation()) {
            // Generally it's required to await default zone dataNodesAutoAdjustScaleUp timeout in order to treat zone as ready one.
            // In order to eliminate awaiting interval, default zone scaleUp is altered to be immediate.
            setDefaultZoneAutoAdjustScaleUpTimeoutToImmediate();
        }

        // In case of empty assignments SQL engine will throw "Mandatory nodes was excluded from mapping: []".
        // In order to eliminate this assignments stabilization is needed, otherwise test may fail. Not related to collocation.
        // awaitAssignmentsStabilization awaits that the default zone/table stable partition assignments size
        // will be DEFAULT_PARTITION_COUNT * DEFAULT_REPLICA_COUNT. It's correct only for a single-node cluster that uses default zone,
        // that's why given method isn't located in a utility class.
        awaitAssignmentsStabilization(node);

        long count = node.transactions().runInTransaction(tx -> {
            return cluster.doInSession(0, session -> {
                try (ResultSet<SqlRow> resultSet = session.execute(tx, "SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    return resultSet.next().longValue(0);
                }
            });
        }, new TransactionOptions().readOnly(true));

        assertThat(count, is(0L));
    }

    /**
     * Make sure that an implicit RO transaction does not look too far in the past (where the corresponding
     * table did not yet exist) even when the 'look in the past' optimization is enabled.
     */
    @Test
    void implicitReadOnlyTxDoesNotLookBeforeTableCreation() {
        long count = cluster.query(0, "SELECT COUNT(*) FROM " + TABLE_NAME, rs -> rs.next().longValue(0));

        assertThat(count, is(0L));
    }

    private void setDefaultZoneAutoAdjustScaleUpTimeoutToImmediate() {
        IgniteImpl node = unwrapIgniteImpl(node(0));
        CatalogManager catalogManager = node.catalogManager();
        CatalogZoneDescriptor defaultZone = getDefaultZone(catalogManager, node.clock().nowLong());

        node(0).sql().executeScript(String.format("ALTER ZONE \"%s\"SET (AUTO SCALE UP 0)", defaultZone.name()));
    }

    private static void awaitAssignmentsStabilization(Ignite node) throws InterruptedException {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);
        TableImpl table = unwrapTableImpl(node.tables().table(TABLE_NAME));
        int tableOrZoneId = enabledColocation() ? table.zoneId() : table.tableId();

        HybridTimestamp timestamp = igniteImpl.clock().now();

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            int totalPartitionSize = 0;

            // Within given test default zone is used.
            for (int p = 0; p < DEFAULT_PARTITION_COUNT; p++) {
                CompletableFuture<TokenizedAssignments> assignmentsFuture = igniteImpl.placementDriver().getAssignments(
                        enabledColocation()
                                ? new ZonePartitionId(tableOrZoneId, p)
                                : new TablePartitionId(tableOrZoneId, p),
                        timestamp);

                assertThat(assignmentsFuture, willCompleteSuccessfully());

                totalPartitionSize += assignmentsFuture.join().nodes().size();
            }

            return totalPartitionSize == DEFAULT_PARTITION_COUNT * DEFAULT_REPLICA_COUNT;
        }, 10_000));
    }
}
