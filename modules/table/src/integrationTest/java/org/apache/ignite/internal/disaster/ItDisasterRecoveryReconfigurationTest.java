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

import static java.lang.String.format;
import static java.util.Map.of;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.lang.RunnableX;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Tests for scenarios where pajority of peers is not available.
 */
public class ItDisasterRecoveryReconfigurationTest extends ClusterPerClassIntegrationTest {
    /** Scale-down timeout. */
    private static final int SCALE_DOWN_TIMEOUT_SECONDS = 2;

    /** Test table name. */
    private static final String TABLE_NAME = "test";

    /**  */
    private static final int ENTRIES = 5;

    private String zoneName;
    private int zoneId;

    @Override
    protected int initialNodes() {
        return 5;
    }

    @BeforeEach
    void setUp(TestInfo testInfo) {
        Method testMethod = testInfo.getTestMethod().orElseThrow();

        for (int i = 0; i < initialNodes(); i++) {
            if (!CLUSTER.isAlive(i)) {
                CLUSTER.startNode(i);
            }
        }

        IgniteImpl node0 = CLUSTER.node(0);

        zoneName = "ZONE_" + testMethod.getName().toUpperCase();

        ZoneParams zoneParams = testMethod.getAnnotation(ZoneParams.class);
        sql(format("CREATE ZONE %s with replicas=%d, partitions=%d, data_nodes_auto_adjust_scale_down=%d",
                zoneName, zoneParams.replicas(), zoneParams.partitions(), SCALE_DOWN_TIMEOUT_SECONDS
        ));

        CatalogZoneDescriptor zone = node0.catalogManager().zone(zoneName, node0.clock().nowLong());
        zoneId = requireNonNull(zone).id();

        sql(format("CREATE TABLE %s (id INT PRIMARY KEY, val INT) WITH PRIMARY_ZONE='%s'", TABLE_NAME, zoneName));
    }

    @AfterEach
    void tearDown() {
        if (CLUSTER.isAlive(0)) {
            sql(format("DROP TABLE IF EXISTS %s", TABLE_NAME));
            sql(format("DROP ZONE %s", zoneName));
        }
    }

    /**
     * Tests the scenario in which a 5-nodes cluster loses 2 nodes, making one of its partitions unavailable for writes.
     * In this situation write should not work, and there should be no automatic raft group re-configuration that would replicate the data
     * from a single remaining peer.
     */
    @Test
    @ZoneParams(replicas = 3, partitions = 2)
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21284")
    void testNoRebalanceIfMajorityIsLost1(TestInfo testInfo) throws Exception {
        Table table = CLUSTER.node(0).tables().table(TABLE_NAME);
        KeyValueView<Tuple, Tuple> kvView = table.keyValueView();
        for (int i = 0; i < ENTRIES; ++i) {
            kvView.put(null, Tuple.create(of("id", i)), Tuple.create(of("val", i)));
        }

        stopNodesInParallel(0, 1, 2, 3, 4);
        startNodesInParallel(0, 1, 2);

        IgniteImpl node0 = CLUSTER.node(0);
        waitForScaleDown(node0, 3);

        table = node0.tables().table(TABLE_NAME);
        kvView = table.keyValueView();

        for (int i = 0; i < ENTRIES; ++i) {
            try {
                assertNotNull(kvView.getAsync(null, Tuple.create(of("id", i))).get(500, MILLISECONDS));
            } catch (Exception ignore) {
                // Ignore regular exceptions, don't ignore AssertionErrors. "get" should either fail or return non-null value.
            }
        }

        List<Throwable> errors = insertValues(table);

        assertFalse(errors.isEmpty());
    }

    private static List<Throwable> insertValues(Table table) {
        List<Throwable> errors = new ArrayList<>();

        for (int i = 0; i < ENTRIES; ++i) {
            CompletableFuture<Void> fut = table.keyValueView()
                    .putAsync(null, Tuple.create(of("id", i)), Tuple.create(of("val", i)));

            try {
                fut.get(200, MILLISECONDS);
            } catch (Throwable e) {
                Throwable cause = unwrapCause(e);

                if (cause instanceof TransactionException
                        || cause instanceof TimeoutException
                        || cause instanceof IgniteException && cause.getMessage().contains("The primary replica has changed")
                ) {
                    errors.add(cause);
                } else {
                    fail("Unexpected exception", e);
                }
            }
        }
        return errors;
    }

    /**
     * It's important to stop nodes in parallel, not only to save time, but to remove them from data nodes at the same time
     * with a single scale-down event. Otherwise tests will start failing randomly.
     */
    private static void stopNodesInParallel(int... nodeIndexes) {
        runRace(IntStream.of(nodeIndexes).<RunnableX>mapToObj(i -> () -> CLUSTER.stopNode(i)).toArray(RunnableX[]::new));
    }

    private static void startNodesInParallel(int... nodeIndexes) {
        runRace(30_000, IntStream.of(nodeIndexes).<RunnableX>mapToObj(i -> () -> CLUSTER.startNode(i)).toArray(RunnableX[]::new));
    }

    private void waitForScaleDown(IgniteImpl node, int targetDataNodesCount) throws InterruptedException {
        DistributionZoneManager dzManager = node.distributionZoneManager();

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            long causalityToken = node.metaStorageManager().appliedRevision();

            long msSafeTime = node.metaStorageManager().timestampByRevision(causalityToken).longValue();
            int catalogVersion = node.catalogManager().activeCatalogVersion(msSafeTime);

            CompletableFuture<Set<String>> dataNodes = dzManager.dataNodes(causalityToken, catalogVersion, zoneId);

            try {
                return dataNodes.get(SCALE_DOWN_TIMEOUT_SECONDS, SECONDS).size() == targetDataNodesCount;
            } catch (Exception e) {
                return false;
            }
        }, 0, SECONDS.toMillis(SCALE_DOWN_TIMEOUT_SECONDS * 5)));
    }

    @Retention(RetentionPolicy.RUNTIME)
    @interface ZoneParams {
        int replicas();

        int partitions();
    }
}
