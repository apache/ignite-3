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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.colocationEnabled;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.table.TableImpl;

/**
 * Utilities to work with assignments in integration tests.
 */
public class AssignmentsTestUtils {
    /**
     * Waits for assignments stabilization: that is, for all partitions to get primary replicas at least once.
     *
     * @param node Node via which to operate.
     * @param tableName Name of the table which assignments (or which zone's assignments) to wait for.
     */
    public static void awaitAssignmentsStabilization(Ignite node, String tableName) throws InterruptedException {
        IgniteImpl igniteImpl = unwrapIgniteImpl(node);
        TableImpl table = unwrapTableImpl(node.tables().table(tableName));

        Catalog catalog = igniteImpl.catalogManager().catalog(igniteImpl.catalogManager().latestCatalogVersion());
        CatalogZoneDescriptor zone = catalog.zone(table.zoneId());
        assertNotNull(zone);

        HybridTimestamp timestamp = igniteImpl.clock().now();

        assertTrue(waitForCondition(() -> {
            int totalPartitionSize = 0;

            for (int p = 0; p < zone.partitions(); p++) {
                CompletableFuture<TokenizedAssignments> assignmentsFuture = igniteImpl.placementDriver().getAssignments(
                        colocationEnabled()
                                ? new ZonePartitionId(table.zoneId(), p)
                                : new TablePartitionId(table.tableId(), p),
                        timestamp);

                assertThat(assignmentsFuture, willCompleteSuccessfully());

                totalPartitionSize += assignmentsFuture.join().nodes().size();
            }

            return totalPartitionSize == zone.partitions() * zone.replicas();
        }, 10_000));
    }
}
