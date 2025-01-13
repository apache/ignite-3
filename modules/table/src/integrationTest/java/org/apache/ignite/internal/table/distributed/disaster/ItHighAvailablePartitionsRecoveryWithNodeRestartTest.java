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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

/** Test for the HA zones recovery with node restarts. */
class ItHighAvailablePartitionsRecoveryWithNodeRestartTest extends AbstractHighAvailablePartitionsRecoveryTest {
    /** How often we update the low water mark. */
    private static final long LW_UPDATE_TIME_MS = TimeUnit.MILLISECONDS.toMillis(500);

    /** It should be less than {@link #LW_UPDATE_TIME_MS} for the test to work. */
    private static final long CHECK_POINT_INTERVAL_MS = LW_UPDATE_TIME_MS / 2;

    /** Should be greater than 2 x {@link #LW_UPDATE_TIME_MS}. */
    private static final long COMPACTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(10);

    private static final String FAST_FAILURE_DETECTION_AND_FAST_CHECKPOINT_NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "    membership: {\n"
            + "      membershipSyncInterval: 1000,\n"
            + "      failurePingInterval: 500,\n"
            + "      scaleCube: {\n"
            + "        membershipSuspicionMultiplier: 1,\n"
            + "        failurePingRequestMembers: 1,\n"
            + "        gossipInterval: 10\n"
            + "      },\n"
            + "    }\n"
            + "  },\n"
            + "  storage: {\n"
            + "    engines: {\n"
            + "      aipersist: {\n"
            + "        checkpoint: {\n"
            + "          interval: " + CHECK_POINT_INTERVAL_MS + "\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "  clientConnector: { port:{} }, \n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    @Override
    protected int initialNodes() {
        return 3;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_AND_FAST_CHECKPOINT_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        // Configurations to short the catalog compaction time.
        String clusterConfiguration = format(
                "ignite {\n"
                        + "gc: {lowWatermark: {dataAvailabilityTime: {}, updateInterval: {}}},\n"
                        + "}",
                LW_UPDATE_TIME_MS * 2, LW_UPDATE_TIME_MS
        );

        builder.clusterConfiguration(clusterConfiguration);
    }

    @Test
    void testHaRecoveryOnZoneTimersRestoreAfterCatalogCompactionAndNodeRestart() throws InterruptedException {
        IgniteImpl node = igniteImpl(0);

        changePartitionDistributionTimeout(node, 10);

        createHaZoneWithTable();

        assertRecoveryKeyIsEmpty(node);

        // Await for catalog compaction
        int catalogVersion1 = getLatestCatalogVersion(node);
        expectEarliestCatalogVersion(catalogVersion1 - 1);

        stopNodes(2, 1, 0);

        IgniteImpl node1 = unwrapIgniteImpl(startNode(0));

        waitAndAssertRecoveryKeyIsNotEmpty(node1, 30_000);

        assertRecoveryRequestForHaZoneTable(node1);
        assertRecoveryRequestWasOnlyOne(node1);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node1, HA_TABLE_NAME, PARTITION_IDS, Set.of(node1.name()));
    }

    private static int getLatestCatalogVersion(Ignite ignite) {
        Catalog catalog = getLatestCatalog(ignite);

        return catalog.version();
    }

    private static Catalog getLatestCatalog(Ignite ignite) {
        IgniteImpl igniteImpl = unwrapIgniteImpl(ignite);
        CatalogManagerImpl catalogManager = ((CatalogManagerImpl) igniteImpl.catalogManager());

        Catalog catalog = catalogManager.catalog(catalogManager.activeCatalogVersion(igniteImpl.clock().nowLong()));

        Objects.requireNonNull(catalog);

        return catalog;
    }

    private void expectEarliestCatalogVersion(int expectedVersion) {
        Awaitility.await().timeout(COMPACTION_INTERVAL_MS, TimeUnit.SECONDS).untilAsserted(() -> {
            for (var node : runningNodes().collect(Collectors.toList())) {
                IgniteImpl ignite = unwrapIgniteImpl(node);
                CatalogManagerImpl catalogManager = ((CatalogManagerImpl) ignite.catalogManager());

                assertThat("The earliest catalog version does not match. ",
                        catalogManager.earliestCatalogVersion(), is(expectedVersion));
            }
        });
    }
}
