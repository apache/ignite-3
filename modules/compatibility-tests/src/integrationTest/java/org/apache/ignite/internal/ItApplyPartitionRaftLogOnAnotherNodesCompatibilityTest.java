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
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Predicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** Partition Raft log compatibility tests when changing replicas. */
@ParameterizedClass
@MethodSource("baseVersions")
public class ItApplyPartitionRaftLogOnAnotherNodesCompatibilityTest extends CompatibilityTestBase {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "TEST_TABLE";

    @Override
    protected int nodesCount() {
        return 3;
    }

    @Override
    protected boolean restartWithCurrentEmbeddedVersion() {
        return false;
    }

    @Override
    protected void setupBaseVersion(Ignite baseIgnite) {
        String createZoneDdl = String.format(
                "CREATE ZONE %s WITH PARTITIONS=1, REPLICAS=1, STORAGE_PROFILES='default', DATA_NODES_FILTER='$[?(@.nodeIndex == \"0\")]'",
                ZONE_NAME
        );

        sql(baseIgnite, createZoneDdl);

        sql(baseIgnite, String.format("CREATE TABLE %s(ID INT PRIMARY KEY, VAL VARCHAR) ZONE %s", TABLE_NAME, ZONE_NAME));

        String insertDml = String.format("INSERT INTO %s (ID, VAL) VALUES (?, ?) ", TABLE_NAME);

        baseIgnite.transactions().runInTransaction(tx -> {
            for (int i = 0; i < 10; i++) {
                sql(baseIgnite, tx, insertDml, i, "str_" + i);
            }
        });
    }

    @Test
    void testIncreaseReplicas() throws Exception {
        cluster.stop();

        cluster.startEmbedded(nodesCount());

        sql(String.format("ALTER ZONE %s SET REPLICAS=3, DATA_NODES_FILTER='$..*'", ZONE_NAME));

        // Let's wait for replication to complete on other nodes.
        waitForZoneState(ZONE_NAME, zone -> zone.replicas() == 3);

        assertThat(sql(cluster.node(1), String.format("SELECT * FROM %s", TABLE_NAME)), hasSize(10));
        assertThat(sql(cluster.node(2), String.format("SELECT * FROM %s", TABLE_NAME)), hasSize(10));
    }

    private void waitForZoneState(String zoneName, Predicate<CatalogZoneDescriptor> predicate) throws InterruptedException {
        assertTrue(waitForCondition(() -> {
            boolean tested = true;

            for (Ignite n : cluster.nodes()) {
                IgniteImpl node = unwrapIgniteImpl(n);
                CatalogZoneDescriptor zone = node.catalogManager().activeCatalog(node.clock().currentLong()).zone(zoneName);
                tested = tested && predicate.test(zone);
            }

            return tested;
        }, 10_000));
    }
}
