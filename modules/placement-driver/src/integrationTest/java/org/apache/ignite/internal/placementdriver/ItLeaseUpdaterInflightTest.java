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

package org.apache.ignite.internal.placementdriver;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.sleep;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the lease updater's inflight futures.
 */
public class ItLeaseUpdaterInflightTest extends ClusterPerTestIntegrationTest {
    private static final String TEST_ZONE = "TEST_ZONE";
    private static final String TEST_TABLE = "TEST_TABLE";
    private static final String LEASE_EXPIRATION_INTERVAL_MILLIS_STR = "2000";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    public void setup() {
        sql("CREATE ZONE " + TEST_ZONE + " (partitions 1, replicas 1) storage profiles ['" + CatalogService.DEFAULT_STORAGE_PROFILE + "']");
        sql("CREATE TABLE " + TEST_TABLE + " (ID INT PRIMARY KEY, VAL VARCHAR(20)) ZONE " + TEST_ZONE);
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        super.customizeInitParameters(builder);

        builder.clusterConfiguration("ignite {"
                + "  replication.leaseExpirationIntervalMillis: " + LEASE_EXPIRATION_INTERVAL_MILLIS_STR
                + "}");
    }

    @Test
    public void test() {
        AtomicInteger msInflightCount = new AtomicInteger();
        AtomicBoolean stopped = new AtomicBoolean();

        IgniteImpl node = anyNode();

        int zoneId = unwrapTableImpl(node.tables().table(TEST_TABLE)).zoneId();
        ZonePartitionId partId = new ZonePartitionId(zoneId, 0);

        ReplicaMeta replicaMeta = waitAndGetPrimaryReplica(node, partId);

        log.info("Test: zoneId={}, leaseStartTime={}", zoneId, replicaMeta.getStartTime());

        String testKey = "testKey";
        node.metaStorageManager().registerPrefixWatch(new ByteArray(testKey), event -> {
            sleep(10);

            msInflightCount.decrementAndGet();

            return nullCompletedFuture();
        });

        runAsync(() -> {
            while (!stopped.get()) {
                if (msInflightCount.get() > 300) {
                    continue;
                }

                msInflightCount.incrementAndGet();
                node.metaStorageManager().put(new ByteArray(testKey), "testValue".getBytes(StandardCharsets.UTF_8));
            }
        });

        try {
            sleep(Long.parseLong(LEASE_EXPIRATION_INTERVAL_MILLIS_STR) * 5);

            ReplicaMeta newReplicaMeta = waitAndGetPrimaryReplica(node, partId);
            log.info("Test: newLease={}", newReplicaMeta);

            assertEquals(replicaMeta.getStartTime().longValue(), newReplicaMeta.getStartTime().longValue());
        } finally {
            stopped.set(true);
        }
    }

    private static ReplicaMeta waitAndGetPrimaryReplica(IgniteImpl node, ZonePartitionId replicationGrpId) {
        CompletableFuture<ReplicaMeta> primaryReplicaFut = node.placementDriver().awaitPrimaryReplica(
                replicationGrpId,
                node.clock().now(),
                10,
                SECONDS
        );

        assertThat(primaryReplicaFut, willCompleteSuccessfully());

        return primaryReplicaFut.join();
    }
}
