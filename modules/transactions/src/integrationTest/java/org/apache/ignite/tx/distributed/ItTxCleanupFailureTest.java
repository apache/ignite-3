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

package org.apache.ignite.tx.distributed;

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.tx.metrics.TransactionMetricsSource.METRIC_PENDING_WRITE_INTENTS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.metrics.LongMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.tx.message.WriteIntentSwitchReplicaRequest;
import org.apache.ignite.internal.tx.metrics.TransactionMetricsSource;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for transaction cleanup failure.
 */
public class ItTxCleanupFailureTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    private static final String TABLE_NAME = "test_table";
    private static final int REPLICAS = 3;

    @BeforeEach
    public void setup() {
        String zoneSql = "create zone test_zone (partitions 1, replicas " + REPLICAS
                + ") storage profiles ['" + DEFAULT_STORAGE_PROFILE + "']";
        String tableSql = "create table " + TABLE_NAME + " (key bigint primary key, val varchar(20)) zone TEST_ZONE";

        sql(zoneSql);
        sql(tableSql);
    }

    @Test
    public void testRetry() {
        IgniteImpl node = anyNode();
        Transaction tx = node.transactions().begin();
        node.sql().execute(tx, "insert into " + TABLE_NAME + " (key, val) values (1, 'val-1')");

        AtomicInteger failedCleanupAttempts = new AtomicInteger();

        for (IgniteImpl n : runningNodesIter()) {
            n.dropMessages((dest, msg) -> {
                if (msg instanceof WriteIntentSwitchReplicaRequest && failedCleanupAttempts.get() == 0) {
                    // Makes cleanup fail on write intent switch attempt with replication timeout, on the first attempt.
                    return failedCleanupAttempts.incrementAndGet() == 1;
                }

                return false;
            });
        }

        tx.commitAsync();

        await().timeout(5, TimeUnit.SECONDS).until(() -> failedCleanupAttempts.get() == 1);

        // Checks that cleanup finally succeeded.
        await().timeout(5, TimeUnit.SECONDS).until(() -> pendingWriteIntents(node) == 0);
    }

    private static long pendingWriteIntents(IgniteImpl node) {
        Iterable<Metric> metrics = node.metricManager()
                .metricSnapshot()
                .metrics()
                .get(TransactionMetricsSource.SOURCE_NAME);

        for (Metric m : metrics) {
            if (m.name().equals(METRIC_PENDING_WRITE_INTENTS)) {
                return ((LongMetric) m).value();
            }
        }

        fail();

        return -1;
    }
}
