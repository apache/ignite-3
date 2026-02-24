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

package org.apache.ignite.internal.raftsnapshot;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.randomBytes;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.metrics.LongGauge;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ItLogStorageMetricsTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private Ignite node;

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeEach
    void prepare() {
        node = cluster.node(0);
    }

    @Test
    void totalLogStorageMetricIsUpdated() throws Exception {
        LongGauge totalLogStorageSize = totalLogStorageSizeGauge();

        int valueLength = 1_000_000;

        feedLogStorageWithBlob(valueLength);

        await().alias("Total log storage size should reach the expected value")
                .until(totalLogStorageSize::value, is(greaterThanOrEqualTo((long) valueLength)));
    }

    private LongGauge totalLogStorageSizeGauge() {
        MetricSet logStorageMetrics = unwrapIgniteImpl(node)
                .metricManager()
                .metricSnapshot()
                .metrics()
                .get("log.storage");
        assertThat(logStorageMetrics, is(notNullValue()));

        LongGauge totalLogStorageSize = logStorageMetrics.get("TotalLogStorageSize");
        assertThat(totalLogStorageSize, is(notNullValue()));

        return totalLogStorageSize;
    }

    private void feedLogStorageWithBlob(int valueLength) throws Exception {
        node.sql().executeScript("CREATE TABLE " + TABLE_NAME + "(ID INT PRIMARY KEY, VAL VARBINARY(" + valueLength + "))");

        node.tables().table(TABLE_NAME)
                .keyValueView(Integer.class, byte[].class)
                .put(1, randomBytes(new Random(), valueLength));

        DefaultLogStorageManager logStorageManager = (DefaultLogStorageManager) unwrapIgniteImpl(node).partitionsLogStorageManager();
        logStorageManager.flushSstFiles();
    }
}
