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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.metrics.TestMetricManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@link IndexBuilderMetricSource}. */
public class IndexBuilderMetricsTest extends BaseIgniteAbstractTest {
    private final IndexBuilderMetricSource metricSource = new IndexBuilderMetricSource();

    private final MetricManager metricManager = new TestMetricManager();

    @BeforeEach
    void setUp() {
        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);

        assertThat(metricManager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        metricManager.beforeNodeStop();

        assertThat(metricManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void testIndexBuildFlow() {
        int pendingTx = 5;

        metricSource.transitionToReadingRows();
        checkMetrics(1, 1, 0, 0, 0);

        metricSource.transitionToWaitingForTransactions(pendingTx);
        checkMetrics(1, 0, 1, pendingTx, 0);

        metricSource.transitionToWaitingForReplicaResponse(pendingTx);
        checkMetrics(1, 0, 0, 0, 1);

        metricSource.indexBuildFinished();
        checkMetrics(0, 0, 0, 0, 0);
    }

    @Test
    void testReadingRowsFailed() {
        metricSource.transitionToReadingRows();
        checkMetrics(1, 1, 0, 0, 0);

        metricSource.rowsReadError();
        checkMetrics(0, 0, 0, 0, 0);
    }

    @Test
    void testWaitingForTransactionsFailed() {
        int pendingTx = 3;

        metricSource.transitionToReadingRows();
        checkMetrics(1, 1, 0, 0, 0);

        metricSource.transitionToWaitingForTransactions(pendingTx);
        checkMetrics(1, 0, 1, pendingTx, 0);

        metricSource.waitingForTransactionsError(pendingTx);
        checkMetrics(0, 0, 0, 0, 0);
    }

    @Test
    void testWaitingForReplicaResponseFailed() {
        int pendingTx = 4;

        metricSource.transitionToReadingRows();
        checkMetrics(1, 1, 0, 0, 0);

        metricSource.transitionToWaitingForTransactions(pendingTx);
        checkMetrics(1, 0, 1, pendingTx, 0);

        metricSource.transitionToWaitingForReplicaResponse(pendingTx);
        checkMetrics(1, 0, 0, 0, 1);

        metricSource.indexBuildFinished();
        checkMetrics(0, 0, 0, 0, 0);
    }

    private void checkMetrics(int indexesBuilding, int readingRows, int waitingForTx, int transactionWaitingFor, int replicaResponse) {
        checkMetricValue("TotalIndexesBuilding", String.valueOf(indexesBuilding));
        checkMetricValue("IndexesReadingStorage", String.valueOf(readingRows));
        checkMetricValue("IndexesWaitingForTransactions", String.valueOf(waitingForTx));
        checkMetricValue("TransactionsWaitingFor", String.valueOf(transactionWaitingFor));
        checkMetricValue("IndexesWaitingForReplica", String.valueOf(replicaResponse));
    }

    private void checkMetricValue(String metricName, String exp) {
        MetricSet metricsSet = metricManager.metricSnapshot().metrics().get(metricSource.name());

        Metric metric = metricsSet.get(metricName);

        assertNotNull(metric, metricName);

        assertEquals(exp, metric.getValueAsString(), metricName);
    }
}
