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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.Metric;

/**
 * Metric source for indexes build on the current node.
 */
public class IndexBuilderMetricSource extends AbstractMetricSource<IndexBuilderMetricSource.Holder> {
    public static final String METRIC_GROUP = "index.builder";

    private final Map<IndexBuildTaskId, Integer> transactionsToResolveByTaskId = new ConcurrentHashMap<>();
    private final Set<IndexBuildTaskId> indexesReading = ConcurrentHashMap.newKeySet();
    private final Set<IndexBuildTaskId> indexesWaitingForResponse = ConcurrentHashMap.newKeySet();
    // Using separate instead of sum to avoid returning incorrect number of indexes while index changes state.
    private final Set<IndexBuildTaskId> indexesBuilding = ConcurrentHashMap.newKeySet();

    /** Constructor. */
    public IndexBuilderMetricSource() {
        super(METRIC_GROUP, "Index builder metrics.");
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    void onBatchProcessingStarted(IndexBuildTaskId taskId) {
        indexesBuilding.add(taskId);
        indexesReading.add(taskId);
    }

    void onTransitionToWaitingForTransactions(IndexBuildTaskId taskId, int transactionsToWait) {
        indexesReading.remove(taskId);
        transactionsToResolveByTaskId.put(taskId, transactionsToWait);
    }

    void onTransitionToWaitingForReplicaResponse(IndexBuildTaskId taskId) {
        transactionsToResolveByTaskId.remove(taskId);
        indexesWaitingForResponse.add(taskId);
    }

    void onBatchProcessingFinished(IndexBuildTaskId taskId) {
        indexesReading.remove(taskId);
        indexesWaitingForResponse.remove(taskId);
        transactionsToResolveByTaskId.remove(taskId);
        indexesBuilding.remove(taskId);
    }

    /**
     * Holder for the index builder metrics.
     */
    public class Holder implements AbstractMetricSource.Holder<Holder> {
        private final IntGauge totalIndexesBuilding = (
                new IntGauge(
                        "TotalIndexesBuilding",
                        "Total number of indexes that node builds at the moment.",
                        indexesBuilding::size
                ));

        private final IntGauge indexesReadingStorage = (
                new IntGauge(
                        "IndexesReadingStorage",
                        "Number of indexes that are currently reading data from storage.",
                        indexesReading::size
                ));

        private final IntGauge indexesWaitingForTransaction = (
                new IntGauge(
                        "IndexesWaitingForTransactions",
                        "Number of indexes that are currently waiting for transactions to complete.",
                        transactionsToResolveByTaskId::size
                ));

        private final IntGauge transactionsWaitingFor = (
                new IntGauge(
                        "TransactionsWaitingFor",
                        "Number of transactions that indexes are currently waiting for.",
                        () -> transactionsToResolveByTaskId.values().stream().mapToInt(Integer::intValue).sum()
                ));

        private final IntGauge indexesWaitingForReplica = (
                new IntGauge(
                        "IndexesWaitingForReplica",
                        "Number of indexes that are currently waiting for replica response.",
                        indexesWaitingForResponse::size
                ));

        private final List<Metric> metrics = List.of(
                totalIndexesBuilding,
                indexesReadingStorage,
                indexesWaitingForTransaction,
                transactionsWaitingFor,
                indexesWaitingForReplica
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
