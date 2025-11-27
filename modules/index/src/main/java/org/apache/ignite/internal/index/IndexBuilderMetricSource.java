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
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.Metric;

/**
 * Metric source for indexes build on the current node.
 */
public class IndexBuilderMetricSource extends AbstractMetricSource<IndexBuilderMetricSource.Holder> {
    public static final String METRIC_GROUP = "index.builder";

    /**
     * Creates a new metric source for the given storage engine and table, with a name {@code storage} and group {@value METRIC_GROUP}.
     *
     */
    public IndexBuilderMetricSource() {
        super(METRIC_GROUP, "Index metrics.");
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    void onTransitionToReadingRows() {
        Holder holder = holder();
        if (holder != null) {
            holder.totalIndexesBuilding.increment();
            holder.indexesReadingStorage.increment();
        }
    }

    void onTransitionToWaitingForTransactions(int transactionsToWait) {
        Holder holder = holder();
        if (holder != null) {
            holder.indexesReadingStorage.decrement();
            holder.indexesWaitingForTransactionn.increment();
            holder.transactionsWaitingFor.add(transactionsToWait);
        }
    }

    void onRowsReadError() {
        Holder holder = holder();
        if (holder != null) {
            holder.indexesReadingStorage.decrement();
            holder.totalIndexesBuilding.decrement();
        }
    }

    void onWaitingForTransactionsError(int size) {
        Holder holder = holder();
        if (holder != null) {
            holder.totalIndexesBuilding.decrement();
            holder.indexesWaitingForTransactionn.decrement();
            holder.transactionsWaitingFor.add(-size);
        }
    }

    void onTransitionToWaitingForReplicaResponse(int size) {
        Holder holder = holder();
        if (holder != null) {
            holder.indexesWaitingForTransactionn.decrement();
            holder.transactionsWaitingFor.add(-size);
            holder.indexesWaitingForReplica.increment();
        }
    }

    void onIndexBuildFinished() {
        Holder holder = holder();
        if (holder != null) {
            holder.indexesWaitingForReplica.decrement();
            holder.totalIndexesBuilding.decrement();
        }
    }

    /**
     * Holder for the storage engine table metrics.
     */
    public static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final AtomicIntMetric totalIndexesBuilding = (
                new AtomicIntMetric(
                        "TotalIndexesBuilding",
                        "Total number of indexes that node builds at the moment."
                ));

        private final AtomicIntMetric indexesReadingStorage = (
                new AtomicIntMetric(
                        "IndexesReadingStorage",
                        "Number of indexes that are currently reading data from storage."
                ));

        private final AtomicIntMetric indexesWaitingForTransactionn = (
                new AtomicIntMetric(
                        "IndexesWaitingForTransactions",
                        "Number of indexes that are currently waiting for transactions to complete."
                ));

        private final AtomicIntMetric transactionsWaitingFor = (
                new AtomicIntMetric(
                        "TransactionsWaitingFor",
                        "Number of transactions that indexes are currently waiting for."
                ));

        private final AtomicIntMetric indexesWaitingForReplica = (
                new AtomicIntMetric(
                        "IndexesWaitingForReplica",
                        "Number of indexes that are currently waiting for replica response."
                ));

        private final List<Metric> metrics = List.of(
                totalIndexesBuilding,
                indexesReadingStorage,
                indexesWaitingForTransactionn,
                transactionsWaitingFor,
                indexesWaitingForReplica
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
