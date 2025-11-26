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

package org.apache.ignite.internal.tx.metrics;

import java.util.List;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.AtomicIntMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.tx.metrics.ResourceVacuumMetrics.Holder;

/**
 * Metric source for resource vacuum manager.
 */
public class ResourceVacuumMetrics extends AbstractMetricSource<Holder> {
    /** Source name. */
    public static final String SOURCE_NAME = "resource.vacuum";

    public ResourceVacuumMetrics() {
        super(SOURCE_NAME, "Resource vacuum metrics.");
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Is called on volatile state vacuum.
     */
    public void onVolatileStateVacuum() {
        Holder holder = holder();
        if (holder != null) {
            holder.vacuumizedVolatileTxnMetaCount.increment();
        }
    }

    /**
     * Is called when transaction state is marked for vacuum.
     */
    public void onMarkedForVacuum() {
        Holder holder = holder();
        if (holder != null) {
            holder.markedForVacuumTransactionMetaCount.increment();
        }
    }

    /**
     * Is called on vacuum finish.
     *
     * @param persistentStatesVacuumizedCount Count of persistent states vacuumized in the finished vacuum round.
     * @param skippedForFurtherProcessingUnfinishedTxnsCount Count of transaction states skipped in the finished vacuum round.
     */
    public void onVacuumFinish(int persistentStatesVacuumizedCount, int skippedForFurtherProcessingUnfinishedTxnsCount) {
        Holder holder = holder();
        if (holder != null) {
            holder.vacuumizedPersistentTxnMetaCount.add(persistentStatesVacuumizedCount);
            holder.skippedForFurtherProcessingUnfinishedTxnsCount.value(skippedForFurtherProcessingUnfinishedTxnsCount);
        }
    }

    /** Holder. */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongAdderMetric vacuumizedVolatileTxnMetaCount = new LongAdderMetric(
                "VacuumizedVolatileTxnMetaCount",
                "Count of volatile transaction metas that have been vacuumized."
        );

        private final LongAdderMetric vacuumizedPersistentTxnMetaCount = new LongAdderMetric(
                "VacuumizedPersistentTransactionMetaCount",
                "Count of persistent transaction metas that have been vacuumized."
        );

        private final LongAdderMetric markedForVacuumTransactionMetaCount = new LongAdderMetric(
                "MarkedForVacuumTransactionMetaCount",
                "Count of transaction metas that have been marked for vacuum."
        );

        private final AtomicIntMetric skippedForFurtherProcessingUnfinishedTxnsCount = new AtomicIntMetric(
                "SkippedForFurtherProcessingUnfinishedTransactionCount",
                "The current number of unfinished transactions that are skipped by vacuumizer for further processing."
        );

        private final List<Metric> metrics = List.of(
                vacuumizedVolatileTxnMetaCount,
                vacuumizedPersistentTxnMetaCount,
                markedForVacuumTransactionMetaCount,
                skippedForFurtherProcessingUnfinishedTxnsCount
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
