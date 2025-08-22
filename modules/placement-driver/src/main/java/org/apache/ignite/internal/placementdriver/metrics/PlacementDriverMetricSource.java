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

package org.apache.ignite.internal.placementdriver.metrics;

import java.util.List;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.IntMetric;
import org.apache.ignite.internal.metrics.LongAdderMetric;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.placementdriver.metrics.PlacementDriverMetricSource.Holder;

/**
 * Metric source for placement driver metrics.
 */
public class PlacementDriverMetricSource extends AbstractMetricSource<Holder> {
    /** Source name. */
    public static final String SOURCE_NAME = "placement-driver";

    private final IntSupplier activeLeaseSupplier;
    private final IntSupplier leaseWithoutCandidatesSupplier;
    private final IntSupplier currentStableAssignmentSizeSupplier;
    private final IntSupplier currentPendingAssignmentSizeSupplier;

    /**
     * Constructor.
     *
     * @param activeLeaseSupplier Supplier for active leases count.
     * @param leaseWithoutCandidatesSupplier Supplier for leases without candidates count.
     * @param currentStableAssignmentSizeSupplier Supplier for the stable assignments count.
     * @param currentPendingAssignmentSizeSupplier Supplier for the pending assignments count.
     */
    public PlacementDriverMetricSource(
            IntSupplier activeLeaseSupplier,
            IntSupplier leaseWithoutCandidatesSupplier,
            IntSupplier currentStableAssignmentSizeSupplier,
            IntSupplier currentPendingAssignmentSizeSupplier
    ) {
        super(SOURCE_NAME, "Placement driver metrics.");

        this.activeLeaseSupplier = activeLeaseSupplier;
        this.leaseWithoutCandidatesSupplier = leaseWithoutCandidatesSupplier;
        this.currentStableAssignmentSizeSupplier = currentStableAssignmentSizeSupplier;
        this.currentPendingAssignmentSizeSupplier = currentPendingAssignmentSizeSupplier;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Is called on lease creation.
     */
    public void onLeaseCreate() {
        Holder holder = holder();
        if (holder != null) {
            holder.leasesCreated.increment();
        }
    }

    /**
     * Is called on lease prolongation.
     */
    public void onLeaseProlong() {
        Holder holder = holder();
        if (holder != null) {
            holder.leasesProlonged.increment();
        }
    }

    /**
     * Is called on lease publishing.
     */
    public void onLeasePublish() {
        Holder holder = holder();
        if (holder != null) {
            holder.leasesPublished.increment();
        }
    }

    /** Holder. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        private final LongAdderMetric leasesCreated = new LongAdderMetric(
                "LeasesCreated",
                "Total number of created leases."
        );

        private final LongAdderMetric leasesPublished = new LongAdderMetric(
                "LeasesPublished",
                "Total number of published leases."
        );

        private final LongAdderMetric leasesProlonged = new LongAdderMetric(
                "LeasesProlonged",
                "Total number of prolonged leases."
        );

        private final IntMetric activeLeaseCount = new IntGauge(
                "ActiveLeasesCount",
                "Number of currently active leases.",
                activeLeaseSupplier
        );

        private final IntMetric leaseWithoutCandidates = new IntGauge(
                "LeasesWithoutCandidates",
                "Number of leases without candidates currently existing.",
                leaseWithoutCandidatesSupplier
        );

        private final IntMetric currentStableAssignmentSize = new IntGauge(
                "CurrentStableAssignmentsSize",
                "Current size of stable assignments over all partitions.",
                currentStableAssignmentSizeSupplier
        );

        private final IntMetric currentPendingAssignmentSize = new IntGauge(
                "CurrentPendingAssignmentsSize",
                "Current size of pending assignments over all partitions.",
                currentPendingAssignmentSizeSupplier
        );

        private final List<Metric> metrics = List.of(
                leasesCreated,
                leasesPublished,
                leasesProlonged,
                leaseWithoutCandidates,
                activeLeaseCount,
                currentStableAssignmentSize,
                currentPendingAssignmentSize
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
