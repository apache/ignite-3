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
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.placementdriver.AssignmentsTracker;
import org.apache.ignite.internal.placementdriver.leases.Lease;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.metrics.PlacementDriverMetricSource.Holder;

/**
 * Metric source for placement driver metrics.
 */
public class PlacementDriverMetricSource extends AbstractMetricSource<Holder> {
    /** Source name. */
    public static final String SOURCE_NAME = "placement-driver";

    private final LeaseTracker leaseTracker;
    private final AssignmentsTracker assignmentsTracker;

    public PlacementDriverMetricSource(
            LeaseTracker leaseTracker,
            AssignmentsTracker assignmentsTracker
    ) {
        super(SOURCE_NAME, "Placement driver metrics.");

        this.leaseTracker = leaseTracker;
        this.assignmentsTracker = assignmentsTracker;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /** Holder. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        private final IntGauge acceptedLeases = new IntGauge(
                "AcceptedLeases",
                "Number of accepted leases.",
                () -> numberOfLeases(true)
        );

        private final IntGauge leaseNegotiations = new IntGauge(
                "LeaseNegotiations",
                "Number of leases under negotiation.",
                () -> numberOfLeases(false)
        );

        private final IntGauge replicationGroups = new IntGauge(
                "ReplicationGroups",
                "Current number of replication groups.",
                () -> assignmentsTracker.stableAssignments().size()
        );

        private final List<Metric> metrics = List.of(
                acceptedLeases,
                leaseNegotiations,
                replicationGroups
        );

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }

        private int numberOfLeases(boolean accepted) {
            int count = 0;

            for (Lease lease : leaseTracker.leasesCurrent().leaseByGroupId().values()) {
                if (lease != null && accepted == lease.isAccepted()) {
                    count++;
                }
            }

            return count;
        }
    }
}
