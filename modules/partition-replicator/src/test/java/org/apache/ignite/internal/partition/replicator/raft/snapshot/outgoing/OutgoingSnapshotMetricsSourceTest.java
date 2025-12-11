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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.metrics.MetricSet;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.ZonePartitionKey;
import org.junit.jupiter.api.Test;

/**
 * Tests metric source name and outgoing snapshot metric names.
 * If you want to change the name, or add a new metric, please remember to update the corresponding documentation.
 */
class OutgoingSnapshotMetricsSourceTest {
    @Test
    void snapshotMetricsSourceName() {
        UUID snapshotId = UUID.randomUUID();

        var metricSource = new OutgoingSnapshotMetricsSource(snapshotId, new ZonePartitionKey(0, 0));

        assertThat(metricSource.name(), is("snapshots.outgoing." + snapshotId));
    }

    @Test
    void testMetricNames() {
        var metricSource = new OutgoingSnapshotMetricsSource(UUID.randomUUID(), new ZonePartitionKey(0, 0));

        MetricSet set = metricSource.enable();

        assertThat(set, is(notNullValue()));

        Set<String> expectedMetrics = Set.of(
                "RowsSent",
                "RowVersionsSent",
                "TotalBytesSent",
                "OutOfOrderRowsSent",
                "OutOfOrderVersionsSent",
                "OutOfOrderTotalBytesSent",
                "TotalBatches",
                "MaxBatchProcessingTime",
                "MinBatchProcessingTime",
                "AvgBatchProcessingTime",
                "TotalSnapshotInstallationTime",
                "LastAppliedIndex",
                "LastAppliedTerm",
                "LearnerList",
                "PeerList",
                "CatalogVersion"
        );

        var actualMetrics = new HashSet<String>();
        set.forEach(m -> actualMetrics.add(m.name()));

        assertThat(actualMetrics, is(expectedMetrics));
    }
}
