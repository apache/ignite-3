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

package org.apache.ignite.internal.cluster.management.metrics;

import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.cluster.management.ClusterTag;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.ObjectGauge;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * The source of topology metrics.
 */
public class TopologyMetricsSource extends AbstractMetricSource<TopologyMetricsSource.Holder> {
    /** Source name. */
    static final String SOURCE_NAME = "topology";

    /** Logical topology. */
    private final LogicalTopology logicalTopology;

    /** Physical topology. */
    private final TopologyService physicalTopology;

    /** Provider of the cluster's tag. */
    private final Supplier<ClusterTag> clusterTagSupplier;

    /**
     * Creates a new instance of the topology metrics source.
     *
     * @param physicalTopology Physical topology.
     * @param logicalTopology Logical topology.
     * @param clusterTagSupplier Supplier of the cluster's tag.
     */
    public TopologyMetricsSource(
            TopologyService physicalTopology,
            LogicalTopology logicalTopology,
            Supplier<ClusterTag> clusterTagSupplier
    ) {
        super(SOURCE_NAME);

        this.physicalTopology = physicalTopology;
        this.logicalTopology = logicalTopology;
        this.clusterTagSupplier = clusterTagSupplier;
    }

    @Override
    protected Holder createHolder() {
        return new Holder();
    }

    /**
     * Returns name of the local node.
     *
     * @return Name of the local node, or an empty string if the holder is not initialized.
     */
    public String localNodeName() {
        Holder h = holder();

        if (h == null) {
            return "";
        }

        return h.localNodeName.value();
    }

    /**
     * Returns the unique identifier of the local node.
     *
     * @return Unique identifier of the local node, or {@code null} if the holder is not initialized.
     */
    public @Nullable UUID localNodeId() {
        Holder h = holder();

        if (h == null) {
            return null;
        }

        return h.localNodeId.value();
    }

    /**
     * Returns version of the local node.
     *
     * @return Version of the local node, or empty string if the holder is not initialized.
     */
    public String localNodeVersion() {
        Holder h = holder();

        if (h == null) {
            return "";
        }

        return h.localNodeVersion.value();
    }

    /**
     * Returns name of the cluster.
     *
     * @return Name of the cluster, or empty string if the holder is not initialized.
     */
    public String clusterName() {
        Holder h = holder();

        if (h == null) {
            return "";
        }

        return h.clusterName.value();
    }

    /**
     * Returns the unique identifier of the cluster.
     *
     * @return Returns the unique identifier of the cluster, or {@code null} if the holder is not initialized.
     */
    public @Nullable UUID clusterId() {
        Holder h = holder();

        if (h == null) {
            return null;
        }

        return h.clusterId.value();
    }

    /**
     * Returns the total number of nodes in the logical topology.
     *
     * @return Returns the total number of nodes in the logical topology, or {@code 0} if the holder is not initialized.
     */
    public int totalNodes() {
        Holder h = holder();

        if (h == null) {
            return 0;
        }

        return h.clusterSize.value();
    }

    /** Holder. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        // Local node metrics.
        private final ObjectGauge<String> localNodeVersion = new ObjectGauge<>(
                "NodeVersion",
                "Ignite product version",
                IgniteProductVersion.CURRENT_VERSION::toString,
                String.class);

        private final ObjectGauge<UUID> localNodeId = new ObjectGauge<>(
                "NodeId",
                "Unique identifier of the local node",
                () -> physicalTopology.localMember().id(),
                UUID.class);

        private final ObjectGauge<String> localNodeName = new ObjectGauge<>(
                "NodeName",
                "Unique name of the local node",
                () -> physicalTopology.localMember().name(),
                String.class);

        // Cluster metrics.
        private final IntGauge clusterSize = new IntGauge(
                "TotalNodes",
                "Number of nodes in the logical topology",
                () -> logicalTopology.getLogicalTopology().nodes().size());

        private final ObjectGauge<UUID> clusterId = new ObjectGauge<>(
                "ClusterId",
                "Unique identifier of the cluster",
                () -> {
                    ClusterTag tag = clusterTagSupplier.get();

                    return tag != null ? tag.clusterId() : null;
                },
                UUID.class);

        private final ObjectGauge<String> clusterName = new ObjectGauge<>(
                "ClusterName",
                "Unique name of the cluster",
                () -> {
                    ClusterTag tag = clusterTagSupplier.get();

                    return tag != null ? tag.clusterName() : "";
                },
                String.class);

        private final List<Metric> metrics = List.of(clusterId, clusterName, clusterSize, localNodeName, localNodeId, localNodeVersion);

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
