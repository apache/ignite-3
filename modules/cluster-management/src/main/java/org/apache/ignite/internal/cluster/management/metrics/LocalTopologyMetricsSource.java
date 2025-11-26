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
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.metrics.StringGauge;
import org.apache.ignite.internal.metrics.UuidGauge;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * The source of local node metrics.
 */
public class LocalTopologyMetricsSource extends AbstractMetricSource<LocalTopologyMetricsSource.Holder> {
    /** Source name. */
    static final String SOURCE_NAME = "topology.local";

    /** Physical topology. */
    private final TopologyService physicalTopology;

    /**
     * Creates a new instance of the local node metrics source.
     *
     * @param physicalTopology Physical topology.
     */
    public LocalTopologyMetricsSource(TopologyService physicalTopology) {
        super(SOURCE_NAME, "Local topology metrics.", "topology");

        this.physicalTopology = physicalTopology;
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

    /** Holder. */
    protected class Holder implements AbstractMetricSource.Holder<Holder> {
        // Local node metrics.
        private final StringGauge localNodeVersion = new StringGauge(
                "NodeVersion",
                "Ignite product version",
                IgniteProductVersion.CURRENT_VERSION::toString);

        private final UuidGauge localNodeId = new UuidGauge(
                "NodeId",
                "Unique identifier of the local node",
                () -> physicalTopology.localMember().id());

        private final StringGauge localNodeName = new StringGauge(
                "NodeName",
                "Unique name of the local node",
                () -> physicalTopology.localMember().name());

        private final List<Metric> metrics = List.of(localNodeName, localNodeId, localNodeVersion);

        @Override
        public Iterable<Metric> metrics() {
            return metrics;
        }
    }
}
