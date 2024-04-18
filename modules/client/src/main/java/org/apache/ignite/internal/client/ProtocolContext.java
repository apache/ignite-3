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

package org.apache.ignite.internal.client;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.client.IgniteClientFeatureNotSupportedByServerException;
import org.apache.ignite.internal.client.proto.ProtocolVersion;
import org.apache.ignite.network.ClusterNode;

/**
 * Protocol Context.
 */
public class ProtocolContext {
    /** Protocol version. */
    private final ProtocolVersion ver;

    /** Features. */
    private final Set<ProtocolBitmaskFeature> features;

    /** Server idle timeout. */
    private final long serverIdleTimeout;

    /** Cluster node. */
    private final ClusterNode clusterNode;

    /** Cluster id. */
    private final UUID clusterId;

    /** Cluster name. */
    private final String clusterName;

    /**
     * Constructor.
     *
     * @param ver Protocol version.
     * @param features Supported features.
     * @param serverIdleTimeout Server idle timeout.
     * @param clusterNode Cluster node.
     * @param clusterId Cluster id.
     * @param clusterName Cluster name.
     */
    public ProtocolContext(
            ProtocolVersion ver,
            EnumSet<ProtocolBitmaskFeature> features,
            long serverIdleTimeout,
            ClusterNode clusterNode,
            UUID clusterId,
            String clusterName
    ) {
        this.ver = ver;
        this.features = Collections.unmodifiableSet(features != null ? features : EnumSet.noneOf(ProtocolBitmaskFeature.class));
        this.serverIdleTimeout = serverIdleTimeout;
        this.clusterNode = clusterNode;
        this.clusterId = clusterId;
        this.clusterName = clusterName;
    }

    /**
     * Gets a value indicating whether a feature is supported.
     *
     * @param feature Feature.
     * @return {@code true} if bitmask protocol feature supported.
     */
    public boolean isFeatureSupported(ProtocolBitmaskFeature feature) {
        return features.contains(feature);
    }

    /**
     * Check that feature is supported by the server.
     *
     * @param feature Feature.
     * @throws IgniteClientFeatureNotSupportedByServerException If feature is not supported by the server.
     */
    public void checkFeatureSupported(ProtocolBitmaskFeature feature) throws IgniteClientFeatureNotSupportedByServerException {
        if (!isFeatureSupported(feature)) {
            throw new IgniteClientFeatureNotSupportedByServerException(feature.name());
        }
    }

    /**
     * Returns supported features.
     *
     * @return Supported features.
     */
    public Set<ProtocolBitmaskFeature> features() {
        return features;
    }

    /**
     * Returns protocol version.
     *
     * @return Protocol version.
     */
    public ProtocolVersion version() {
        return ver;
    }

    /**
     * Returns server idle timeout.
     *
     * @return Server idle timeout.
     */
    public long serverIdleTimeout() {
        return serverIdleTimeout;
    }

    /**
     * Returns cluster node.
     *
     * @return Cluster node.
     */
    public ClusterNode clusterNode() {
        return clusterNode;
    }

    /**
     * Returns cluster id.
     *
     * @return Cluster id.
     */
    public UUID clusterId() {
        return clusterId;
    }

    /**
     * Returns cluster name.
     *
     * @return Cluster name.
     */
    public String clusterName() {
        return clusterName;
    }
}
