/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.baseline;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.runner.ClusterView;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * Baseline manager is responsible for handling baseline related logic.
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
// TODO: https://issues.apache.org/jira/browse/IGNITE-14716 Adapt concept of baseline topology IEP-4.
@SuppressWarnings({"FieldCanBeLocal", "unused"})
public class BaselineManager implements IgniteComponent {
    /** Cluster configuration in order to handle and listen baseline specific configuration. */
    private final ClusterConfiguration clusterConfiguration;

    /** Cluster network service in order to retrieve information about current cluster nodes. */
    private final ClusterService clusterSvc;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * The constructor.
     *
     * @param clusterConfiguration Cluster configuration.
     * @param clusterSvc       Cluster network service.
     */
    public BaselineManager(
            ClusterConfiguration clusterConfiguration,
            ClusterService clusterSvc
    ) {
        this.clusterConfiguration = clusterConfiguration;
        this.clusterSvc = clusterSvc;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    /**
     * Gets all nodes which participant in baseline and may process user data.
     * TODO: delete this when main functionality of the rebalance will be implemented
     * TODO: https://issues.apache.org/jira/browse/IGNITE-16011
     *
     * @return All nodes which were in baseline.
     */
    public Collection<ClusterNode> nodes() {
        return clusterSvc.topologyService().allMembers();
    }

    /**
     * Gets all nodes which participant in baseline and may process user data.
     *
     * @return All nodes which were in baseline.
     */
    public Collection<ClusterNode> baselineNodes() throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            List<String> baselineNodeNames = List.of(clusterConfiguration.baselineNodes().value());

            return clusterSvc.topologyService().allMembers().stream()
                    .filter(node -> baselineNodeNames.contains(node.name()))
                    .collect(Collectors.toList());
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Sets the provided nodes as a new baseline of the cluster.
     *
     * @param baselineNodes Set of baseline nodes.
     * @throws NodeStoppingException If a node is stopping during the method invocation.
     */
    public void setBaseline(Set<String> baselineNodes) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            clusterConfiguration.change(clusterChange -> clusterChange.changeBaselineNodes(baselineNodes.toArray(new String[0]))).join();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Sets the listener on a baseline change.
     *
     * @param listener Listener for a baseline change.
     * @throws NodeStoppingException If a node is stopping during the method invocation.
     */
    public void listenBaselineChange(ConfigurationListener<ClusterView> listener) throws NodeStoppingException {
        if (!busyLock.enterBusy()) {
            throw new NodeStoppingException();
        }

        try {
            clusterConfiguration.listen(listener);
        } finally {
            busyLock.leaveBusy();
        }
    }
}

