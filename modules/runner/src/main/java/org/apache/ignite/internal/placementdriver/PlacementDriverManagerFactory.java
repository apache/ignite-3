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

package org.apache.ignite.internal.placementdriver;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.placementdriver.metrics.PlacementDriverMetricSource;
import org.apache.ignite.internal.raft.RaftManager;
import org.apache.ignite.internal.raft.client.TopologyAwareRaftGroupServiceFactory;

/**
 * Factory class for creating instances of {@link PlacementDriverManager}.
 *
 * <p>This class is marked with the {@code @Factory} annotation, indicating
 * that it provides a method for producing objects used in the application.
 * It includes a static method to create a fully configured instance of
 * {@link PlacementDriverManager}.
 */
@Factory
public class PlacementDriverManagerFactory {
    /**
     * Creates an instance of {@link PlacementDriverManager} with the specified dependencies.
     *
     * @param metastore The meta storage manager responsible for metadata storage and retrieval.
     * @param clusterService The cluster service providing access to cluster-wide information and operations.
     * @param cmgManager The cluster management group manager responsible for cluster management operations.
     * @param raftManager The Raft manager responsible for managing Raft groups in the cluster.
     * @param topologyAwareRaftGroupServiceFactory The factory for creating Raft group services aware of topology.
     * @param failureProcessor The failure processor for handling node or service failures.
     * @param metricManager The metric manager used to collect and report application metrics.
     * @param leaseTracker The lease tracker managing lease state within the cluster.
     * @param assignmentsTracker The assignments tracker monitoring data partitions and replicas.
     * @param leaseUpdater The lease updater for modifying and updating lease information.
     * @param metricSource The metric source for exposing PlacementDriverManager-related metrics.
     * @return A fully configured instance of {@link PlacementDriverManager}.
     */
    @Singleton
    @Inject
    public static PlacementDriverManager create(
            MetaStorageManager metastore,
            ClusterService clusterService,
            ClusterManagementGroupManager cmgManager,
            RaftManager raftManager,
            TopologyAwareRaftGroupServiceFactory topologyAwareRaftGroupServiceFactory,
            FailureProcessor failureProcessor,
            MetricManager metricManager,
            LeaseTracker leaseTracker,
            AssignmentsTracker assignmentsTracker,
            LeaseUpdater leaseUpdater,
            PlacementDriverMetricSource metricSource
    ) {
        return new PlacementDriverManager(
                metastore,
                MetastorageGroupId.INSTANCE,
                clusterService,
                cmgManager,
                raftManager,
                topologyAwareRaftGroupServiceFactory,
                failureProcessor,
                metricManager,
                leaseTracker,
                assignmentsTracker,
                leaseUpdater,
                metricSource
        );
    }
}
