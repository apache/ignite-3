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
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.IgniteNodeDetails;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.placementdriver.leases.LeaseTracker;
import org.apache.ignite.internal.replicator.configuration.ReplicationConfiguration;
import org.apache.ignite.internal.replicator.configuration.ReplicationExtensionConfiguration;

/**
 * Factory class responsible for creating instances of {@code LeaseUpdater}.
 *
 * <p>The {@code LeaseUpdaterFactory} leverages dependency injection to provide
 * all required components for constructing a {@code LeaseUpdater}. This ensures
 * that all dependencies, such as cluster services, metadata storage management,
 * and configuration registries, are properly initialized and passed to the
 * created instance.
 *
 * <p>The factory integrates with the system framework for dependency injection,
 * ensuring that complex dependencies are resolved and provided efficiently.
 */
@Factory
public class LeaseUpdaterFactory {
    /**
     * Creates an instance of {@code LeaseUpdater} with the specified dependencies.
     *
     * @param nodeDetails Details of the Ignite node including node name and other relevant metadata.
     * @param clusterService Service for cluster communication and member discovery.
     * @param msManager Manager for metadata storage operations.
     * @param failureProcessor Processor for handling failures in the system.
     * @param topologyService Service for managing the logical topology of the cluster.
     * @param leaseTracker Tracker responsible for managing lease states and updates.
     * @param clockService Service providing clock synchronization and time-related utilities.
     * @param assignmentsTracker Tracker for managing assignment updates within the cluster.
     * @param clusterConfigRegistry Registry holding configuration settings scoped to the cluster.
     * @param executorService Executor service used for scheduling background tasks.
     * @return A fully initialized instance of {@code LeaseUpdater}.
     */
    @Singleton
    @Inject
    public static LeaseUpdater create(
            IgniteNodeDetails nodeDetails,
            ClusterService clusterService,
            MetaStorageManager msManager,
            FailureProcessor failureProcessor,
            LogicalTopologyService topologyService,
            LeaseTracker leaseTracker,
            ClockService clockService,
            AssignmentsTracker assignmentsTracker,
            @Named("distributed")ConfigurationRegistry clusterConfigRegistry,
            @Named("common")ScheduledExecutorService executorService
    ) {
        ReplicationConfiguration replicationConfig = clusterConfigRegistry
                .getConfiguration(ReplicationExtensionConfiguration.KEY).replication();

        return new LeaseUpdater(
                nodeDetails.nodeName(),
                clusterService,
                msManager,
                failureProcessor,
                topologyService,
                leaseTracker,
                clockService,
                assignmentsTracker,
                replicationConfig,
                executorService
        );
    }
}
