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

package org.apache.ignite.internal.metastorage.cache;

import static java.util.concurrent.TimeUnit.MINUTES;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.internal.IgniteNodeDetails;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.configuration.RaftExtensionConfiguration;

/**
 * Factory class for creating instances of the {@link IdempotentCacheVacuumizer}.
 * This factory ensures the proper initialization of the {@link IdempotentCacheVacuumizer} with the required dependencies.
 * The created vacuumizer handles the scheduled vacuumization of idempotent caches in an Ignite cluster.
 *
 * <p>The idempotent cache vacuumization involves the removal of old cache entries based on their Time-To-Live (TTL)
 * configuration. The vacuumizer schedules periodic actions where it computes the eviction timestamp using the system clock
 * and forwards it to the vacuumization action. Exceptions in the vacuumization process are logged without halting the scheduler,
 * ensuring the resilience of the vacuumization process.
 *
 * <p>This factory relies on dependency injection to supply the necessary parts for constructing a fully functional
 * {@link IdempotentCacheVacuumizer}, including:
 * - {@link IgniteNodeDetails}: Provides node-specific details like node name.
 * - {@link ScheduledExecutorService}: Scheduler for periodic execution of vacuumization actions.
 * - {@link MetaStorageManagerImpl}: Used to interact with the meta-storage manager for cache eviction.
 * - {@link ConfigurationRegistry}: Supplies configuration details for Raft and related parameters.
 * - {@link ClockService}: Provides time-related utilities, such as the current timestamp and clock skew.
 * - {@link FailureProcessor}: Handles errors and ensures fault tolerance during the vacuumization process.
 *
 * <p>Dependency injection annotations:
 * - {@link Singleton}: Ensures that the factory produces a single instance of the {@link IdempotentCacheVacuumizer}.
 * - {@link Inject}: Marks the dependencies required for constructing the {@link IdempotentCacheVacuumizer}.
 * - {@link Named} annotations are used for specifying particular configuration providers (e.g., "node").
 */
@Factory
public class IdempotentCacheVacuumizerFactory {
    /**
     * Creates an instance of {@link IdempotentCacheVacuumizer} with the provided dependencies.
     * This method initializes the vacuumizer, which is responsible for periodic cleanup of idempotent caches
     * based on their Time-To-Live (TTL) configuration. The vacuumizer computes eviction timestamps and schedules
     * vacuumization tasks to remove obsolete cache entries.
     *
     * @param nodeDetails Provides node-specific details, such as the node name.
     * @param commonScheduler A scheduler used for periodic execution of vacuumization tasks.
     * @param metaStorageManager Manager for meta storage operations, used for evicting idempotent commands cache.
     * @param nodeConfigRegistry A configuration registry providing Raft-specific configuration details.
     * @param clockService A clock utility for getting system time and managing clock-related utilities.
     * @param failureManager A processor for handling failures during the execution of vacuumization tasks.
     * @return An initialized {@link IdempotentCacheVacuumizer} instance.
     */
    @Singleton
    @Inject
    public static IdempotentCacheVacuumizer create(
            IgniteNodeDetails nodeDetails,
            ScheduledExecutorService commonScheduler,
            MetaStorageManagerImpl metaStorageManager,
            @Named("node") ConfigurationRegistry nodeConfigRegistry,
            ClockService clockService,
            FailureProcessor failureManager
    ) {
        RaftConfiguration raftConfiguration = nodeConfigRegistry.getConfiguration(RaftExtensionConfiguration.KEY).raft();

        var instance = new IdempotentCacheVacuumizer(
                nodeDetails.nodeName(),
                commonScheduler,
                metaStorageManager::evictIdempotentCommandsCache,
                raftConfiguration.retryTimeoutMillis(),
                clockService,
                failureManager,
                1,
                1,
                MINUTES
        );

        metaStorageManager.addElectionListener(instance);

        return instance;
    }
}
