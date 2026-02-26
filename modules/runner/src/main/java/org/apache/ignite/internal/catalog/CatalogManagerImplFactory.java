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

package org.apache.ignite.internal.catalog;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationConfiguration;
import org.apache.ignite.internal.catalog.configuration.SchemaSynchronizationExtensionConfiguration;
import org.apache.ignite.internal.catalog.storage.UpdateLogImpl;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.metastorage.MetaStorageManager;

/**
 * Factory class for creating instances of {@link CatalogManager}.
 * Uses dependency injection to provide required components and configuration
 * for the creation of a {@link CatalogManagerImpl}.
 */
@Factory
public class CatalogManagerImplFactory {
    /**
     * Creates an instance of {@link CatalogManager} using the given dependencies and configuration.
     *
     * @param metaStorageMgr MetaStorageManager instance used to handle metadata storage operations.
     * @param clockService ClockService instance used for handling time-related operations.
     * @param failureManager FailureManager instance used for managing failure scenarios.
     * @param clusterConfigRegistry ConfigurationRegistry instance providing access to cluster configuration.
     * @param partitionCountProviderWrapper Wrapper provider for partition count information.
     * @return A new instance of {@link CatalogManager}.
     */
    @Singleton
    @Inject
    public static CatalogManager create(
            MetaStorageManager metaStorageMgr,
            ClockService clockService,
            FailureManager failureManager,
            @Named("distributed") ConfigurationRegistry clusterConfigRegistry,
            PartitionCountProviderWrapper partitionCountProviderWrapper
    ) {
        SchemaSynchronizationConfiguration schemaSyncConfig = clusterConfigRegistry
                .getConfiguration(SchemaSynchronizationExtensionConfiguration.KEY).schemaSync();

        return new CatalogManagerImpl(
                new UpdateLogImpl(metaStorageMgr, failureManager),
                clockService,
                failureManager,
                () -> schemaSyncConfig.delayDurationMillis().value(),
                partitionCountProviderWrapper
        );
    }
}
