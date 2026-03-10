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

package org.apache.ignite.internal.app.di;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.SystemDistributedExtensionConfiguration;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.wrapper.JumpToExecutorByConsistentIdAfterSend;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfiguration;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.configuration.TransactionExtensionConfiguration;

/**
 * Micronaut factory for transaction-related components.
 */
@Factory
public class TransactionFactory {
    /** Creates the transaction configuration from the cluster config registry. */
    @Singleton
    public TransactionConfiguration transactionConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry.getConfiguration(TransactionExtensionConfiguration.KEY).transaction();
    }

    /** Creates the system distributed configuration from the cluster config registry. */
    @Singleton
    public SystemDistributedConfiguration systemDistributedConfiguration(
            @Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry
    ) {
        return clusterConfigRegistry.getConfiguration(SystemDistributedExtensionConfiguration.KEY).system();
    }

    /** Creates the GC configuration from the cluster config registry. */
    @Singleton
    public GcConfiguration gcConfiguration(@Named("clusterConfig") ConfigurationRegistry clusterConfigRegistry) {
        return clusterConfigRegistry.getConfiguration(GcExtensionConfiguration.KEY).gc();
    }

    /** Creates the low watermark configuration from the GC config. */
    @Singleton
    public LowWatermarkConfiguration lowWatermarkConfiguration(GcConfiguration gcConfiguration) {
        return gcConfiguration.lowWatermark();
    }

    /** Creates the messaging service wrapper that jumps to the storage operations thread pool. */
    @Singleton
    @Named("storageOperations")
    public MessagingService storageOperationsMessagingService(
            @Named("clusterMessaging") MessagingService clusterMessagingService,
            NodeIdentity nodeIdentity,
            @Named("partitionOperationsExecutor") ExecutorService partitionOperationsExecutor
    ) {
        return new JumpToExecutorByConsistentIdAfterSend(
                clusterMessagingService,
                nodeIdentity.nodeName(),
                message -> partitionOperationsExecutor
        );
    }

}
