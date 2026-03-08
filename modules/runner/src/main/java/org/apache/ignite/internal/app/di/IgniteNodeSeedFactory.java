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

import static org.apache.ignite.internal.configuration.IgnitePaths.vaultPath;

import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.components.NodeIdentity;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.vault.VaultService;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;

/**
 * Micronaut factory that produces infrastructure beans derived from {@link NodeSeedParams}.
 */
@Factory
public class IgniteNodeSeedFactory {
    private final NodeSeedParams seedParams;

    public IgniteNodeSeedFactory(NodeSeedParams seedParams) {
        this.seedParams = seedParams;
    }

    /** Creates the node identity POJO for injection into components across modules. */
    @Singleton
    public NodeIdentity nodeIdentity() {
        return new NodeIdentity(seedParams.nodeName(), seedParams.workDir(), seedParams.clusterIdSupplier());
    }

    /** Creates the persistent vault service backed by the node's work directory. */
    @Singleton
    public VaultService vaultService() {
        return new PersistentVaultService(vaultPath(seedParams.workDir()));
    }

    /** Discovers configuration modules from the service provider class loader. */
    @Singleton
    public ConfigurationModules configurationModules() {
        return ConfigurationModules.create(seedParams.serviceProviderClassLoader());
    }
}
