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

package org.apache.ignite.migrationtools.config.converters;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.config.registry.ConfigurationRegistryInterface;
import org.apache.ignite3.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite3.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite3.internal.storage.configurations.StorageProfileChange;
import org.apache.ignite3.internal.storage.pagememory.configuration.schema.PersistentPageMemoryProfileChange;
import org.apache.ignite3.internal.storage.pagememory.configuration.schema.VolatilePageMemoryProfileChange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** DataRegionConfigurationConverter. */
public class DataRegionConfigurationConverter implements ConfigurationConverter {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataRegionConfigurationConverter.class);

    @SuppressWarnings("checkstyle:linelength")
    private static void convertDataRegion(
            ConfigurationRegistryInterface registry,
            DataRegionConfiguration regionCfg, boolean isDefault) throws ExecutionException, InterruptedException {
        String regionName = (!isDefault) ? regionCfg.getName() : DataStorageConfiguration.DFLT_DATA_REG_DEFAULT_NAME;
        if (!regionCfg.isPersistenceEnabled()) {
            convertMemoryRegion(registry, regionCfg, regionName);
        } else {
            convertPersistentRegion(registry, regionCfg, regionName);
        }
    }

    private static void convertPersistentRegion(ConfigurationRegistryInterface registry,
            DataRegionConfiguration regionCfg,
            String regionName) throws ExecutionException, InterruptedException {
        StorageConfiguration storageConfig = registry.getConfiguration(StorageExtensionConfiguration.KEY).storage();

        Consumer<StorageProfileChange> changer = t -> t.convert(PersistentPageMemoryProfileChange.class)
                .changeSizeBytes(regionCfg.getMaxSize())
                .changeReplacementMode(regionCfg.getPageReplacementMode().name());

        storageConfig.profiles().change(c -> c.createOrUpdate(regionName, changer)).get();
        // TODO: Configure the allocator
    }

    private static void convertMemoryRegion(ConfigurationRegistryInterface registry,
            DataRegionConfiguration regionCfg,
            String regionName) throws ExecutionException, InterruptedException {
        StorageConfiguration storageConfig = registry.getConfiguration(StorageExtensionConfiguration.KEY).storage();

        // TODO: Check where the eviction configuration went!!
        // TODO: Check where the changeEmptyPagesPoolSize configuration went!!
        Consumer<StorageProfileChange> changer = t -> t.convert(VolatilePageMemoryProfileChange.class)
                .changeInitSizeBytes(regionCfg.getInitialSize())
                .changeMaxSizeBytes(regionCfg.getMaxSize());

        storageConfig.profiles().change(c -> c.createOrUpdate(regionName, changer)).get();
        // TODO: Configure the allocator
    }

    // TODO: Clean-up the code.
    @Override
    public void convert(IgniteConfiguration src,
            ConfigurationRegistryInterface registry) throws ExecutionException, InterruptedException {
        DataStorageConfiguration dataStorageCfg = src.getDataStorageConfiguration();
        if (dataStorageCfg == null) {
            LOGGER.warn("Could not find data storage configuration");
            return;
        }

        // TODO: I'm not sure how to change the pageSize afterwards.
        if (dataStorageCfg.getPageSize() != 0) {
            LOGGER.warn("Unable to force pageSize to :{}. Unsupported in Ignite 3", dataStorageCfg.getPageSize());
        }

        DataRegionConfiguration defaultRegionCfg = dataStorageCfg.getDefaultDataRegionConfiguration();
        convertDataRegion(registry, defaultRegionCfg, true);

        DataRegionConfiguration[] otherRegionCfgs = dataStorageCfg.getDataRegionConfigurations();
        if (otherRegionCfgs != null) {
            for (DataRegionConfiguration regionCfg : otherRegionCfgs) {
                convertDataRegion(registry, regionCfg, false);
            }
        }
    }
}
