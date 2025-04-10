/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.migrationtools.config.converters;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.migrationtools.config.registry.ConfigurationRegistryInterface;
import org.apache.ignite3.internal.pagememory.configuration.schema.PersistentPageMemoryProfileChange;
import org.apache.ignite3.internal.pagememory.configuration.schema.VolatilePageMemoryProfileChange;
import org.apache.ignite3.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite3.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite3.internal.storage.configurations.StorageProfileChange;
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
