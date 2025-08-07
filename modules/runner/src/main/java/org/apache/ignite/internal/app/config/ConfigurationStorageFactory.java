/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.app.config;

import org.apache.ignite.NodeConfig;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.LocalConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.VaultConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.vault.VaultManager;

public class ConfigurationStorageFactory implements NodeConfigVisitor<LocalConfigurationStorage> {
    private final String nodeName;

    private final ConfigurationTreeGenerator localConfigurationGenerator;

    private final VaultManager vaultManager;

    private final ConfigurationModules modules;

    public ConfigurationStorageFactory(
            String nodeName,
            ConfigurationTreeGenerator localConfigurationGenerator,
            VaultManager vaultManager,
            ConfigurationModules modules
    ) {
        this.nodeName = nodeName;
        this.localConfigurationGenerator = localConfigurationGenerator;
        this.vaultManager = vaultManager;
        this.modules = modules;
    }

    @Override
    public LocalConfigurationStorage visitFileConfig(FileNodeConfig fileNodeConfig) {
        return new LocalFileConfigurationStorage(
                nodeName,
                fileNodeConfig.configPath(),
                localConfigurationGenerator,
                modules.local()
        );
    }

    @Override
    public LocalConfigurationStorage visitBootstrapConfig(BootstrapNodeConfig bootstrapNodeConfig) {
        return new VaultConfigurationStorage(
                nodeName,
                vaultManager
        );
    }
}
