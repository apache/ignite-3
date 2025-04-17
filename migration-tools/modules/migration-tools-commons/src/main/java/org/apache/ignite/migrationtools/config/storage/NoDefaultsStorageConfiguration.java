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

package org.apache.ignite.migrationtools.config.storage;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite3.configuration.ConfigurationModule;
import org.apache.ignite3.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite3.internal.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite3.internal.configuration.storage.Data;
import org.apache.ignite3.internal.configuration.storage.LocalFileConfigurationStorage;

/**
 * Patched version of the LocalFileConfiguration Storage that does not store defaults.
 * This class should be considered experimental. The best solution would be to skip the usage of the
 * Configuration registry entirely because we do not use it's features.
 */
public class NoDefaultsStorageConfiguration extends LocalFileConfigurationStorage {
    // TODO: Check if it needs to be volatile. The listener must see the defaults after .write().
    private Map<String, ? extends Serializable> defaultValues = null;

    public NoDefaultsStorageConfiguration(Path configPath, ConfigurationTreeGenerator generator, ConfigurationModule module) {
        super(configPath, generator, module);
    }

    @Override
    public CompletableFuture<Data> readDataOnRecovery() {
        return CompletableFuture.completedFuture(new Data(Collections.emptyMap(), 0));
    }

    @Override public void registerConfigurationListener(ConfigurationStorageListener lsnr) {
        // Decorate the listener to let the user know we handled the defaults properly.
        super.registerConfigurationListener(data -> {
            // Check if this is the default configs
            if (data.changeId() == 1L && data.values().isEmpty()) {
                // Return the default values to the listener.
                return lsnr.onEntriesChanged(new Data(defaultValues, data.changeId()));
            } else {
                return lsnr.onEntriesChanged(data);
            }
        });
    }

    @Override public CompletableFuture<Boolean> write(Map<String, ? extends Serializable> newValues, long ver) {
        // Skipping default storage. It assumes that the defaults will be stored in version 0.
        if (ver == 0) {
            // The defaults are stored for later use.
            this.defaultValues = newValues;
            return super.write(Collections.emptyMap(), ver);
        } else {
            return super.write(newValues, ver);
        }
    }
}
