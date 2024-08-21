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

package org.apache.ignite.internal.storage.pagememory.configuration;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileChange;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.storage.configurations.StorageExtensionChange;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineExtensionConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineExtensionConfigurationSchema;

/**
 * {@link ConfigurationModule} for local node configuration provided by ignite-storage-page-memory.
 */
@AutoService(ConfigurationModule.class)
public class PageMemoryStorageEngineLocalConfigurationModule implements ConfigurationModule {
    public static final String DEFAULT_PROFILE_NAME = "default";

    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return List.of(
                VolatilePageMemoryProfileConfigurationSchema.class,
                PersistentPageMemoryProfileConfigurationSchema.class);
    }

    @Override
    public Collection<Class<?>> schemaExtensions() {
        return List.of(
                PersistentPageMemoryStorageEngineExtensionConfigurationSchema.class,
                VolatilePageMemoryStorageEngineExtensionConfigurationSchema.class);
    }

    @Override
    public void patchConfigurationWithDynamicDefaults(SuperRootChange rootChange) {
        StorageExtensionChange storageExtensionChange = (StorageExtensionChange) rootChange.changeRoot(StorageExtensionConfiguration.KEY);
        storageExtensionChange.changeStorage().changeProfiles().createOrUpdate(DEFAULT_PROFILE_NAME, p -> {
            p.convert(PersistentPageMemoryProfileChange.class);
        });
    }
}
