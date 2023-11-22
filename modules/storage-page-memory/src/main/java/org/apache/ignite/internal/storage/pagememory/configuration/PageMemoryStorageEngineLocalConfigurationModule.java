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
import java.util.Set;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryStorageProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryStorageProfileConfigurationSchema;
import org.apache.ignite.internal.storage.configurations.StoragesConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.VolatilePageMemoryStorageEngineConfiguration;

/**
 * {@link ConfigurationModule} for cluster-wide configuration provided by ignite-storage-page-memory.
 */
@AutoService(ConfigurationModule.class)
public class PageMemoryStorageEngineLocalConfigurationModule implements ConfigurationModule {
    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<RootKey<?, ?>> rootKeys() {
        return List.of(StoragesConfiguration.KEY);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return List.of(
                PersistentPageMemoryStorageEngineConfigurationSchema.class,
                PersistentPageMemoryStorageProfileConfigurationSchema.class,
                VolatilePageMemoryDataStorageConfigurationSchema.class,
                PersistentPageMemoryDataStorageConfigurationSchema.class);
    }

    /** {@inheritDoc} */
    @Override
    public Set<Validator<?, ?>> validators() {
        return Set.of(PageMemoryDataRegionValidatorImpl.INSTANCE);
    }
}
