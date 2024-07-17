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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.SuperRootChangeImpl;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileView;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test for {@link PageMemoryStorageEngineLocalConfigurationModule}.
 */
public class PageMemoryStorageEngineLocalConfigurationModuleTest {
    private SuperRootChange rootChange;

    private  final PageMemoryStorageEngineLocalConfigurationModule module = new PageMemoryStorageEngineLocalConfigurationModule();

    @BeforeEach
    void setUp() {
        Collection<Class<?>> polymorphicSchemaExtensions = module.polymorphicSchemaExtensions();

        ConfigurationTreeGenerator generator = new ConfigurationTreeGenerator(
                List.of(StorageConfiguration.KEY),
                module.schemaExtensions(),
                polymorphicSchemaExtensions
        );

        SuperRoot superRoot = generator.createSuperRoot();

        rootChange = new SuperRootChangeImpl(superRoot);
    }

    @Test
    void setDefaultStorageProfile() {
        module.patchConfigurationWithDynamicDefaults(rootChange);

        StorageView storageConfigurationView = rootChange.viewRoot(StorageConfiguration.KEY);

        assertEquals(1, storageConfigurationView.profiles().size());

        assertThat(storageConfigurationView.profiles().get("default"), instanceOf(PersistentPageMemoryProfileView.class));
    }
}
