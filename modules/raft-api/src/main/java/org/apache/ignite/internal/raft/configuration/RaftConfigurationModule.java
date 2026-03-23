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

package org.apache.ignite.internal.raft.configuration;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;

/**
 * {@link ConfigurationModule} for node-local configuration provided by ignite-raft.
 */
@AutoService(ConfigurationModule.class)
public class RaftConfigurationModule implements ConfigurationModule {
    @Override
    public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }

    @Override
    public Collection<Class<?>> schemaExtensions() {
        return List.of(RaftExtensionConfigurationSchema.class);
    }

    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return List.of(
                UnlimitedBudgetConfigurationSchema.class,
                EntryCountBudgetConfigurationSchema.class
        );
    }

    @Override
    public Set<Validator<?, ?>> validators() {
        return Set.of(LogStorageConfigurationValidator.INSTANCE);
    }

    @Override
    public void migrateDeprecatedConfigurations(SuperRootChange superRootChange) {
        RaftExtensionChange raftExtensionChange = superRootChange.changeRoot(RaftExtensionConfiguration.KEY);
        RaftExtensionView raftExtensionView = superRootChange.viewRoot(RaftExtensionConfiguration.KEY);

        RaftChange raftChange = raftExtensionChange.changeRaft();
        RaftView raftView = raftExtensionView.raft();

        changeDisruptorStripesIfNeeded(raftView, raftChange);
        changeDisruptorLogManagerStripesIfNeeded(raftView, raftChange);
    }

    private static void changeDisruptorStripesIfNeeded(RaftView view, RaftChange change) {
        int stripes = view.stripes();

        if (stripes != DisruptorConfigurationSchema.DEFAULT_STRIPES_COUNT) {
            change.changeDisruptor().changeStripes(stripes);
        }
    }

    private static void changeDisruptorLogManagerStripesIfNeeded(RaftView view, RaftChange change) {
        int logStripesCount = view.logStripesCount();

        if (logStripesCount != DisruptorConfigurationSchema.DEFAULT_LOG_MANAGER_STRIPES_COUNT) {
            change.changeDisruptor().changeLogManagerStripes(logStripesCount);
        }
    }
}
