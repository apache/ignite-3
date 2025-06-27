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

package org.apache.ignite.internal.configuration;

import static org.apache.ignite.internal.worker.configuration.CriticalWorkersConfigurationSchema.DEFAULT_LIVENESS_CHECK_INTERVAL_MILLIS;
import static org.apache.ignite.internal.worker.configuration.CriticalWorkersConfigurationSchema.DEFAULT_MAX_ALLOWED_LAG_MILLIS;
import static org.apache.ignite.internal.worker.configuration.CriticalWorkersConfigurationSchema.DEFAULT_NETTY_THREADS_HEARTBEAT_INTERVAL_MILLIS;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.worker.configuration.CriticalWorkersChange;
import org.apache.ignite.internal.worker.configuration.CriticalWorkersView;

/** {@link ConfigurationModule} for node-local system configuration. */
@AutoService(ConfigurationModule.class)
public class SystemLocalConfigurationModule implements ConfigurationModule {
    @Override
    public ConfigurationType type() {
        return ConfigurationType.LOCAL;
    }

    @Override
    public Collection<Class<?>> schemaExtensions() {
        return List.of(SystemLocalExtensionConfigurationSchema.class);
    }

    @Override
    public void migrateDeprecatedConfigurations(SuperRootChange superRootChange) {
        SystemLocalExtensionView rootView = superRootChange.viewRoot(SystemLocalExtensionConfiguration.KEY);
        SystemLocalExtensionChange rootChange = superRootChange.changeRoot(SystemLocalExtensionConfiguration.KEY);

        CriticalWorkersView criticalWorkersView = rootView.criticalWorkers();
        CriticalWorkersChange criticalWorkersChange = rootChange.changeSystem().changeCriticalWorkers();

        if (criticalWorkersView.livenessCheckIntervalMillis() != DEFAULT_LIVENESS_CHECK_INTERVAL_MILLIS) {
            criticalWorkersChange.changeLivenessCheckIntervalMillis(criticalWorkersView.livenessCheckIntervalMillis());
        }
        if (criticalWorkersView.maxAllowedLagMillis() != DEFAULT_MAX_ALLOWED_LAG_MILLIS) {
            criticalWorkersChange.changeMaxAllowedLagMillis(criticalWorkersView.maxAllowedLagMillis());
        }
        if (criticalWorkersView.nettyThreadsHeartbeatIntervalMillis() != DEFAULT_NETTY_THREADS_HEARTBEAT_INTERVAL_MILLIS) {
            criticalWorkersChange.changeNettyThreadsHeartbeatIntervalMillis(criticalWorkersView.nettyThreadsHeartbeatIntervalMillis());
        }
    }
}
