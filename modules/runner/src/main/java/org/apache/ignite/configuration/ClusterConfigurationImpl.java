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

package org.apache.ignite.configuration;

import static org.apache.ignite.configuration.ConfigurationBuilderUtil.createChanger;
import static org.apache.ignite.configuration.ConfigurationBuilderUtil.loadConfigurationModules;
import static org.apache.ignite.configuration.ConfigurationBuilderUtil.renderConfig;
import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;

import org.apache.ignite.eventlog.config.schema.EventLogBuilder;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.eventlog.config.schema.EventLogBuilderImpl;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityBuilderImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.security.configuration.SecurityBuilder;

public class ClusterConfigurationImpl implements ClusterConfiguration {
    private SecurityBuilderImpl security;
    private EventLogBuilderImpl eventLogBuilder;

    @Override
    public ClusterConfiguration security(SecurityBuilder securityBuilder) {
        this.security = (SecurityBuilderImpl) securityBuilder;
        return this;
    }

    @Override
    public ClusterConfiguration eventlog(EventLogBuilder eventLogBuilder) {
        this.eventLogBuilder = (EventLogBuilderImpl) eventLogBuilder;
        return this;
    }

    public String build(ClassLoader classLoader) {
        ConfigurationModules modules = loadConfigurationModules(classLoader);
        ConfigurationTreeGenerator configurationGenerator = new ConfigurationTreeGenerator(
                modules.distributed().rootKeys(),
                modules.distributed().schemaExtensions(),
                modules.distributed().polymorphicSchemaExtensions()
        );

        ConfigurationChanger changer = createChanger(DISTRIBUTED, configurationGenerator, modules.distributed().rootKeys());
        changer.start();
        changer.onDefaultsPersisted().join();


        if (security != null) {
            SecurityConfiguration securityConfiguration =
                    (SecurityConfiguration) configurationGenerator.instantiateCfg(SecurityConfiguration.KEY, changer);

            securityConfiguration.change(security::change).join();
        }
        if (eventLogBuilder != null) {
            EventLogConfiguration eventLogConfiguration =
                    (EventLogConfiguration) configurationGenerator.instantiateCfg(EventLogConfiguration.KEY, changer);

            eventLogConfiguration.change(eventLogBuilder::change).join();
        }


        String rendered = renderConfig(changer);
        changer.stop();
        return rendered;
    }
}
