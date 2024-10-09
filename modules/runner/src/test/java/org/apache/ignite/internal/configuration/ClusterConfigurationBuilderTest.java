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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import org.apache.ignite.configuration.ClusterConfiguration;
import org.apache.ignite.configuration.ClusterConfigurationImpl;
import org.apache.ignite.eventlog.config.schema.EventLogBuilder;
import org.apache.ignite.eventlog.config.schema.LogSinkBuilder;
import org.apache.ignite.security.authentication.basic.BasicAuthenticationProviderBuilder;
import org.apache.ignite.security.authentication.basic.BasicUserBuilder;
import org.apache.ignite.security.authentication.configuration.AuthenticationBuilder;
import org.apache.ignite.security.configuration.SecurityBuilder;
import org.junit.jupiter.api.Test;

class ClusterConfigurationBuilderTest {
    @Test
    void sinks() {
        // Create polymorphic config instance
        LogSinkBuilder logSinkBuilder = LogSinkBuilder.create()
                .channel("channel")
                .level("DEBUG");
        // Create root config value
        EventLogBuilder eventLogBuilder = EventLogBuilder.create();
        // Add named list value
        eventLogBuilder.addSink("logSinkBuilder", logSinkBuilder);
        // Create configuration builder instance
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.create();
        // Set the root config value
        clusterConfiguration.eventlog(eventLogBuilder);

        String config = ((ClusterConfigurationImpl) clusterConfiguration).build(null);

        assertThat(config, containsString("eventlog {\n"
                + "    sinks=[\n"
                + "        {\n"
                + "            channel=channel\n"
                + "            criteria=EventLog\n"
                + "            format=JSON\n"
                + "            level=DEBUG\n"
                + "            name=logSinkBuilder\n"
                + "            type=log\n"
                + "        }\n"
                + "    ]\n"
                + "}\n"
        ));
    }

    @Test
    void basicAuthentication() {
        ClusterConfiguration clusterConfiguration = ClusterConfiguration.create().security(
                SecurityBuilder.create()
                        .enabled(true)
                        .authentication(AuthenticationBuilder.create()
                                .addProvider("basic", BasicAuthenticationProviderBuilder.create()
                                        .addUser("username2", BasicUserBuilder.create().password("password"))
                                        .addUser("username1", BasicUserBuilder.create().password("password"))
                                )
                        )
        );

        String config = ((ClusterConfigurationImpl) clusterConfiguration).build(null);

        assertThat(config, containsString(
                "security {\n"
                        + "    authentication {\n"
                        + "        providers=[\n"
                        + "            {\n"
                        + "                name=basic\n"
                        + "                type=basic\n"
                        + "                users=[\n"
                        + "                    {\n"
                        + "                        password=password\n"
                        + "                        username=username2\n"
                        + "                    },\n"
                        + "                    {\n"
                        + "                        password=password\n"
                        + "                        username=username1\n"
                        + "                    }\n"
                        + "                ]\n"
                        + "            }\n"
                        + "        ]\n"
                        + "    }\n"
                        + "    enabled=true\n"
                        + "}\n"
        ));
    }
}
