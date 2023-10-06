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
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.apache.ignite.internal.security.authentication.SecurityConfigurationModule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClusterConfigurationDefaultsSetterTest {
    private ConfigurationTreeGenerator generator;

    private ClusterConfigurationDefaultsSetter setter;

    @BeforeEach
    void setUp() {
        SecurityConfigurationModule module = new SecurityConfigurationModule();

        generator = new ConfigurationTreeGenerator(
                module.rootKeys(),
                module.schemaExtensions(),
                module.polymorphicSchemaExtensions()
        );

        setter = new ClusterConfigurationDefaultsSetter(module.rootKeys(), generator);

        setter.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        setter.stop();
        generator.close();
    }

    @Test
    void setDefaultUserIfProvidersIsEmpty() {
        String defaults = setter.setDefaults("").join();

        Config config = ConfigFactory.parseString(defaults);

        List<? extends Config> providers = config.getConfigList("security.authentication.providers");

        assertThat(providers.size(), equalTo(1));

        Config defaultProvider = providers.get(0);

        assertAll(
                () -> assertThat(defaultProvider.getString("type"), equalTo("basic")),
                () -> assertThat(defaultProvider.getString("name"), equalTo("default")),
                () -> assertThat(defaultProvider.getString("username"), equalTo("ignite")),
                () -> assertThat(defaultProvider.getString("password"), equalTo("ignite"))
        );
    }

    @Test
    void noSetDefaultUserIfProvidersIsNotEmpty() {
        String defaults = setter.setDefaults(
                        "{security.authentication.providers:[{name:basic,password:password,type:basic,username:admin}]}")
                .join();

        Config config = ConfigFactory.parseString(defaults);

        List<? extends Config> providers = config.getConfigList("security.authentication.providers");

        assertThat(providers.size(), equalTo(1));

        Config defaultProvider = providers.get(0);

        assertAll(
                () -> assertThat(defaultProvider.getString("type"), equalTo("basic")),
                () -> assertThat(defaultProvider.getString("name"), equalTo("basic")),
                () -> assertThat(defaultProvider.getString("username"), equalTo("admin")),
                () -> assertThat(defaultProvider.getString("password"), equalTo("password"))
        );
    }
}
