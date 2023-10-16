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

package org.apache.ignite.internal.security.authentication;

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertAll;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.apache.ignite.internal.configuration.ClusterConfigurationDefaultsPatcherImpl;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderConfigurationSchema;
import org.apache.ignite.internal.security.authentication.configuration.validator.AuthenticationProvidersValidatorImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SecurityConfigurationModuleTest {
    private final SecurityConfigurationModule module = new SecurityConfigurationModule();

    private ConfigurationTreeGenerator generator;

    private ClusterConfigurationDefaultsPatcherImpl setter;

    @BeforeEach
    void setUp() {
        SecurityConfigurationModule module = new SecurityConfigurationModule();

        generator = new ConfigurationTreeGenerator(
                module.rootKeys(),
                module.schemaExtensions(),
                module.polymorphicSchemaExtensions()
        );

        setter = new ClusterConfigurationDefaultsPatcherImpl(module, generator);
    }

    @AfterEach
    void tearDown() {
        generator.close();
    }

    @Test
    void typeIsDistributed() {
        assertThat(module.type(), is(DISTRIBUTED));
    }

    @Test
    void hasConfigurationRoots() {
        assertThat(module.rootKeys(), Matchers.contains(SecurityConfiguration.KEY));
    }

    @Test
    void providesValidators() {
        assertThat(module.validators(),
                hasItems(
                        instanceOf(AuthenticationProvidersValidatorImpl.class))
        );
    }

    @Test
    void providesNoSchemaExtensions() {
        assertThat(module.schemaExtensions(), is(empty()));
    }

    @Test
    void providesPolymorphicSchemaExtensions() {
        assertThat(module.polymorphicSchemaExtensions(), contains(BasicAuthenticationProviderConfigurationSchema.class));
    }

    @Test
    void setDefaultUserIfProvidersIsEmpty() {
        String defaults = setter.patchDefaults("");

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
        String defaults = setter.patchDefaults(
                "{security.authentication.providers:[{name:basic,password:password,type:basic,username:admin}]}");

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
