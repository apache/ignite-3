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

import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.SuperRootChangeImpl;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderConfigurationSchema;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.validator.AuthenticationProvidersValidatorImpl;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SecurityConfigurationModuleTest {
    private ConfigurationTreeGenerator generator;

    private SuperRootChange rootChange;

    private final SecurityConfigurationModule module = new SecurityConfigurationModule();

    @BeforeEach
    void setUp() {
        generator = new ConfigurationTreeGenerator(
                module.rootKeys(),
                module.schemaExtensions(),
                module.polymorphicSchemaExtensions()
        );

        SuperRoot superRoot = generator.createSuperRoot();

        rootChange = new SuperRootChangeImpl(superRoot);
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
        assertThat(module.rootKeys(), contains(SecurityConfiguration.KEY));
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
        module.patchConfigurationWithDynamicDefaults(rootChange);

        SecurityView securityView = rootChange.viewRoot(SecurityConfiguration.KEY);

        assertThat(securityView.authentication().providers().size(), is(1));

        BasicAuthenticationProviderView defaultProvider = (BasicAuthenticationProviderView) securityView.authentication()
                .providers()
                .get(0);

        assertAll(
                () -> assertThat(defaultProvider.users().get(0).username(), equalTo("ignite")),
                () -> assertThat(defaultProvider.users().get(0).password(), equalTo("ignite"))
        );
    }

    @Test
    void doNotSetDefaultUserIfProvidersIsNotEmpty() {
        rootChange.changeRoot(SecurityConfiguration.KEY).changeAuthentication(authenticationChange -> {
            authenticationChange.changeProviders().create("basic", change -> {
                change.convert(BasicAuthenticationProviderChange.class)
                        .changeUsers(users -> users.create("admin", user -> user.changePassword("password")));
            });
        });

        module.patchConfigurationWithDynamicDefaults(rootChange);

        SecurityView securityView = rootChange.viewRoot(SecurityConfiguration.KEY);

        assertThat(securityView.authentication().providers().size(), is(1));

        BasicAuthenticationProviderView defaultProvider = (BasicAuthenticationProviderView) securityView.authentication()
                .providers()
                .get(0);

        assertAll(
                () -> assertThat(defaultProvider.users().get(0).username(), equalTo("admin")),
                () -> assertThat(defaultProvider.users().get(0).password(), equalTo("password"))
        );
    }
}
