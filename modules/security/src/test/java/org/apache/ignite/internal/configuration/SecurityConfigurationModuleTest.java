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

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

class SecurityConfigurationModuleTest {

    private final SecurityConfigurationModule module = new SecurityConfigurationModule();

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
        MatcherAssert.assertThat(module.validators(),
                hasItems(
                        instanceOf(AuthenticationConfigurationValidatorImpl.class),
                        instanceOf(AuthenticationProvidersValidatorImpl.class))
        );
    }

    @Test
    void providesNoSchemaExtensions() {
        assertThat(module.allSchemaExtensions(), is(empty()));
    }

    @Test
    void providesPolymorphicSchemaExtensions() {
        assertThat(module.polymorphicSchemaExtensions(), contains(BasicAuthenticationProviderConfigurationSchema.class));
    }
}
