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

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.ClusterConfiguration;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderConfigurationSchema;
import org.apache.ignite.internal.security.authentication.validator.AuthenticationProvidersValidatorImpl;
import org.apache.ignite.internal.security.configuration.SecurityExtensionChange;
import org.apache.ignite.internal.security.configuration.SecurityExtensionConfigurationSchema;

/**
 * {@link ConfigurationModule} for cluster configuration provided by ignite-rest.
 */
@AutoService(ConfigurationModule.class)
public class SecurityConfigurationModule implements ConfigurationModule {
    static final String DEFAULT_PROVIDER_NAME = "default";

    static final String DEFAULT_USERNAME = "ignite";

    static final String DEFAULT_PASSWORD = "ignite";

    @Override
    public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    @Override
    public Set<Validator<?, ?>> validators() {
        return Set.of(AuthenticationProvidersValidatorImpl.INSTANCE);
    }

    @Override
    public Collection<Class<?>> schemaExtensions() {
        return List.of(SecurityExtensionConfigurationSchema.class);
    }

    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return Collections.singleton(BasicAuthenticationProviderConfigurationSchema.class);
    }

    @Override
    public void patchConfigurationWithDynamicDefaults(SuperRootChange rootChange) {
        SecurityExtensionChange securityExtensionChange = (SecurityExtensionChange) rootChange.changeRoot(ClusterConfiguration.KEY);
        securityExtensionChange.changeSecurity().changeAuthentication().changeProviders(providersChange -> {
            if (providersChange.isEmpty()) {
                providersChange.create(DEFAULT_PROVIDER_NAME, change ->
                        change.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers().create(DEFAULT_USERNAME, user ->
                                        user.changePassword(DEFAULT_PASSWORD)
                                )
                );
            }
        });
    }
}
