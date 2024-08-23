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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.function.Consumer;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.ClusterConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestValidationUtil;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderConfigurationSchema;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.configuration.validator.AuthenticationProvidersValidator;
import org.apache.ignite.internal.security.authentication.validator.AuthenticationProvidersValidatorImpl;
import org.apache.ignite.internal.security.configuration.SecurityChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityExtensionView;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class AuthenticationProvidersValidatorImplTest extends BaseIgniteAbstractTest {
    @InjectConfiguration(polymorphicExtensions = CustomAuthenticationProviderConfigurationSchema.class)
    private SecurityConfiguration securityConfiguration;

    @Test
    public void enableAuthEmptyProviders() {
        // When configuration is empty
        SecurityView securityView = securityConfiguration.value();

        // Then validation fails
        validate(securityView, "At least one provider is required.");
    }

    @Test
    public void multipleBasicProviders() {
        // When there are multiple basic providers
        SecurityView newValue = mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication().changeProviders(providers -> {
                providers.create(
                                "basic1",
                                providerChange -> providerChange.convert(AuthenticationProviderConfigurationSchema.TYPE_BASIC)
                        )
                        .create(
                                "basic2",
                                providerChange -> providerChange.convert(AuthenticationProviderConfigurationSchema.TYPE_BASIC)
                        );
            });
        });

        // Then validation fails
        validate(newValue, "Only one basic provider is supported.");
    }

    @Test
    public void basicProviderWithoutUsers() {
        // When there's only one provider, its type is basic and there are no users
        SecurityView newValue = mutateConfiguration(securityConfiguration, change -> {
            change.changeEnabled(true);
            change.changeAuthentication().changeProviders().create("basic",
                    providerChange -> providerChange.convert(AuthenticationProviderConfigurationSchema.TYPE_BASIC)
            );
        });

        // Then validation fails
        validate(newValue, "Basic provider must have at least one user.");
    }

    @Test
    public void customProviderWithoutBasicProvider() {
        // When there's only one non-basic provider
        SecurityView newValue = mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication().changeProviders().create("custom",
                    providerChange -> providerChange.convert(CustomAuthenticationProviderConfigurationSchema.TYPE)
            );
        });

        // Then validation succeeds
        validate(newValue, "Basic provider is required.");
    }

    @Test
    public void basicProviderWithoutUsersAndCustomProvider() {
        // When there's a basic provider without users and a non-basic provider
        SecurityView newValue = mutateConfiguration(securityConfiguration, change -> {
            change.changeEnabled(true);
            change.changeAuthentication().changeProviders(providers -> {
                providers.create(
                                "basic",
                                providerChange -> providerChange.convert(AuthenticationProviderConfigurationSchema.TYPE_BASIC)
                        )
                        .create(
                                "custom",
                                providerChange -> providerChange.convert(CustomAuthenticationProviderConfigurationSchema.TYPE)
                        );
            });
        });

        // Then validation succeeds
        validate(newValue, "Basic provider must have at least one user.");
    }

    @Test
    public void basicProviderWithUsers() {
        // When there's a basic provider with users
        SecurityView newValue = mutateConfiguration(securityConfiguration, change -> {
            change.changeEnabled(true);
            change.changeAuthentication().changeProviders(providers -> {
                providers.create(
                        "basic",
                        providerChange -> providerChange.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers().create("user", userChange -> {})
                );
            });
        });

        // Then validation succeeds
        validate(newValue);
    }

    @Test
    public void basicProviderWithUsersAndCustomProvider() {
        // When there's a basic provider with users
        SecurityView newValue = mutateConfiguration(securityConfiguration, change -> {
            change.changeEnabled(true);
            change.changeAuthentication().changeProviders(providers -> {
                providers.create(
                        "basic",
                        providerChange -> providerChange.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers().create("user", userChange -> {})
                ).create(
                        "custom",
                        providerChange -> providerChange.convert(CustomAuthenticationProviderConfigurationSchema.TYPE)
                );
            });
        });

        // Then validation succeeds
        validate(newValue);
    }

    private static void validate(SecurityView securityView) {
        validate(securityView, (String[]) null);
    }

    private static void validate(SecurityView securityView, String @Nullable ... errorMessagePrefixes) {
        ValidationContext<NamedListView<? extends AuthenticationProviderView>> ctx = mockValidationContext(
                null,
                securityView.authentication().providers()
        );

        SecurityExtensionView mock = mock(SecurityExtensionView.class);
        doReturn(securityView).when(mock).security();
        doReturn(mock).when(ctx).getNewRoot(ClusterConfiguration.KEY);

        TestValidationUtil.validate(
                AuthenticationProvidersValidatorImpl.INSTANCE,
                mock(AuthenticationProvidersValidator.class),
                ctx,
                errorMessagePrefixes
        );
    }

    private static SecurityView mutateConfiguration(SecurityConfiguration configuration, Consumer<SecurityChange> consumer) {
        return configuration.change(consumer).thenApply(unused -> configuration).join().value();
    }
}
