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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith(ConfigurationExtension.class)
class AuthenticationConfigurationValidatorTest extends BaseIgniteAbstractTest {

    @InjectConfiguration
    private AuthenticationConfiguration authenticationConfiguration;

    @Test
    public void nullAuth() {
        // when
        ValidationContext<AuthenticationView> ctx = mockValidationContext(null, null);

        // then
        validate(AuthenticationConfigurationValidatorImpl.INSTANCE, mock(AuthenticationConfigurationValidator.class), ctx,
                "Auth config must not be null");
    }

    @Test
    public void enableAuthEmptyProviders() {
        // when
        AuthenticationView newValue = mutateConfiguration(authenticationConfiguration, change -> change.changeEnabled(true)).value();
        ValidationContext<AuthenticationView> ctx = mockValidationContext(
                null,
                newValue
        );

        // then
        validate(AuthenticationConfigurationValidatorImpl.INSTANCE, mock(AuthenticationConfigurationValidator.class), ctx,
                "Providers must be present, if auth is enabled");
    }

    @Test
    public void enableAuthNotEmptyProviders() {
        // when
        AuthenticationView newValue = mutateConfiguration(authenticationConfiguration, change -> {
            change.changeProviders(providers -> providers.create("basic", provider -> {
                provider.convert(BasicAuthenticationProviderChange.class)
                        .changeUsername("admin")
                        .changePassword("admin");
            }));
            change.changeEnabled(true);
        }).value();

        ValidationContext<AuthenticationView> ctx = mockValidationContext(
                null,
                newValue
        );

        // then
        validate(AuthenticationConfigurationValidatorImpl.INSTANCE, mock(AuthenticationConfigurationValidator.class), ctx, null);
    }

    @Test
    public void disableAuthEmptyProviders() {
        // when
        AuthenticationView newValue = mutateConfiguration(authenticationConfiguration, change -> change.changeEnabled(false)).value();
        ValidationContext<AuthenticationView> ctx = mockValidationContext(
                null,
                newValue
        );

        // then
        validate(AuthenticationConfigurationValidatorImpl.INSTANCE, mock(AuthenticationConfigurationValidator.class), ctx, null);
    }

    @Test
    public void disableAuthNotEmptyProviders() {
        // when
        AuthenticationView newValue = mutateConfiguration(authenticationConfiguration, change -> {
            change.changeProviders(providers -> providers.create("basic", provider -> {
                provider.convert(BasicAuthenticationProviderChange.class)
                        .changeUsername("admin")
                        .changePassword("admin");
            }));
            change.changeEnabled(false);
        }).value();

        ValidationContext<AuthenticationView> ctx = mockValidationContext(
                null,
                newValue
        );
        validate(AuthenticationConfigurationValidatorImpl.INSTANCE, mock(AuthenticationConfigurationValidator.class), ctx, null);
    }

    private static AuthenticationConfiguration mutateConfiguration(AuthenticationConfiguration configuration,
            Consumer<AuthenticationChange> consumer) {
        CompletableFuture<AuthenticationConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);
        assertThat(future, willCompleteSuccessfully());
        return future.join();
    }
}
