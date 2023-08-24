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
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith(ConfigurationExtension.class)
class AuthenticationProvidersValidatorTest extends BaseIgniteAbstractTest {

    @InjectConfiguration
    private AuthenticationConfiguration authenticationConfiguration;

    @Test
    public void basicProvider() {
        // when
        NamedListView<AuthenticationProviderView> newValue = (NamedListView<AuthenticationProviderView>) mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("admin");
                    }));
                    change.changeEnabled(false);
                })
                .value()
                .providers();

        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                null,
                newValue
        );

        // then
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx, null);
    }

    @Test
    public void basicProviderWithNullLogin() {
        // when
        NamedListView<AuthenticationProviderView> newValue = (NamedListView<AuthenticationProviderView>) mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changePassword("admin");
                    }));
                    change.changeEnabled(false);
                })
                .value()
                .providers();

        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                null,
                newValue
        );

        // then
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx,
                "Username must not be blank");
    }

    @Test
    public void basicProviderWithEmptyLogin() {
        // when
        NamedListView<AuthenticationProviderView> newValue = (NamedListView<AuthenticationProviderView>) mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("")
                                .changePassword("admin");
                    }));
                    change.changeEnabled(false);
                })
                .value()
                .providers();

        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                null,
                newValue
        );

        // then
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx,
                "Username must not be blank");
    }

    @Test
    public void basicProviderWithNullPassword() {
        // when
        NamedListView<AuthenticationProviderView> newValue = (NamedListView<AuthenticationProviderView>) mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin");
                    }));
                    change.changeEnabled(false);
                })
                .value()
                .providers();

        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                null,
                newValue
        );

        // then
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx,
                "Password must not be blank");
    }

    @Test
    public void basicProviderWithEmptyPassword() {
        // when
        NamedListView<AuthenticationProviderView> newValue = (NamedListView<AuthenticationProviderView>) mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("");
                    }));
                    change.changeEnabled(false);
                })
                .value()
                .providers();

        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                null,
                newValue
        );

        // then
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx,
                "Password must not be blank");
    }

    private static AuthenticationConfiguration mutateConfiguration(AuthenticationConfiguration configuration,
            Consumer<AuthenticationChange> consumer) {
        CompletableFuture<AuthenticationConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);
        assertThat(future, willCompleteSuccessfully());
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
