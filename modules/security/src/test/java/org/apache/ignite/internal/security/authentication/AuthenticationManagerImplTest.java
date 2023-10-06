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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationChange;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationView;
import org.apache.ignite.internal.security.authentication.configuration.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.security.authentication.exception.InvalidCredentialsException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith(ConfigurationExtension.class)
class AuthenticationManagerImplTest extends BaseIgniteAbstractTest {
    private final AuthenticationManagerImpl manager = new AuthenticationManagerImpl();

    @InjectConfiguration
    private AuthenticationConfiguration authenticationConfiguration;

    @Test
    public void enableAuth() throws InvalidCredentialsException {
        // when
        AuthenticationView adminPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("password");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordRequest validCredentials = new UsernamePasswordRequest("admin", "password");

        assertEquals("admin", manager.authenticate(validCredentials).username());
    }

    @Test
    public void leaveOldSettingWhenInvalidConfiguration() throws InvalidCredentialsException {
        // when
        AuthenticationView invalidAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeEnabled(true);
                })
                .value();
        manager.onUpdate(new StubAuthenticationViewEvent(null, invalidAuthView)).join();

        // then
        // authentication is still disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());
    }

    @Test
    public void disableAuthEmptyProviders() throws InvalidCredentialsException {
        //when
        AuthenticationView adminPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("password");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then

        // just to be sure that authentication is enabled
        // successful authentication with valid credentials
        UsernamePasswordRequest validCredentials = new UsernamePasswordRequest("admin", "password");

        assertEquals("admin", manager.authenticate(validCredentials).username());

        // disable authentication
        AuthenticationView disabledAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.delete("basic"));
                    change.changeEnabled(false);
                })
                .value();

        manager.onUpdate(new StubAuthenticationViewEvent(adminPasswordAuthView, disabledAuthView)).join();

        // then
        // authentication is disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());
    }

    @Test
    public void disableAuthNotEmptyProviders() throws InvalidCredentialsException {
        //when
        AuthenticationView adminPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("password");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordRequest validCredentials = new UsernamePasswordRequest("admin", "password");

        assertEquals("admin", manager.authenticate(validCredentials).username());

        // disable authentication
        AuthenticationView disabledAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeEnabled(false);
                })
                .value();

        manager.onUpdate(new StubAuthenticationViewEvent(adminPasswordAuthView, disabledAuthView)).join();

        // then
        // authentication is disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());
    }

    @Test
    public void changedCredentials() throws InvalidCredentialsException {
        // when
        AuthenticationView adminPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("password");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordRequest adminPasswordCredentials = new UsernamePasswordRequest("admin", "password");

        assertEquals("admin", manager.authenticate(adminPasswordCredentials).username());

        // change authentication settings - change password
        AuthenticationView adminNewPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.update("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("new-password");
                    }));
                })
                .value();

        manager.onUpdate(new StubAuthenticationViewEvent(adminPasswordAuthView, adminNewPasswordAuthView)).join();

        assertThrows(InvalidCredentialsException.class, () -> manager.authenticate(adminPasswordCredentials));

        // then
        // successful authentication with the new password
        UsernamePasswordRequest adminNewPasswordCredentials = new UsernamePasswordRequest("admin", "new-password");

        assertEquals("admin", manager.authenticate(adminNewPasswordCredentials).username());
    }

    private static AuthenticationConfiguration mutateConfiguration(AuthenticationConfiguration configuration,
            Consumer<AuthenticationChange> consumer) {
        CompletableFuture<AuthenticationConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);
        assertThat(future, willCompleteSuccessfully());
        return future.join();
    }
}
