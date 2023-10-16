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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.configuration.SecurityChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith(ConfigurationExtension.class)
class AuthenticationManagerImplTest extends BaseIgniteAbstractTest {
    private final AuthenticationManagerImpl manager = new AuthenticationManagerImpl();

    @InjectConfiguration
    private SecurityConfiguration securityConfiguration;

    @Test
    public void enableAuth() {
        // when
        SecurityView adminPasswordView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("password");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(null, adminPasswordView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordRequest validCredentials = new UsernamePasswordRequest("admin", "password");

        assertEquals("admin", manager.authenticate(validCredentials).username());

        // and failed authentication with invalid credentials
        assertThrows(InvalidCredentialsException.class,
                () -> manager.authenticate(new UsernamePasswordRequest("admin", "invalid-password")));
    }

    @Test
    public void leaveOldSettingWhenInvalidConfiguration() {
        // when
        SecurityView invalidAuthView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeEnabled(true);
                })
                .value();
        manager.onUpdate(new StubSecurityViewEvent(null, invalidAuthView)).join();

        // then
        // authentication is still disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());
    }

    @Test
    public void disableAuthEmptyProviders() {
        //when
        SecurityView adminPasswordView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("password");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(null, adminPasswordView)).join();

        // then

        // just to be sure that authentication is enabled
        // successful authentication with valid credentials
        UsernamePasswordRequest validCredentials = new UsernamePasswordRequest("admin", "password");

        assertEquals("admin", manager.authenticate(validCredentials).username());

        // disable authentication
        SecurityView disabledView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.delete("basic"));
                    change.changeEnabled(false);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(adminPasswordView, disabledView)).join();

        // then
        // authentication is disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());
    }

    @Test
    public void disableAuthNotEmptyProviders() {
        //when
        SecurityView adminPasswordView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("password");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(null, adminPasswordView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordRequest validCredentials = new UsernamePasswordRequest("admin", "password");

        assertEquals("admin", manager.authenticate(validCredentials).username());

        // disable authentication
        SecurityView disabledView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeEnabled(false);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(adminPasswordView, disabledView)).join();

        // then
        // authentication is disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());
    }

    @Test
    public void changedCredentials() {
        // when
        SecurityView adminPasswordView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("password");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(null, adminPasswordView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordRequest adminPasswordCredentials = new UsernamePasswordRequest("admin", "password");

        assertEquals("admin", manager.authenticate(adminPasswordCredentials).username());

        // change authentication settings - change password
        SecurityView adminNewPasswordView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.update("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("admin")
                                .changePassword("new-password");
                    }));
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(adminPasswordView, adminNewPasswordView)).join();

        assertThrows(InvalidCredentialsException.class, () -> manager.authenticate(adminPasswordCredentials));

        // then
        // successful authentication with the new password
        UsernamePasswordRequest adminNewPasswordCredentials = new UsernamePasswordRequest("admin", "new-password");

        assertEquals("admin", manager.authenticate(adminNewPasswordCredentials).username());
    }

    @Test
    public void exceptionsDuringAuthentication() {
        UsernamePasswordRequest credentials = new UsernamePasswordRequest("admin", "password");

        Authenticator authenticator1 = mock(Authenticator.class);
        doThrow(new InvalidCredentialsException("Invalid credentials")).when(authenticator1).authenticate(credentials);

        Authenticator authenticator2 = mock(Authenticator.class);
        doThrow(new UnsupportedAuthenticationTypeException("Unsupported type")).when(authenticator2).authenticate(credentials);

        Authenticator authenticator3 = mock(Authenticator.class);
        doThrow(new RuntimeException("Test exception")).when(authenticator3).authenticate(credentials);

        Authenticator authenticator4 = mock(Authenticator.class);
        doReturn(new UserDetails("admin")).when(authenticator4).authenticate(credentials);

        manager.authEnabled(true);
        manager.authenticators(List.of(authenticator1, authenticator2, authenticator3, authenticator4));

        assertEquals("admin", manager.authenticate(credentials).username());

        verify(authenticator1).authenticate(credentials);
        verify(authenticator2).authenticate(credentials);
        verify(authenticator3).authenticate(credentials);
        verify(authenticator4).authenticate(credentials);
    }

    private static SecurityConfiguration mutateConfiguration(SecurityConfiguration configuration,
            Consumer<SecurityChange> consumer) {
        CompletableFuture<SecurityConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);
        assertThat(future, willCompleteSuccessfully());
        return future.join();
    }
}
