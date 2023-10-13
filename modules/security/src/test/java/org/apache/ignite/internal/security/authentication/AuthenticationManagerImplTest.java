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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.authentication.event.AuthenticationDisabled;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEnabled;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEvent;
import org.apache.ignite.internal.security.authentication.event.AuthenticationListener;
import org.apache.ignite.internal.security.authentication.event.AuthenticationProviderRemoved;
import org.apache.ignite.internal.security.authentication.event.AuthenticationProviderUpdated;
import org.apache.ignite.internal.security.configuration.SecurityChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class AuthenticationManagerImplTest extends BaseIgniteAbstractTest {
    private static final String PROVIDER = "basic";

    private static final String USERNAME = "admin";

    private static final String PASSWORD = "password";

    private static final UsernamePasswordRequest USERNAME_PASSWORD_REQUEST = new UsernamePasswordRequest(USERNAME, PASSWORD);

    private final AuthenticationManagerImpl manager = new AuthenticationManagerImpl();

    private final List<AuthenticationEvent> events = new ArrayList<>();

    private final AuthenticationListener listener = events::add;

    @InjectConfiguration
    private SecurityConfiguration securityConfiguration;

    @BeforeEach
    void setUp() {
        manager.listen(listener);
    }

    @Test
    public void enableAuth() {
        // when
        enableAuthentication();

        // then
        // successful authentication with valid credentials

        assertEquals(USERNAME, manager.authenticate(USERNAME_PASSWORD_REQUEST).username());

        // and failed authentication with invalid credentials
        assertThrows(InvalidCredentialsException.class,
                () -> manager.authenticate(new UsernamePasswordRequest(USERNAME, "invalid-password")));

        assertEquals(1, events.size());
        assertInstanceOf(AuthenticationEnabled.class, events.get(0));
    }

    @Test
    public void leaveOldSettingWhenInvalidConfiguration() {
        // when
        SecurityView oldValue = securityConfiguration.value();

        SecurityView invalidAuthView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeEnabled(true);
                })
                .value();
        manager.onUpdate(new StubSecurityViewEvent(oldValue, invalidAuthView)).join();

        // then
        // authentication is still disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());

        assertEquals(0, events.size());
    }

    @Test
    public void disableAuthEmptyProviders() {
        //when
        enableAuthentication();

        // then

        // disable authentication
        SecurityView currentView = securityConfiguration.value();

        SecurityView disabledView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.delete(PROVIDER));
                    change.changeEnabled(false);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(currentView, disabledView)).join();

        // then
        // authentication is disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());

        assertEquals(3, events.size());
        assertInstanceOf(AuthenticationEnabled.class, events.get(0));
        assertInstanceOf(AuthenticationDisabled.class, events.get(1));
        AuthenticationProviderRemoved removed = assertInstanceOf(AuthenticationProviderRemoved.class, events.get(2));
        assertEquals(PROVIDER, removed.name());
    }

    @Test
    public void disableAuthNotEmptyProviders() {
        //when
        enableAuthentication();

        // disable authentication
        SecurityView currentView = securityConfiguration.value();

        SecurityView disabledView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeEnabled(false);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(currentView, disabledView)).join();

        // then
        // authentication is disabled
        UsernamePasswordRequest emptyCredentials = new UsernamePasswordRequest("", "");

        assertEquals("Unknown", manager.authenticate(emptyCredentials).username());

        assertEquals(2, events.size());
        assertInstanceOf(AuthenticationEnabled.class, events.get(0));
        assertInstanceOf(AuthenticationDisabled.class, events.get(1));
    }

    @Test
    public void changedCredentials() {
        // when
        enableAuthentication();

        // then
        // change authentication settings - change password
        SecurityView currentView = securityConfiguration.value();

        SecurityView adminNewPasswordView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.update(PROVIDER, provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername(USERNAME)
                                .changePassword("new-password");
                    }));
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(currentView, adminNewPasswordView)).join();

        assertThrows(InvalidCredentialsException.class, () -> manager.authenticate(USERNAME_PASSWORD_REQUEST));

        // then
        // successful authentication with the new password
        UsernamePasswordRequest adminNewPasswordCredentials = new UsernamePasswordRequest(USERNAME, "new-password");

        assertEquals(USERNAME, manager.authenticate(adminNewPasswordCredentials).username());

        assertEquals(2, events.size());
        assertInstanceOf(AuthenticationEnabled.class, events.get(0));
        AuthenticationProviderUpdated updated = assertInstanceOf(AuthenticationProviderUpdated.class, events.get(1));
        assertEquals(PROVIDER, updated.name());
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
        doReturn(new UserDetails("admin", "mock")).when(authenticator4).authenticate(credentials);

        manager.authEnabled(true);
        manager.authenticators(List.of(authenticator1, authenticator2, authenticator3, authenticator4));

        assertEquals("admin", manager.authenticate(credentials).username());

        verify(authenticator1).authenticate(credentials);
        verify(authenticator2).authenticate(credentials);
        verify(authenticator3).authenticate(credentials);
        verify(authenticator4).authenticate(credentials);
    }

    private void enableAuthentication() {
        SecurityView oldValue = securityConfiguration.value();

        SecurityView adminPasswordView = mutateConfiguration(
                securityConfiguration, change -> {
                    change.changeAuthentication().changeProviders(providers -> providers.create(PROVIDER, provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername(USERNAME)
                                .changePassword(PASSWORD);
                    }));
                    change.changeEnabled(true);
                })
                .value();

        manager.onUpdate(new StubSecurityViewEvent(oldValue, adminPasswordView)).join();
    }

    private static SecurityConfiguration mutateConfiguration(SecurityConfiguration configuration,
            Consumer<SecurityChange> consumer) {
        CompletableFuture<SecurityConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);
        assertThat(future, willCompleteSuccessfully());
        return future.join();
    }
}
