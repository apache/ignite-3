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

import static org.apache.ignite.internal.security.authentication.event.AuthenticationEvent.AUTHENTICATION_DISABLED;
import static org.apache.ignite.internal.security.authentication.event.AuthenticationEvent.AUTHENTICATION_ENABLED;
import static org.apache.ignite.internal.security.authentication.event.AuthenticationEvent.AUTHENTICATION_PROVIDER_REMOVED;
import static org.apache.ignite.internal.security.authentication.event.AuthenticationEvent.AUTHENTICATION_PROVIDER_UPDATED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEvent;
import org.apache.ignite.internal.security.authentication.event.AuthenticationEventParameters;
import org.apache.ignite.internal.security.authentication.event.AuthenticationProviderEventParameters;
import org.apache.ignite.internal.security.authentication.event.UserEventParameters;
import org.apache.ignite.internal.security.configuration.SecurityChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.security.exception.UnsupportedAuthenticationTypeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class AuthenticationManagerImplTest extends BaseIgniteAbstractTest {
    private static final String PROVIDER = SecurityConfigurationModule.DEFAULT_PROVIDER_NAME;

    private static final String USERNAME = SecurityConfigurationModule.DEFAULT_USERNAME;

    private static final String PASSWORD = SecurityConfigurationModule.DEFAULT_PASSWORD;

    private static final UsernamePasswordRequest USERNAME_PASSWORD_REQUEST = new UsernamePasswordRequest(USERNAME, PASSWORD);

    @InjectConfiguration(polymorphicExtensions = CustomAuthenticationProviderConfigurationSchema.class, rootName = "security")
    private SecurityConfiguration securityConfiguration;

    private AuthenticationManagerImpl manager;

    private final List<AuthenticationEventParameters> events = new CopyOnWriteArrayList<>();

    private final EventListener<AuthenticationEventParameters> listener = parameters -> {
        events.add(parameters);
        return falseCompletedFuture();
    };

    @BeforeEach
    void setUp() {
        manager = new AuthenticationManagerImpl(securityConfiguration, ign -> {});

        Arrays.stream(AuthenticationEvent.values()).forEach(event -> manager.listen(event, listener));

        assertThat(manager.startAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        Arrays.stream(AuthenticationEvent.values()).forEach(event -> manager.removeListener(event, listener));

        assertThat(manager.stopAsync(new ComponentContext()), willCompleteSuccessfully());
    }

    @Test
    void shouldFireEventWhenAuthenticationIsEnabled() {
        // When authentication is enabled.
        enableAuthentication();

        // Then event is fired.
        assertEquals(1, events.size());
        assertEquals(AUTHENTICATION_ENABLED, events.get(0).type());
    }

    @Test
    void shouldFireEventsWhenAuthenticationIsEnabledAndThenDisabled() {
        // When authentication is enabled.
        enableAuthentication();

        // And then disabled.
        disableAuthentication();

        // Then event is fired.
        assertEquals(2, events.size());
        assertEquals(AUTHENTICATION_ENABLED, events.get(0).type());
        assertEquals(AUTHENTICATION_DISABLED, events.get(1).type());
    }

    @Test
    void shouldFireUserUpdatedEventWhenUserPasswordIsChanged() {
        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update(PROVIDER, provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.update(USERNAME, user -> user.changePassword("new-password"))
                                );
                    });
        });

        assertEquals(1, events.size());
        UserEventParameters userEventParameters = (UserEventParameters) events.get(0);
        assertEquals(AuthenticationEvent.USER_UPDATED, userEventParameters.type());
        assertEquals(USERNAME, userEventParameters.username());
        assertEquals(PROVIDER, userEventParameters.providerName());
    }

    @Test
    void shouldFireUserRemovedEventWhenUserIsDeleted() {
        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update(PROVIDER, provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.delete(USERNAME)
                                );
                    });
        });

        assertEquals(1, events.size());
        UserEventParameters userEventParameters = (UserEventParameters) events.get(0);
        assertEquals(AuthenticationEvent.USER_REMOVED, userEventParameters.type());
        assertEquals(USERNAME, userEventParameters.username());
        assertEquals(PROVIDER, userEventParameters.providerName());
    }

    @Test
    void shouldFireUserRemovedEventWhenUserIsRenamed() {
        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update(PROVIDER, provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.rename(USERNAME, USERNAME + "-renamed")
                                );
                    });
        });

        assertEquals(1, events.size());
        UserEventParameters userEventParameters = (UserEventParameters) events.get(0);
        assertEquals(AuthenticationEvent.USER_REMOVED, userEventParameters.type());
        assertEquals(USERNAME, userEventParameters.username());
        assertEquals(PROVIDER, userEventParameters.providerName());
    }

    @Test
    void shouldFireEventsWhenProviderIsRenamedAndUserPasswordChanged() {
        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .rename(PROVIDER, PROVIDER + "-renamed");
        });

        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update(PROVIDER + "-renamed", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.update(USERNAME, user -> user.changePassword("new-password"))
                                );
                    });
        });

        assertEquals(2, events.size());

        AuthenticationProviderEventParameters providerEventParameters = (AuthenticationProviderEventParameters) events.get(0);
        assertEquals(AUTHENTICATION_PROVIDER_REMOVED, providerEventParameters.type());
        assertEquals(PROVIDER, providerEventParameters.name());

        UserEventParameters userEventParameters = (UserEventParameters) events.get(1);
        assertEquals(AuthenticationEvent.USER_UPDATED, userEventParameters.type());
        assertEquals(USERNAME, userEventParameters.username());
        assertEquals(PROVIDER + "-renamed", userEventParameters.providerName());
    }

    @Test
    void shouldFireProviderUpdatedEventWhenCustomProviderPropertyIsChanged() {
        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .create("custom", provider -> {
                        provider.convert(CustomAuthenticationProviderChange.class)
                                .changeCustomProperty("custom-property");
                    });
        });

        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update("custom", provider -> {
                        provider.convert(CustomAuthenticationProviderChange.class)
                                .changeCustomProperty("custom-property2");
                    });
        });

        assertEquals(1, events.size());
        AuthenticationProviderEventParameters authenticationProviderEventParameters = (AuthenticationProviderEventParameters) events.get(0);
        assertEquals(AUTHENTICATION_PROVIDER_UPDATED, authenticationProviderEventParameters.type());
        assertEquals("custom", authenticationProviderEventParameters.name());
    }

    @Test
    void shouldFireProviderRemovedEventWhenProviderIsRenamed() {
        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .rename(PROVIDER, PROVIDER + "-renamed");
        });

        assertEquals(1, events.size());
        AuthenticationProviderEventParameters authenticationProviderEventParameters = (AuthenticationProviderEventParameters) events.get(0);
        assertEquals(AUTHENTICATION_PROVIDER_REMOVED, authenticationProviderEventParameters.type());
        assertEquals(PROVIDER, authenticationProviderEventParameters.name());
    }

    @Test
    void shouldNotFireProviderUpdatedEventForChangesInBasicProviderUsers() {
        mutateConfiguration(securityConfiguration, change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update(PROVIDER, provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users ->
                                        users.create("new-user", user -> user.changePassword("new-password"))
                                );
                    });
        });

        assertEquals(0, events.size());
    }

    @Test
    void shouldAuthenticateWithValidCredentials() {
        // when
        enableAuthentication();

        // then
        // successful authentication with valid credentials
        assertEquals(USERNAME, manager.authenticateAsync(USERNAME_PASSWORD_REQUEST).username());
    }

    @Test
    void shouldThrowInvalidCredentialsExceptionForInvalidCredentials() {
        // when
        enableAuthentication();

        // then
        // failed authentication with invalid credentials
        assertThrows(InvalidCredentialsException.class,
                () -> manager.authenticateAsync(new UsernamePasswordRequest(USERNAME, "invalid-password")));
    }

    @Test
    void shouldReturnUnknownUserDetailsWhenAuthenticationIsDisabled() {
        // when
        disableAuthentication();

        // then
        assertEquals(UserDetails.UNKNOWN, manager.authenticateAsync(USERNAME_PASSWORD_REQUEST));
    }

    @Test
    public void shouldAuthenticateWithFallbackOnSequentialAuthenticatorExceptions() {
        UsernamePasswordRequest credentials = new UsernamePasswordRequest("admin", "password");

        Authenticator authenticator1 = mock(Authenticator.class);
        doThrow(new InvalidCredentialsException("Invalid credentials")).when(authenticator1).authenticateAsync(credentials);

        Authenticator authenticator2 = mock(Authenticator.class);
        doThrow(new UnsupportedAuthenticationTypeException("Unsupported type")).when(authenticator2).authenticateAsync(credentials);

        Authenticator authenticator3 = mock(Authenticator.class);
        doThrow(new RuntimeException("Test exception")).when(authenticator3).authenticateAsync(credentials);

        Authenticator authenticator4 = mock(Authenticator.class);
        doReturn(new UserDetails("admin", "mock")).when(authenticator4).authenticateAsync(credentials);

        manager.authEnabled(true);
        manager.authenticators(List.of(authenticator1, authenticator2, authenticator3, authenticator4));

        assertEquals("admin", manager.authenticateAsync(credentials).username());

        verify(authenticator1).authenticateAsync(credentials);
        verify(authenticator2).authenticateAsync(credentials);
        verify(authenticator3).authenticateAsync(credentials);
        verify(authenticator4).authenticateAsync(credentials);
    }

    private void enableAuthentication() {
        mutateConfiguration(securityConfiguration, change -> change.changeEnabled(true));
    }

    private void disableAuthentication() {
        mutateConfiguration(securityConfiguration, change -> change.changeEnabled(false));
    }

    private static void mutateConfiguration(SecurityConfiguration configuration, Consumer<SecurityChange> consumer) {
        CompletableFuture<SecurityConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);
        assertThat(future, willCompleteSuccessfully());
    }
}
