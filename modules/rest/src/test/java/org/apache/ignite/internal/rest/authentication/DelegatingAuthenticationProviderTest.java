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

package org.apache.ignite.internal.rest.authentication;

import static org.apache.ignite.internal.testframework.flow.TestFlowUtils.subscribeToValue;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailFast;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertAll;
import static reactor.adapter.JdkFlowAdapter.publisherToFlowPublisher;

import io.micronaut.http.HttpMethod;
import io.micronaut.http.simple.SimpleHttpRequest;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.AuthenticationResponse;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.configuration.AuthenticationChange;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.AuthenticationView;
import org.apache.ignite.internal.configuration.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith(ConfigurationExtension.class)
class DelegatingAuthenticationProviderTest {

    private final SimpleHttpRequest<Object> httpRequest = new SimpleHttpRequest<>(HttpMethod.GET, "/", null);

    private final DelegatingAuthenticationProvider provider = new DelegatingAuthenticationProvider();

    @InjectConfiguration
    private AuthenticationConfiguration authenticationConfiguration;

    @Test
    public void enableAuth() {
        // when
        AuthenticationView adminPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeLogin("admin")
                                .changePassword("password")
                                .changeName("basic");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        provider.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordCredentials validCredentials = new UsernamePasswordCredentials("admin", "password");

        CompletableFuture<AuthenticationResponse> authenticate = authenticate(provider, validCredentials);
        assertAll(
                () -> assertThat(authenticate, willCompleteSuccessfully()),
                () -> assertThat(authenticate.join().isAuthenticated(), is(true))
        );

        // unsuccessful authentication with invalid credentials
        UsernamePasswordCredentials invalidCredentials = new UsernamePasswordCredentials("admin", "wrong-password");
        assertThat(authenticate(provider, invalidCredentials), willFailFast(AuthenticationException.class));

    }

    @Test
    public void leaveOldSettingWhenInvalidConfiguration() {
        // when
        AuthenticationView invalidAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeEnabled(true);
                })
                .value();
        provider.onUpdate(new StubAuthenticationViewEvent(null, invalidAuthView)).join();

        // then
        // authentication is still disabled
        UsernamePasswordCredentials emptyCredentials = new UsernamePasswordCredentials();

        CompletableFuture<AuthenticationResponse> authenticate = authenticate(provider, emptyCredentials);
        assertAll(
                () -> assertThat(authenticate, willCompleteSuccessfully()),
                () -> assertThat(authenticate.join().isAuthenticated(), is(true))
        );
    }

    @Test
    public void disableAuthEmptyProviders() {
        //when
        AuthenticationView adminPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeLogin("admin")
                                .changePassword("password")
                                .changeName("basic");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        provider.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then

        // just to be sure that authentication is enabled
        // successful authentication with valid credentials
        UsernamePasswordCredentials validCredentials = new UsernamePasswordCredentials("admin", "password");

        CompletableFuture<AuthenticationResponse> validCredentialResponse = authenticate(provider, validCredentials);
        assertAll(
                () -> assertThat(validCredentialResponse, willCompleteSuccessfully()),
                () -> assertThat(validCredentialResponse.join().isAuthenticated(), is(true))
        );

        // disable authentication
        AuthenticationView disabledAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.delete("basic"));
                    change.changeEnabled(false);
                })
                .value();

        provider.onUpdate(new StubAuthenticationViewEvent(adminPasswordAuthView, disabledAuthView)).join();

        // then
        // authentication is disabled
        UsernamePasswordCredentials emptyCredentials = new UsernamePasswordCredentials();

        CompletableFuture<AuthenticationResponse> emptyCredentialsResponse = authenticate(provider, emptyCredentials);
        assertAll(
                () -> assertThat(emptyCredentialsResponse, willCompleteSuccessfully()),
                () -> assertThat(emptyCredentialsResponse.join().isAuthenticated(), is(true))
        );

    }

    @Test
    public void disableAuthNotEmptyProviders() {
        //when
        AuthenticationView adminPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeLogin("admin")
                                .changePassword("password")
                                .changeName("basic");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        provider.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordCredentials validCredentials = new UsernamePasswordCredentials("admin", "password");

        CompletableFuture<AuthenticationResponse> validCredentialsResponse = authenticate(provider, validCredentials);
        assertAll(
                () -> assertThat(validCredentialsResponse, willCompleteSuccessfully()),
                () -> assertThat(validCredentialsResponse.join().isAuthenticated(), is(true))
        );

        // disable authentication
        AuthenticationView disabledAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeEnabled(false);
                })
                .value();

        provider.onUpdate(new StubAuthenticationViewEvent(adminPasswordAuthView, disabledAuthView)).join();

        // then
        // authentication is disabled
        UsernamePasswordCredentials emptyCredentials = new UsernamePasswordCredentials();

        CompletableFuture<AuthenticationResponse> emptyCredentialsResponse = authenticate(provider, emptyCredentials);
        assertAll(
                () -> assertThat(emptyCredentialsResponse, willCompleteSuccessfully()),
                () -> assertThat(emptyCredentialsResponse.join().isAuthenticated(), is(true))
        );
    }

    @Test
    public void changedCredentials() {
        // when
        AuthenticationView adminPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.create("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeLogin("admin")
                                .changePassword("password")
                                .changeName("basic");
                    }));
                    change.changeEnabled(true);
                })
                .value();

        provider.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordCredentials adminPasswordCredentials = new UsernamePasswordCredentials("admin", "password");

        CompletableFuture<AuthenticationResponse> adminPasswordResponse = authenticate(provider, adminPasswordCredentials);
        assertAll(
                () -> assertThat(adminPasswordResponse, willCompleteSuccessfully()),
                () -> assertThat(adminPasswordResponse.join().isAuthenticated(), is(true))
        );

        // change authentication settings - change password
        AuthenticationView adminNewPasswordAuthView = mutateConfiguration(
                authenticationConfiguration, change -> {
                    change.changeProviders(providers -> providers.update("basic", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeLogin("admin")
                                .changePassword("new-password")
                                .changeName("basic");
                    }));
                })
                .value();

        provider.onUpdate(new StubAuthenticationViewEvent(adminPasswordAuthView, adminNewPasswordAuthView)).join();

        assertThat(authenticate(provider, adminPasswordCredentials), willFailFast(AuthenticationException.class));

        // then
        // successful authentication with the new password
        UsernamePasswordCredentials adminNewPasswordCredentials = new UsernamePasswordCredentials("admin", "new-password");

        CompletableFuture<AuthenticationResponse> adminNewPasswordResponse = authenticate(provider, adminNewPasswordCredentials);
        assertAll(
                () -> assertThat(adminNewPasswordResponse, willCompleteSuccessfully()),
                () -> assertThat(adminNewPasswordResponse.join().isAuthenticated(), is(true))
        );
    }

    private CompletableFuture<AuthenticationResponse> authenticate(
            DelegatingAuthenticationProvider provider,
            UsernamePasswordCredentials credentials
    ) {
        return subscribeToValue(publisherToFlowPublisher(provider.authenticate(httpRequest, credentials)));
    }

    private static AuthenticationConfiguration mutateConfiguration(AuthenticationConfiguration configuration,
            Consumer<AuthenticationChange> consumer) {
        CompletableFuture<AuthenticationConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);
        assertThat(future, willCompleteSuccessfully());
        return future.join();
    }
}
