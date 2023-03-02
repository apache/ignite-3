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

import static org.apache.ignite.internal.rest.authentication.TestSubscriberUtils.subscribeToValue;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willFailFast;
import static org.hamcrest.MatcherAssert.assertThat;

import io.micronaut.http.HttpMethod;
import io.micronaut.http.simple.SimpleHttpRequest;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import java.util.Collections;
import org.apache.ignite.internal.configuration.AuthenticationView;
import org.apache.ignite.internal.configuration.stub.StubAuthenticationView;
import org.apache.ignite.internal.configuration.stub.StubAuthenticationViewEvent;
import org.apache.ignite.internal.configuration.stub.StubBasicAuthenticationProviderView;
import org.junit.jupiter.api.Test;

class DelegatingAuthenticationProviderTest {

    private final SimpleHttpRequest<Object> httpRequest = new SimpleHttpRequest<>(HttpMethod.GET, "/", null);

    private final DelegatingAuthenticationProvider authenticationProvider = new DelegatingAuthenticationProvider();

    @Test
    public void enableAuth() throws Throwable {
        // when
        AuthenticationView adminPasswordAuthView = new StubAuthenticationView(true,
                new StubBasicAuthenticationProviderView("admin", "password"));
        authenticationProvider.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordCredentials validCredentials = new UsernamePasswordCredentials("admin", "password");
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, validCredentials)), willCompleteSuccessfully());

        // unsuccessful authentication with invalid credentials
        UsernamePasswordCredentials invalidCredentials = new UsernamePasswordCredentials("admin", "wrong-password");
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, invalidCredentials)),
                willFailFast(AuthenticationException.class));

    }

    @Test
    public void leaveOldSettingWhenInvalidConfiguration() {
        // when
        AuthenticationView invalidAuthView = new StubAuthenticationView(true, Collections.emptyList());
        authenticationProvider.onUpdate(new StubAuthenticationViewEvent(null, invalidAuthView)).join();

        // then
        // authentication is still disabled
        UsernamePasswordCredentials emptyCredentials = new UsernamePasswordCredentials();
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, emptyCredentials)), willCompleteSuccessfully());
    }

    @Test
    public void disableAuthEmptyProviders() {
        //when
        AuthenticationView adminPasswordAuthView = new StubAuthenticationView(true,
                new StubBasicAuthenticationProviderView("admin", "password"));

        authenticationProvider.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordCredentials validCredentials = new UsernamePasswordCredentials("admin", "password");
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, validCredentials)), willCompleteSuccessfully());

        // disable authentication
        AuthenticationView disabledAuthView = new StubAuthenticationView(false, Collections.emptyList());
        authenticationProvider.onUpdate(new StubAuthenticationViewEvent(adminPasswordAuthView, disabledAuthView)).join();

        // then
        // authentication is disabled
        UsernamePasswordCredentials emptyCredentials = new UsernamePasswordCredentials();
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, emptyCredentials)), willCompleteSuccessfully());
    }

    @Test
    public void disableAuthNotEmptyProviders() {
        //when
        AuthenticationView adminPasswordAuthView = new StubAuthenticationView(true,
                new StubBasicAuthenticationProviderView("admin", "password"));

        authenticationProvider.onUpdate(new StubAuthenticationViewEvent(null, adminPasswordAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordCredentials validCredentials = new UsernamePasswordCredentials("admin", "password");
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, validCredentials)), willCompleteSuccessfully());

        // disable authentication
        AuthenticationView disabledAuthView = new StubAuthenticationView(false,
                new StubBasicAuthenticationProviderView("admin", "password"));
        authenticationProvider.onUpdate(new StubAuthenticationViewEvent(adminPasswordAuthView, disabledAuthView)).join();

        // then
        // authentication is disabled
        UsernamePasswordCredentials emptyCredentials = new UsernamePasswordCredentials();
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, emptyCredentials)), willCompleteSuccessfully());
    }

    @Test
    public void changedCredentials() {
        // when
        AuthenticationView adminAdminAuthView = new StubAuthenticationView(true,
                new StubBasicAuthenticationProviderView("admin", "password"));

        authenticationProvider.onUpdate(new StubAuthenticationViewEvent(null, adminAdminAuthView)).join();

        // then
        // successful authentication with valid credentials
        UsernamePasswordCredentials adminAdminCredentials = new UsernamePasswordCredentials("admin", "password");
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, adminAdminCredentials)), willCompleteSuccessfully());

        // change authentication settings - change password
        AuthenticationView adminPasswordAuthView = new StubAuthenticationView(true,
                new StubBasicAuthenticationProviderView("admin", "new-password"));
        authenticationProvider.onUpdate(new StubAuthenticationViewEvent(adminAdminAuthView, adminPasswordAuthView)).join();

        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, adminAdminCredentials)),
                willFailFast(AuthenticationException.class));

        // then
        // successful authentication with the new password
        UsernamePasswordCredentials adminPasswordCredentials = new UsernamePasswordCredentials("admin", "new-password");
        assertThat(subscribeToValue(authenticationProvider.authenticate(httpRequest, adminPasswordCredentials)),
                willCompleteSuccessfully());
    }
}
