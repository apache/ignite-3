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

package org.apache.ignite.internal.rest.auth;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.http.HttpMethod;
import io.micronaut.http.simple.SimpleHttpRequest;
import io.micronaut.security.authentication.AuthenticationException;
import io.micronaut.security.authentication.UsernamePasswordCredentials;
import java.util.Collections;
import org.apache.ignite.internal.rest.configuration.AuthView;
import org.apache.ignite.internal.rest.configuration.stub.StubAuthView;
import org.apache.ignite.internal.rest.configuration.stub.StubAuthViewEvent;
import org.apache.ignite.internal.rest.configuration.stub.StubBasicAuthProviderView;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DelegatingAuthenticationProviderTest {

    private final SimpleHttpRequest<Object> httpRequest = new SimpleHttpRequest<>(HttpMethod.GET, "/", null);

    private DelegatingAuthenticationProvider authenticationProvider;

    @BeforeEach
    void setUp() {
        authenticationProvider = new DelegatingAuthenticationProvider();
    }


    @Test
    public void enableAuth() {
        TestAuthSubscriber subscriber = new TestAuthSubscriber();

        AuthView enableAuthView = new StubAuthView(true, new StubBasicAuthProviderView("admin", "admin"));
        authenticationProvider.onUpdate(new StubAuthViewEvent(null, enableAuthView)).join();

        UsernamePasswordCredentials validCredentials = new UsernamePasswordCredentials("admin", "admin");
        authenticationProvider.authenticate(httpRequest, validCredentials).subscribe(subscriber);
        assertAll(
                () -> assertNull(subscriber.lastError()),
                () -> assertTrue(subscriber.lastResponse().isAuthenticated())
        );

        UsernamePasswordCredentials invalidCredentials = new UsernamePasswordCredentials("admin", "password");
        authenticationProvider.authenticate(httpRequest, invalidCredentials).subscribe(subscriber);
        assertAll(
                () -> assertThat(subscriber.lastError(), is(instanceOf(AuthenticationException.class))),
                () -> assertNull(subscriber.lastResponse())
        );
    }

    @Test
    public void enableAuthWithEmptyProviders() {
        TestAuthSubscriber subscriber = new TestAuthSubscriber();

        AuthView enableAuthView = new StubAuthView(true, Collections.emptyList());
        authenticationProvider.onUpdate(new StubAuthViewEvent(null, enableAuthView)).join();

        UsernamePasswordCredentials emptyCredentials = new UsernamePasswordCredentials();
        authenticationProvider.authenticate(httpRequest, emptyCredentials).subscribe(subscriber);
        assertAll(
                () -> assertNull(subscriber.lastError()),
                () -> assertTrue(subscriber.lastResponse().isAuthenticated())
        );
    }

    @Test
    public void disableAuth() {
        AuthView enableAuthView = new StubAuthView(true, new StubBasicAuthProviderView("admin", "admin"));

        authenticationProvider.onUpdate(new StubAuthViewEvent(null, enableAuthView)).join();

        UsernamePasswordCredentials validCredentials = new UsernamePasswordCredentials("admin", "admin");
        TestAuthSubscriber subscriber = new TestAuthSubscriber();
        authenticationProvider.authenticate(httpRequest, validCredentials).subscribe(subscriber);
        assertAll(
                () -> assertNull(subscriber.lastError()),
                () -> assertTrue(subscriber.lastResponse().isAuthenticated()));

        AuthView disableAuthView = new StubAuthView(false, Collections.emptyList());
        authenticationProvider.onUpdate(new StubAuthViewEvent(enableAuthView, disableAuthView)).join();

        UsernamePasswordCredentials emptyCredentials = new UsernamePasswordCredentials();
        authenticationProvider.authenticate(httpRequest, emptyCredentials).subscribe(subscriber);
        assertAll(
                () -> assertNull(subscriber.lastError()),
                () -> assertTrue(subscriber.lastResponse().isAuthenticated())
        );
    }

    @Test
    public void changedCredentials() {
        AuthView adminAdminAuthView = new StubAuthView(true, new StubBasicAuthProviderView("admin", "admin"));

        authenticationProvider.onUpdate(new StubAuthViewEvent(null, adminAdminAuthView)).join();

        UsernamePasswordCredentials adminAdminCredentials = new UsernamePasswordCredentials("admin", "admin");
        TestAuthSubscriber subscriber = new TestAuthSubscriber();
        authenticationProvider.authenticate(httpRequest, adminAdminCredentials).subscribe(subscriber);
        assertAll(
                () -> assertNull(subscriber.lastError()),
                () -> assertTrue(subscriber.lastResponse().isAuthenticated())
        );

        AuthView adminPasswordAuthView = new StubAuthView(true, new StubBasicAuthProviderView("admin", "password"));
        authenticationProvider.onUpdate(new StubAuthViewEvent(adminAdminAuthView, adminPasswordAuthView)).join();

        authenticationProvider.authenticate(httpRequest, adminAdminCredentials).subscribe(subscriber);
        assertAll(
                () -> assertThat(subscriber.lastError(), is(instanceOf(AuthenticationException.class))),
                () -> assertNull(subscriber.lastResponse())
        );

        UsernamePasswordCredentials adminPasswordCredentials = new UsernamePasswordCredentials("admin", "password");
        authenticationProvider.authenticate(httpRequest, adminPasswordCredentials).subscribe(subscriber);
        assertAll(
                () -> assertNull(subscriber.lastError()),
                () -> assertTrue(subscriber.lastResponse().isAuthenticated())
        );
    }
}
