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

package org.apache.ignite.internal.runner.app.client;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.BasicAuthenticator;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.apache.ignite.sql.Session;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Thin client authentication test.
 */
public class ItThinClientAuthenticationTest extends ItAbstractThinClientTest {
    private static final String USERNAME = "admin";

    private static final String PASSWORD = "password";

    /** Client. */
    private IgniteClient clientWithAuth;

    private SecurityConfiguration securityConfiguration;

    private final BasicAuthenticator basicAuthenticator = BasicAuthenticator.builder()
            .username(USERNAME)
            .password(PASSWORD)
            .build();

    @BeforeEach
    void setUp() {
        IgniteImpl server = (IgniteImpl) server();
        securityConfiguration = server.clusterConfiguration().getConfiguration(SecurityConfiguration.KEY);

        CompletableFuture<Void> enableAuthentication = securityConfiguration.change(change -> {
            change.changeEnabled(true);
            change.changeAuthentication()
                    .changeProviders()
                    .update("default", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers()
                                .createOrUpdate(USERNAME, user -> user.changePassword(PASSWORD));
                    });
        });

        assertThat(enableAuthentication, willCompleteSuccessfully());

        clientWithAuth = IgniteClient.builder()
                .authenticator(basicAuthenticator)
                .reconnectThrottlingRetries(0)
                .addresses(getClientAddresses().toArray(new String[0]))
                .build();

        await().untilAsserted(() -> checkConnection(clientWithAuth));
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(clientWithAuth);
    }

    @Test
    void connectionIsClosedIfAuthenticationEnabled() {
        await().until(() -> checkConnection(client()), willThrowWithCauseOrSuppressed(InvalidCredentialsException.class));
    }

    @Test
    void connectionIsClosedIfPasswordChanged() {
        assertThat(securityConfiguration.change(change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update("default", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers()
                                .update(USERNAME, user -> user.changePassword("newPassword"));
                    });
        }), willCompleteSuccessfully());

        await().until(() -> checkConnection(clientWithAuth), willThrowWithCauseOrSuppressed(InvalidCredentialsException.class));
    }

    @Test
    void connectionIsClosedIfUserRemoved() {
        assertThat(securityConfiguration.change(change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update("default", provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers()
                                .delete(USERNAME);
                    });
        }), willCompleteSuccessfully());

        await().until(() -> checkConnection(clientWithAuth), willThrowWithCauseOrSuppressed(InvalidCredentialsException.class));
    }

    private static CompletableFuture<Void> checkConnection(IgniteClient client) {
        try (Session session = client.sql().createSession()) {
            return session.executeAsync(null, "select 1 as num, 'hello' as str")
                    .thenApply(ignored -> null);
        }
    }
}
