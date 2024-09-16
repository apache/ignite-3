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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.configuration.hocon.HoconConverter.hoconSource;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrowWithCauseOrSuppressed;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;

import com.typesafe.config.ConfigFactory;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.client.BasicAuthenticator;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityExtensionConfiguration;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Thin client authentication test.
 */
public class ItThinClientAuthenticationTest extends ItAbstractThinClientTest {
    private static final String PROVIDER_NAME = "default";

    private static final String USERNAME_1 = "admin";

    private static final String PASSWORD_1 = "password";

    private static final String USERNAME_2 = "developer";

    private static final String PASSWORD_2 = "password";

    private IgniteClient clientWithAuth;

    private SecurityConfiguration securityConfiguration;

    private final BasicAuthenticator basicAuthenticator = BasicAuthenticator.builder()
            .username(USERNAME_1)
            .password(PASSWORD_1)
            .build();

    @BeforeEach
    void setUp() {
        securityConfiguration = clusterConfigurationRegistry().getConfiguration(SecurityExtensionConfiguration.KEY).security();

        CompletableFuture<Void> enableAuthentication = securityConfiguration.change(change -> {
            change.changeEnabled(true);
            change.changeAuthentication()
                    .changeProviders(providers -> {
                        providers.namedListKeys().forEach(name -> {
                            if (!name.equals(PROVIDER_NAME)) {
                                providers.delete(name);
                            }
                        });

                        providers.createOrUpdate(PROVIDER_NAME, provider -> {
                            provider.convert(BasicAuthenticationProviderChange.class)
                                    .changeUsers()
                                    .createOrUpdate(USERNAME_1, user -> user.changePassword(PASSWORD_1))
                                    .createOrUpdate(USERNAME_2, user -> user.changePassword(PASSWORD_2));
                        });
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
        closeAll(clientWithAuth);
    }

    @Test
    void connectionIsNotClosedIfAnotherUserUpdated() {
        assertThat(
                securityConfiguration.authentication().providers()
                        .get(PROVIDER_NAME)
                        .change(change -> {
                            change.convert(BasicAuthenticationProviderChange.class)
                                    .changeUsers()
                                    .update(USERNAME_2, user -> user.changePassword(PASSWORD_2 + "-changed"));
                        }),
                willCompleteSuccessfully()
        );

        // Connection should be alive after update.
        await().during(3, TimeUnit.SECONDS)
                .until(() -> checkConnection(clientWithAuth), willCompleteSuccessfully());
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
                    .update(PROVIDER_NAME, provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers()
                                .update(USERNAME_1, user -> user.changePassword("newPassword"));
                    });
        }), willCompleteSuccessfully());

        await().until(() -> checkConnection(clientWithAuth), willThrowWithCauseOrSuppressed(InvalidCredentialsException.class));
    }

    @Test
    void connectionIsClosedIfUserRemoved() {
        assertThat(securityConfiguration.change(change -> {
            change.changeAuthentication()
                    .changeProviders()
                    .update(PROVIDER_NAME, provider -> {
                        provider.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers()
                                .delete(USERNAME_1);
                    });
        }), willCompleteSuccessfully());

        await().until(() -> checkConnection(clientWithAuth), willThrowWithCauseOrSuppressed(InvalidCredentialsException.class));
    }

    @Test
    void renameBasicProviderAndThenChangeUserPassword() {
        updateClusterConfiguration("ignite {\n"
                + "security.authentication.providers.basic={\n"
                + "type=basic,\n"
                + "users=[{username=newuser,password=newpassword}]},"
                + "security.authentication.providers.default=null\n"
                + "}");

        try (IgniteClient client = IgniteClient.builder()
                .authenticator(BasicAuthenticator.builder().username("newuser").password("newpassword").build())
                .reconnectThrottlingRetries(0)
                .addresses(getClientAddresses().toArray(new String[0]))
                .build()) {

            checkConnection(client);

            securityConfiguration.authentication().providers()
                    .get("basic")
                    .change(change -> {
                        change.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers()
                                .update("newuser", user -> user.changePassword("newpassword-changed"));
                    }).join();

            await().until(() -> checkConnection(client), willThrowWithCauseOrSuppressed(InvalidCredentialsException.class));
        }
    }

    private static CompletableFuture<Void> checkConnection(IgniteClient client) {
        return client.sql().executeAsync(null, "select 1 as num, 'hello' as str")
                .thenApply(ignored -> null);
    }

    private void updateClusterConfiguration(String hocon) {
        assertThat(
                clusterConfigurationRegistry().change(hoconSource(ConfigFactory.parseString(hocon).root())),
                willCompleteSuccessfully()
        );
    }

    private ConfigurationRegistry clusterConfigurationRegistry() {
        IgniteImpl server = unwrapIgniteImpl(server());
        return server.clusterConfiguration();
    }
}
