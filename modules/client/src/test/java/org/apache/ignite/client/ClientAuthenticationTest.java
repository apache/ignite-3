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

package org.apache.ignite.client;

import java.util.UUID;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.security.AuthenticationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests client authentication.
 */
@SuppressWarnings({"resource", "ThrowableNotThrown"})
@ExtendWith(ConfigurationExtension.class)
public class ClientAuthenticationTest {
    @SuppressWarnings("unused")
    @InjectConfiguration
    private AuthenticationConfiguration authenticationConfiguration;

    private TestServer server;

    private IgniteClient client;

    @BeforeEach
    public void beforeEach() {
        authenticationConfiguration.change(change -> {
            change.changeEnabled(false);
            change.changeProviders().delete("basic");
        }).join();
    }

    @AfterEach
    public void afterEach() throws Exception {
        IgniteUtils.closeAll(client, server);
    }

    @Test
    public void testNoAuthnOnServerNoAuthnOnClient() {
        server = startServer(false);
        client = startClient(null);
    }

    @Test
    public void testAuthnOnClientNoAuthnOnServer() {
        server = startServer(false);

        startClient(BasicAuthenticator.builder().username("u").password("p").build());
    }

    @Test
    public void testAuthnOnServerNoAuthnOnClient() {
        server = startServer(true);

        IgniteTestUtils.assertThrowsWithCause(() -> startClient(null), AuthenticationException.class, "Authentication failed");
    }

    @Test
    public void testAuthnOnServerBadAuthnOnClient() {
        server = startServer(true);

        BasicAuthenticator authenticator = BasicAuthenticator.builder().username("u").password("p").build();

        IgniteTestUtils.assertThrowsWithCause(() -> startClient(authenticator), AuthenticationException.class, "Authentication failed");
    }

    @Test
    public void testAuthnOnClientAuthnOnServer() {
        server = startServer(false);

        startClient(BasicAuthenticator.builder().username("usr").password("pwd").build());
    }

    private IgniteClient startClient(@Nullable IgniteClientAuthenticator authenticator) {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .authenticator(authenticator)
                .build();
    }

    @NotNull
    private TestServer startServer(boolean basicAuthn) {
        var server = new TestServer(
                1000,
                new FakeIgnite(),
                null,
                null,
                null,
                UUID.randomUUID(),
                authenticationConfiguration,
                null);

        if (basicAuthn) {
            authenticationConfiguration.change(change -> {
                change.changeEnabled(true);
                change.changeProviders().create("basic", authenticationProviderChange ->
                        authenticationProviderChange.convert(BasicAuthenticationProviderChange.class)
                                .changeUsername("usr")
                                .changePassword("pwd"));
            }).join();
        }

        return server;
    }
}
