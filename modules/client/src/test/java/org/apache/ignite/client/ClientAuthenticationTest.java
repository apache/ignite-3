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

import static org.apache.ignite.configuration.annotation.ConfigurationType.DISTRIBUTED;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.UUID;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.configuration.ClusterConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityExtensionConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.security.exception.InvalidCredentialsException;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests client authentication.
 */
@SuppressWarnings({"resource", "ThrowableNotThrown"})
@ExtendWith(ConfigurationExtension.class)
public class ClientAuthenticationTest extends BaseIgniteAbstractTest {
    @InjectConfiguration(rootName = "ignite", type = DISTRIBUTED)
    private ClusterConfiguration clusterConfiguration;

    private TestServer server;

    private IgniteClient client;

    @AfterEach
    public void afterEach() throws Exception {
        closeAll(client, server);
    }

    @Test
    public void testNoAuthnOnServerNoAuthnOnClient() {
        server = startServer(false);
        client = startClient(null);
    }

    @Test
    public void testAuthnOnClientNoAuthnOnServer() {
        server = startServer(false);

        client = startClient(BasicAuthenticator.builder().username("u").password("p").build());
    }

    @Test
    public void testAuthnOnServerNoAuthnOnClient() {
        server = startServer(true);

        IgniteTestUtils.assertThrowsWithCause(() -> startClient(null), InvalidCredentialsException.class, "Authentication failed");
    }

    @Test
    public void testAuthnOnServerBadAuthnOnClient() {
        server = startServer(true);

        BasicAuthenticator authenticator = BasicAuthenticator.builder().username("u").password("p").build();

        IgniteTestUtils.assertThrowsWithCause(() -> startClient(authenticator), InvalidCredentialsException.class, "Authentication failed");
    }

    @Test
    public void testAuthnOnClientAuthnOnServer() {
        server = startServer(false);

        client = startClient(BasicAuthenticator.builder().username("usr").password("pwd").build());
    }

    private IgniteClient startClient(@Nullable IgniteClientAuthenticator authenticator) {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .authenticator(authenticator)
                .build();
    }

    private TestServer startServer(boolean basicAuthn) {
        SecurityConfiguration securityConfiguration = ((SecurityExtensionConfiguration) clusterConfiguration).security();

        var server = new TestServer(
                1000,
                new FakeIgnite(),
                null,
                null,
                null,
                UUID.randomUUID(),
                securityConfiguration,
                null);

        if (basicAuthn) {
            securityConfiguration.change(securityChange -> {
                securityChange.changeEnabled(true);
                securityChange.changeAuthentication().changeProviders().create("basic", change ->
                        change.convert(BasicAuthenticationProviderChange.class)
                                .changeUsers(users -> users.create("usr", user ->
                                        user.changePassword("pwd"))
                                )
                );
            }).join();
        }

        return server;
    }
}
