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

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.UUID;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.configuration.AuthenticationConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests client authentication.
 */
@ExtendWith(ConfigurationExtension.class)
public class ClientAuthenticationTest {
    @InjectConfiguration
    private AuthenticationConfiguration authenticationConfiguration;

    private TestServer server;

    private IgniteClient client;

    @AfterEach
    public void afterEach() throws Exception {
        IgniteUtils.closeAll(client, server);
    }

    // TODO: No authn on server, no authn on client
    // TODO: Authn on server, no authn on client
    // TODO: No authn on server, authn on client
    // TODO: Authn on server, authn on client (valid creds)
    // TODO: Authn on server, authn on client (invalid creds)
    @Test
    public void testNoAuthnOnServerNoAuthnOnClient() throws Exception {
        assertNotNull(authenticationConfiguration);

        server = new TestServer(
                10800,
                10,
                1000,
                new FakeIgnite(),
                null,
                null,
                null,
                UUID.randomUUID(),
                authenticationConfiguration);
    }
}
