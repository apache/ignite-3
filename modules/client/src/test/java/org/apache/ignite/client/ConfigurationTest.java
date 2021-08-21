/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.ignite.configuration.schemas.client.ClientChange;
import org.apache.ignite.configuration.schemas.client.ClientConfiguration;
import org.apache.ignite.configuration.schemas.client.ClientView;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests thin client configuration.
 */
public class ConfigurationTest extends AbstractClientTest {
    @Test
    public void testClientBuilder() throws Exception {
        IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:" + serverPort)
                .connectTimeout(1234)
                .build();

        checkClient(client);
    }

    @Test
    public void testConfigurationBuilder() throws Exception {
        ClientConfiguration config = IgniteClient.configurationBuilder();

        config.change((ClientChange c) -> c
                .changeAddresses("127.0.0.1:" + serverPort)
                .changeConnectTimeout(1234))
                .join();

        // ClientView is an immutable representation of ClientConfiguration.
        ClientView value = config.value();

        IgniteClient client = IgniteClient.startAsync(value).join();

        checkClient(client);
    }

    private static void checkClient(IgniteClient client) throws Exception {
        try (client) {
            // Check that client works.
            assertEquals(0, client.tables().tables().size());

            // Check config values.
            assertEquals(1234, client.configuration().connectTimeout());
            assertArrayEquals(new String[]{"127.0.0.1:" + serverPort}, client.configuration().addresses());
        }
    }
}
