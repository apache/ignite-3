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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests thin client configuration.
 */
public class ConfigurationTest extends AbstractClientTest {
    @Test
    public void testClientBuilderPropagatesAllConfigurationValues() throws Exception {
        String addr = "127.0.0.1:" + serverPort;

        IgniteClient client = IgniteClient.builder()
                .addresses(addr)
                .connectTimeout(1234)
                .retryLimit(7)
                .reconnectThrottlingPeriod(123)
                .reconnectThrottlingRetries(8)
                .addressFinder(() -> new String[] {addr})
                .build();

        try (client) {
            // Check that client works.
            assertEquals(0, client.tables().tables().size());

            // Check config values.
            assertEquals("thin-client", client.name());
            assertEquals(1234, client.configuration().connectTimeout());
            assertEquals(7, client.configuration().retryLimit());
            assertEquals(123, client.configuration().reconnectThrottlingPeriod());
            assertEquals(8, client.configuration().reconnectThrottlingRetries());
            assertArrayEquals(new String[]{addr}, client.configuration().addresses());
            assertArrayEquals(new String[]{addr}, client.configuration().addressesFinder().getAddresses());
        }
    }
}
