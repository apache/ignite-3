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

import org.apache.ignite.app.Ignite;
import org.junit.jupiter.api.Test;

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

        try (client) {
            assertEquals(0, client.tables().tables().size());

            assertEquals(1234, client.configuration().getConnectTimeout());
        }
    }

    @Test
    public void testConfigurationBuilder() throws Exception {
        var config = IgniteClient.configurationBuilder();

        config.change(c -> c
                .changeAddresses("127.0.0.1:" + serverPort)
                .changeConnectTimeout(1234))
                .join();

        IgniteClient client = IgniteClient.start(config);

        try (client) {
            assertEquals(0, client.tables().tables().size());

            assertEquals(1234, client.configuration().getConnectTimeout());
        }
    }
}
