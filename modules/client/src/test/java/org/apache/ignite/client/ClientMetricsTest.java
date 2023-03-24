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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.client.ClientMetricSource;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests client-side metrics (see also server-side metrics tests in {@link ServerMetricsTest}).
 */
public class ClientMetricsTest {
    private TestServer server;
    private IgniteClient client;

    @Test
    public void testActiveConnections() throws Exception {
        server = AbstractClientTest.startServer(10800, 10, 1000, new FakeIgnite());
        client = IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port())
                .metricsEnabled(true)
                .build();

        ClientMetricSource metrics = ((TcpIgniteClient)client).metrics();

        assertEquals(1, metrics.connectionsEstablished());
        assertEquals(1, metrics.connectionsActive());

        server.close();

        assertTrue(
                IgniteTestUtils.waitForCondition(() -> metrics.connectionsActive() == 0, 1000),
                () -> "connectionsActive: " + metrics.connectionsActive());
    }

    @AfterEach
    public void afterAll() throws Exception {
        client.close();
        server.close();
    }
}
