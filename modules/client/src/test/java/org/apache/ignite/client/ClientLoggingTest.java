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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.LoggerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests verifies an ability to set custom logger to the client.
 */
public class ClientLoggingTest {
    /** Test server. */
    private TestServer server;

    /** Test server 2. */
    private TestServer server2;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server, server2);
    }

    @Test
    public void loggersSetToDifferentClientsNotInterfereWithEachOther() throws Exception {
        FakeIgnite ignite1 = new FakeIgnite();
        ((FakeIgniteTables) ignite1.tables()).createTable("t");

        server = startServer(10950, ignite1);

        var loggerFactory1 = new TestLoggerFactory("client1");
        var loggerFactory2 = new TestLoggerFactory("client2");

        var client1 = createClient(loggerFactory1);
        var client2 = createClient(loggerFactory2);

        assertEquals("t", client1.tables().tables().get(0).name());
        assertEquals("t", client2.tables().tables().get(0).name());

        server.close();

        FakeIgnite ignite2 = new FakeIgnite();
        ((FakeIgniteTables) ignite2.tables()).createTable("t2");

        server2 = startServer(10951, ignite2);

        assertEquals("t2", client1.tables().tables().get(0).name());
        assertEquals("t2", client2.tables().tables().get(0).name());

        assertThat(loggerFactory1.logger.entries(), not(empty()));
        assertThat(loggerFactory2.logger.entries(), not(empty()));

        loggerFactory1.logger.entries().forEach(msg -> assertThat(msg, startsWith("client1:")));
        loggerFactory2.logger.entries().forEach(msg -> assertThat(msg, startsWith("client2:")));
    }

    @Test
    public void testBasicLogging() throws Exception {
        FakeIgnite ignite = new FakeIgnite();
        ((FakeIgniteTables) ignite.tables()).createTable("t");

        server = startServer(10950, ignite);
        server2 = startServer(10955, ignite);

        var loggerFactory = new TestLoggerFactory("c");

        try (var client = createClient(loggerFactory)) {
            client.tables().tables();
            client.tables().table("t");

            assertTrue(IgniteTestUtils.waitForCondition(() -> loggerFactory.logger.entries().size() > 10, 5_000));

            loggerFactory.assertLogContains("Connection established");
            loggerFactory.assertLogContains("c:Sending request [opCode=3, remoteAddress=127.0.0.1:1095");
            loggerFactory.assertLogContains("c:Failed to establish connection to 127.0.0.1:1095");
        }
    }

    private static TestServer startServer(int port, FakeIgnite ignite) {
        return AbstractClientTest.startServer(
                port,
                10,
                0,
                ignite
        );
    }

    private static IgniteClient createClient(LoggerFactory loggerFactory) {
        return IgniteClient.builder()
                .addresses("127.0.0.1:10950..10960")
                .loggerFactory(loggerFactory)
                .build();
    }
}
