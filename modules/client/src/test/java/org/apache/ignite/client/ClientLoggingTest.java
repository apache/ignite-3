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

import java.util.ArrayList;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.client.fakes.FakeIgniteTables;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.LoggerFactory;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests verifies an ability to set custom logger to the client.
 */
public class ClientLoggingTest extends BaseIgniteAbstractTest {
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

        server = startServer(ignite1, 10901);

        var loggerFactory1 = new TestLoggerFactory("client1");
        var loggerFactory2 = new TestLoggerFactory("client2");

        var client1 = createClient(loggerFactory1, 10901, 10902);
        var client2 = createClient(loggerFactory2, 10901, 10902);

        assertEquals("t", client1.tables().tables().get(0).name());
        assertEquals("t", client2.tables().tables().get(0).name());

        server.close();

        FakeIgnite ignite2 = new FakeIgnite();
        ((FakeIgniteTables) ignite2.tables()).createTable("t2");

        server2 = startServer(ignite2, 10902);

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

        server = startServer(ignite, null);
        server2 = startServer(ignite, null);

        var loggerFactory = new TestLoggerFactory("c");

        try (var client = IgniteClient.builder()
                .addresses("127.0.0.1:" + server.port(), "127.0.0.1:" + server2.port(), "127.0.0.1:43210")
                .loggerFactory(loggerFactory)
                .build()) {
            client.tables().tables();
            client.tables().table("t");

            loggerFactory.waitForLogContains("Connection established", 5000);
            loggerFactory.waitForLogContains("c:Sending request [opCode=3, remoteAddress=127.0.0.1", 5000);
            loggerFactory.waitForLogMatches(".*c:Failed to establish connection to 127\\.0\\.0\\.1(/<unresolved>)?:43210.*", 5000);
        }
    }

    private static TestServer startServer(FakeIgnite ignite, @Nullable Integer port) {
        return new TestServer(0, ignite, null, null, null, AbstractClientTest.clusterId, null, port);
    }

    private static IgniteClient createClient(LoggerFactory loggerFactory, int... ports) {
        var addrs = new ArrayList<String>();

        for (int port : ports) {
            addrs.add("127.0.0.1:" + port);
        }

        return IgniteClient.builder()
                .addresses(addrs.toArray(new String[0]))
                .loggerFactory(loggerFactory)
                .build();
    }
}
