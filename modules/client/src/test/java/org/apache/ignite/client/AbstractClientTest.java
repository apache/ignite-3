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
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.util.ResourceLeakDetector;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base class for client tests.
 */
public abstract class AbstractClientTest {
    protected static final String DEFAULT_TABLE = "default_test_table";

    protected static TestServer testServer;

    protected static Ignite server;

    protected static IgniteClient client;

    protected static int serverPort;

    /**
     * Before all.
     */
    @BeforeAll
    public static void beforeAll() {
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);

        server = new FakeIgnite("server-1");

        testServer = startServer(10800, 10, 0, server);

        serverPort = testServer.port();

        client = startClient();
    }

    /**
     * After all.
     */
    @AfterAll
    public static void afterAll() throws Exception {
        client.close();
        testServer.close();
    }

    /**
     * After each.
     */
    @BeforeEach
    public void beforeEach() {
        dropTables(server);
    }

    protected void dropTables(Ignite ignite) {
        for (var t : ignite.tables().tables()) {
            ignite.tables().dropTable(t.name());
        }
    }

    /**
     * Returns client.
     *
     * @param addrs Addresses.
     * @return Client.
     */
    public static IgniteClient startClient(String... addrs) {
        if (addrs == null || addrs.length == 0) {
            addrs = new String[]{"127.0.0.1:" + serverPort};
        }

        var builder = IgniteClient.builder().addresses(addrs);

        return builder.build();
    }

    /**
     * Returns server.
     *
     * @param port Port.
     * @param portRange Port range.
     * @param idleTimeout Idle timeout.
     * @param ignite Ignite.
     * @return Server.
     */
    public static TestServer startServer(
            int port,
            int portRange,
            long idleTimeout,
            Ignite ignite
    ) {
        return startServer(port, portRange, idleTimeout, ignite, null);
    }

    /**
     * Returns server.
     *
     * @param port Port.
     * @param portRange Port range.
     * @param idleTimeout Idle timeout.
     * @param ignite Ignite.
     * @param nodeName Node name.
     * @return Server.
     */
    public static TestServer startServer(
            int port,
            int portRange,
            long idleTimeout,
            Ignite ignite,
            String nodeName
    ) {
        return new TestServer(port, portRange, idleTimeout, ignite, null, nodeName);
    }

    /**
     * Assertion of {@link Tuple} equality.
     *
     * @param x Tuple.
     * @param y Tuple.
     */
    public static void assertTupleEquals(Tuple x, Tuple y) {
        if (x == null) {
            assertNull(y);
            return;
        }

        if (y == null) {
            //noinspection ConstantConditions
            assertNull(x);
            return;
        }

        assertEquals(x.columnCount(), y.columnCount(), x + " != " + y);

        for (var i = 0; i < x.columnCount(); i++) {
            assertEquals(x.columnName(i), y.columnName(i));
            assertEquals((Object) x.value(i), y.value(i));
        }
    }
}
