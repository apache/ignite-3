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

import static org.apache.ignite.internal.hlc.HybridTimestamp.LOGICAL_TIME_BITS_SIZE;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.tx.ClientLazyTransaction;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that observable timestamp (causality token) is propagated from server to client and back.
 */
public class ObservableTimestampPropagationTest extends BaseIgniteAbstractTest {
    private static TestServer testServer;

    private static FakeIgnite ignite;

    private static IgniteClient client;

    private static final AtomicLong currentServerTimestamp = new AtomicLong(1);

    @BeforeAll
    public static void startServer2() {
        ignite = new FakeIgnite("server-2", new TestHybridClock(currentServerTimestamp::get));
        testServer = new TestServer(0, ignite, null, null, "server-2", UUID.randomUUID(), null, null, true, null);

        client = IgniteClient.builder().addresses("127.0.0.1:" + testServer.port()).build();
    }

    @AfterAll
    public static void stopServer2() throws Exception {
        closeAll(client, testServer);
    }

    @Test
    public void testClientPropagatesLatestKnownHybridTimestamp() {
        ReliableChannel ch = ((TcpIgniteClient) client).channel();
        TransactionOptions roOpts = new TransactionOptions().readOnly(true);

        // +1 because logical time is incremented on every call to nowLong - on client handler start.
        assertEquals(
                (currentServerTimestamp.get() << LOGICAL_TIME_BITS_SIZE) + 1,
                ch.observableTimestamp().get().longValue(),
                "Handshake should initialize observable timestamp");

        // RW TX does not propagate timestamp.
        var rwTx = client.transactions().begin();
        ClientLazyTransaction.ensureStarted(rwTx, ch).get1().join();

        // RO TX propagates timestamp.
        var roTx = client.transactions().begin(roOpts);
        ClientLazyTransaction.ensureStarted(roTx, ch).get1().join();
        assertEquals(1, lastObservableTimestamp(ch));

        // Increase timestamp on server - client does not know about it initially.
        currentServerTimestamp.set(11);
        ClientLazyTransaction.ensureStarted(client.transactions().begin(roOpts), ch).get1().join();
        assertEquals(1, lastObservableTimestamp(ch));

        // Subsequent RO TX propagates latest known timestamp.
        client.tables().tables();
        ClientLazyTransaction.ensureStarted(client.transactions().begin(roOpts), ch).get1().join();
        assertEquals(11, lastObservableTimestamp(ch));

        // Smaller timestamp from server is ignored by client.
        currentServerTimestamp.set(9);
        ClientLazyTransaction.ensureStarted(client.transactions().begin(roOpts), ch).get1().join();
        ClientLazyTransaction.ensureStarted(client.transactions().begin(roOpts), ch).get1().join();
        assertEquals(11, lastObservableTimestamp(ch));
    }

    private static Long lastObservableTimestamp(ReliableChannel ch) {
        return ch.observableTimestamp().get().longValue() >> LOGICAL_TIME_BITS_SIZE;
    }
}
