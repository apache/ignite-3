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

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that observable timestamp (causality token) is propagated from server to client and back.
 */
public class ObservableTimestampPropagationTest {
    private static TestServer testServer;

    private static Ignite ignite;

    private static IgniteClient client;

    private static final AtomicLong observableTimestamp = new AtomicLong(1);

    @BeforeAll
    public static void startServer2() {
        TestHybridClock clock = new TestHybridClock(observableTimestamp::get);

        ignite = new FakeIgnite("server-2");
        testServer = new TestServer(0, ignite, unused -> false, null, "server-2", UUID.randomUUID(), null, null, clock);

        client = IgniteClient.builder().addresses("127.0.0.1:" + testServer.port()).build();
    }

    @AfterAll
    public static void stopServer2() throws Exception {
        IgniteUtils.closeAll(client, testServer, ignite);
    }

    @Test
    public void testClientPropagatesLatestKnownHybridTimestamp() {
        // Perform request.
        client.tables().tables();

        // Verify current timestamp is propagated by performing TX_BEGIN and checking ts value received by server.
        client.transactions().begin();
    }
}
