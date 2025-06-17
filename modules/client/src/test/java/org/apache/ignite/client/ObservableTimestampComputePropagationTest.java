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
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that observable timestamp (causality token) is propagated from server to client from compute jobs and streamer receiver.
 */
@SuppressWarnings("DataFlowIssue")
public class ObservableTimestampComputePropagationTest extends BaseIgniteAbstractTest {
    private static TestServer testServer1;

    private static TestServer testServer2;

    private static IgniteClient client;

    private static final AtomicLong serverTimestamp1 = new AtomicLong(1);
    private static final AtomicLong serverTimestamp2 = new AtomicLong(1);

    @BeforeAll
    public static void startServer2() {
        TestHybridClock clock1 = new TestHybridClock(serverTimestamp1::get);
        var ignite1 = new FakeIgnite("server-1");
        testServer1 = new TestServer(0, ignite1, null, null, "server-1", UUID.randomUUID(), null, null, clock1, true, null);

        TestHybridClock clock2 = new TestHybridClock(serverTimestamp2::get);
        var ignite2 = new FakeIgnite("server-2");
        testServer2 = new TestServer(0, ignite2, null, null, "server-2", UUID.randomUUID(), null, null, clock2, true, null);

        client = IgniteClient.builder()
                .addresses("127.0.0.1:" + testServer1.port(), "127.0.0.2:" + testServer1.port())
                .build();
    }

    @AfterAll
    public static void stopServer2() throws Exception {
        closeAll(client, testServer1, testServer2);
    }

    @Test
    public void testComputeJobPropagatesTimestampFromTargetNode() {
        // TODO: Connect to node1, send a compute job to node2, and check that the observable timestamp is propagated.
        ReliableChannel ch = ((TcpIgniteClient) client).channel();
        assertEquals(1, lastObservableTimestamp(ch));
    }

    private static Long lastObservableTimestamp(ReliableChannel ch) {
        return ch.observableTimestamp().get().longValue() >> LOGICAL_TIME_BITS_SIZE;
    }
}
