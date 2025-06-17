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

import static org.apache.ignite.client.AbstractClientTest.getClusterNodes;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.client.fakes.FakeCompute;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that observable timestamp (causality token) is propagated from server to client from compute jobs and streamer receiver.
 */
@SuppressWarnings({"DataFlowIssue", "AssignmentToStaticFieldFromInstanceMethod"})
public class ObservableTimestampComputePropagationTest extends BaseIgniteAbstractTest {
    private static TestServer testServer;

    private static final AtomicLong serverTimestamp = new AtomicLong(1);

    @BeforeAll
    public static void startServers() {
        TestHybridClock clock1 = new TestHybridClock(serverTimestamp::get);
        var ignite1 = new FakeIgnite("server-1");
        testServer = new TestServer(0, ignite1, null, null, "server-1", UUID.randomUUID(), null, null, clock1, true, null);
    }

    @AfterAll
    public static void stopServers() throws Exception {
        closeAll(testServer);
    }

    @AfterEach
    public void resetFakeCompute() {
        FakeCompute.observableTimestamp = HybridTimestamp.MIN_VALUE;
    }

    @Test
    public void testComputeJobPropagatesTimestampFromTargetNode() {
        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.1:" + testServer.port())
                .build()) {
            ReliableChannel ch = ((TcpIgniteClient) client).channel();
            assertEquals(1, ch.observableTimestamp().get().getPhysical());

            JobTarget target = getClusterNodes("server-2");
            JobDescriptor<Object, String> job = JobDescriptor.<Object, String>builder("job").build();

            FakeCompute.observableTimestamp = new HybridTimestamp(123, 456);

            String res = client.compute().execute(target, job, null);
            assertEquals("server-1", res);

            assertEquals(FakeCompute.observableTimestamp, ch.observableTimestamp().get());
        }
    }
}
