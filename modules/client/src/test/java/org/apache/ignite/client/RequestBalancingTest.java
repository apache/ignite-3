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

import static org.apache.ignite.client.AbstractClientTest.getClient;
import static org.apache.ignite.client.AbstractClientTest.getClusterNodes;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests client request balancing.
 */
public class RequestBalancingTest extends BaseIgniteAbstractTest {
    /** Test server 1. */
    private TestServer server1;

    /** Test server 2. */
    private TestServer server2;

    /** Test server 3. */
    private TestServer server3;

    @BeforeEach
    void setUp() {
        FakeIgnite ignite = new FakeIgnite();
        server1 = new TestServer(0, ignite, null, null, "s1", AbstractClientTest.clusterId, null, 10991);
        server2 = new TestServer(0, ignite, null, null, "s2", AbstractClientTest.clusterId, null, 10992);
        server3 = new TestServer(0, ignite, null, null, "s3", AbstractClientTest.clusterId, null, 10993);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(server1, server2);
    }

    @Test
    public void testRequestsAreRoundRobinBalanced() throws Exception {
        try (var client = getClient(server1, server2, server3)) {
            assertTrue(IgniteTestUtils.waitForCondition(() -> client.connections().size() == 3, 3000));

            // Execute on unknown node to fall back to balancing.
            List<Object> res = IntStream.range(0, 5)
                    .mapToObj(i -> client.compute().execute(getClusterNodes("s123"), JobDescriptor.builder("job").build(), null))
                    .collect(Collectors.toList());

            assertEquals(5, res.size());
            assertEquals("s2", res.get(0));
            assertEquals("s1", res.get(1));
            assertEquals("s3", res.get(2));
            assertEquals("s2", res.get(3));
            assertEquals("s1", res.get(4));
        }
    }
}
