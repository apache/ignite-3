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

import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests client request balancing.
 */
public class RequestBalancingTest {
    /** Test server 1. */
    TestServer server1;

    /** Test server 2. */
    TestServer server2;

    /** Test server 3. */
    TestServer server3;

    @BeforeEach
    void setUp() throws Exception {
        FakeIgnite ignite = new FakeIgnite();
        server1 = AbstractClientTest.startServer(10900, 10, 0, ignite);
        server2 = AbstractClientTest.startServer(10900, 10, 0, ignite);
        server3 = AbstractClientTest.startServer(10900, 10, 0, ignite);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server1, server2);
    }

    @Test
    public void testRequestsAreRoundRobinBalanced() {

    }
}
