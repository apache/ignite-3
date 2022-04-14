/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Compute tests.
 */
public class ClientComputeTest {
    private TestServer server1;
    private TestServer server2;
    private TestServer server3;

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(server1, server2, server3);
    }

    @Test
    public void testClientSendsComputeJobToTargetNodeWhenDirectConnectionExists() throws Exception {
        initServers(reqId -> false);

        try (var client = getClient()) {
            IgniteTestUtils.waitForCondition(() -> client.connections().size() == 3, 3000);

            String res1 = client.compute().<String>execute(getClusterNodes("s1"), "job").join();
            String res2 = client.compute().<String>execute(getClusterNodes("s2"), "job").join();
            String res3 = client.compute().<String>execute(getClusterNodes("s3"), "job").join();

            assertEquals("s1", res1);
            assertEquals("s2", res2);
            assertEquals("s3", res3);
        }
    }

    @Test
    public void testClientSendsComputeJobToDefaultNodeWhenDirectConnectionToTargetDoesNotExist() {

    }

    @Test
    public void testClientRetriesComputeJobOnPrimaryAndDefaultNodes() {

    }

    private IgniteClient getClient() {
        return IgniteClient.builder()
                .addresses("127.0.0.1:" + server1.port(), "127.0.0.1:" + server2.port(), "127.0.0.1:" + server3.port())
                .reconnectThrottlingPeriod(0)
                .build();
    }

    private void initServers(Function<Integer, Boolean> shouldDropConnection) {
        server1 = new TestServer(10900, 10, 0, new FakeIgnite(), shouldDropConnection, "s1");
        server2 = new TestServer(10910, 10, 0, new FakeIgnite(), shouldDropConnection, "s2");
        server3 = new TestServer(10920, 10, 0, new FakeIgnite(), shouldDropConnection, "s3");
    }

    private Set<ClusterNode> getClusterNodes(String... names) {
        return Arrays.stream(names)
                .map(s -> new ClusterNode("id", s, new NetworkAddress("127.0.0.1", 8080)))
                .collect(Collectors.toSet());
    }
}
