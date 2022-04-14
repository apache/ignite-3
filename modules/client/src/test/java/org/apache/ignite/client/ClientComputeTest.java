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

import java.util.function.Function;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.util.IgniteUtils;
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
    public void testClientSendsComputeJobToTargetNodeWhenDirectConnectionExists() {
        // TODO: Multiple servers.
        initServers(reqId -> false);


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
        server1 = new TestServer(10900, 10, 0, new FakeIgnite(), shouldDropConnection);
        server2 = new TestServer(10910, 10, 0, new FakeIgnite(), shouldDropConnection);
        server3 = new TestServer(10920, 10, 0, new FakeIgnite(), shouldDropConnection);
    }
}
