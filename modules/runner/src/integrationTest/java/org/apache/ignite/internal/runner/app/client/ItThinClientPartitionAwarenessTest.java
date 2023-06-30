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

package org.apache.ignite.internal.runner.app.client;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.runner.app.client.proxy.IgniteClientProxy;
import org.junit.jupiter.api.Test;

/**
 * Thin client partition awareness test with real cluster.
 */
public class ItThinClientPartitionAwarenessTest extends ItAbstractThinClientTest {
    @Test
    void testClusterNodes() throws Exception {
        List<String> adds = new ArrayList<>();
        List<IgniteClientProxy> proxies = new ArrayList<>();
        for (int port : getClientPorts()) {
            var proxy = IgniteClientProxy.start(port, port + 1000);
            adds.add("127.0.0.1:" + proxy.listenPort());
        }

        var client = IgniteClient.builder().addresses(adds.get(0)).build();

        var nodes = client.clusterNodes();
    }
}
