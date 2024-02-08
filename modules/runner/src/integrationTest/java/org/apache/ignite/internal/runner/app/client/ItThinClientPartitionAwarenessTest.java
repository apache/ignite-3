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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.runner.app.client.proxy.IgniteClientProxy;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Thin client partition awareness test with real cluster.
 */
public class ItThinClientPartitionAwarenessTest extends ItAbstractThinClientTest {
    List<IgniteClientProxy> proxies = new ArrayList<>();

    IgniteClient proxyClient;

    @BeforeAll
    void createProxies() throws Exception {
        List<String> addrs = new ArrayList<>();
        for (int port : getClientPorts()) {
            var proxy = IgniteClientProxy.start(port, port + 1000);
            addrs.add("127.0.0.1:" + proxy.listenPort());
            proxies.add(proxy);
        }

        proxyClient = IgniteClient.builder().addresses(addrs.toArray(new String[0])).build();
    }

    @AfterAll
    void stopProxies() throws Exception {
        proxyClient.close();

        for (var proxy : proxies) {
            proxy.close();
        }
    }

    @BeforeEach
    void resetRequestCount() {
        for (IgniteClientProxy proxy : proxies) {
            proxy.resetRequestCount();
        }
    }

    @Test
    void testGetRequestIsRoutedToPrimaryNode() {
        // Warm up.
        RecordView<Tuple> view = proxyClient.tables().table(TABLE_NAME).recordView();
        view.get(null, Tuple.create().set("key", 1));

        for (int key = 0; key < 50; key++) {
            // Get actual primary node using compute.
            Tuple keyTuple = Tuple.create().set("key", key);
            var primaryNodeName = proxyClient.compute()
                    .executeColocated(TABLE_NAME, keyTuple, List.of(), NodeNameJob.class.getName());

            // Perform request and check routing with proxy.
            resetRequestCount();
            view.get(null, keyTuple);
            String requestNodeName = getLastRequestNodeName();

            assertEquals(primaryNodeName, requestNodeName, "Key: " + key);
        }
    }

    private @Nullable String getLastRequestNodeName() {
        for (int i = 0; i < proxies.size(); i++) {
            IgniteClientProxy proxy = proxies.get(i);

            if (proxy.requestCount() > 0) {
                //noinspection resource
                return server(i).name();
            }
        }

        return null;
    }

    private static class NodeNameJob implements ComputeJob<String> {
        @Override
        public String execute(JobExecutionContext context, Object... args) {
            return context.ignite().name() + Arrays.stream(args).map(Object::toString).collect(Collectors.joining("_"));
        }
    }
}
