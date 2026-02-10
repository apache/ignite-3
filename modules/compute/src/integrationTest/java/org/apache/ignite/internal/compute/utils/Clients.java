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

package org.apache.ignite.internal.compute.utils;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.IgniteCompute;

/**
 * Utility class for keeping the list of thin clients in tests.
 */
public class Clients {
    private final Map<String, IgniteClient> clients = new HashMap<>();

    /**
     * Gets thin client referenced by the embedded node.
     *
     * @param node Node to get the client for.
     * @return Thin client.
     */
    public Ignite client(Ignite node) {
        String address = "127.0.0.1:" + unwrapIgniteImpl(node).clientAddress().port();
        return clients.computeIfAbsent(address, addr -> IgniteClient.builder().addresses(addr).build());
    }

    /**
     * Gets compute API for the thin client referenced by the embedded node.
     *
     * @param node Node to get the client for.
     * @return Compute API.
     */
    public IgniteCompute compute(Ignite node) {
        return client(node).compute();
    }

    /**
     * Cleans clients list.
     */
    public void cleanup() {
        for (IgniteClient igniteClient : clients.values()) {
            igniteClient.close();
        }
        clients.clear();
    }
}
