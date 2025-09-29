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

import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_DELAYED_ACKS;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_DIRECT_MAPPING;
import static org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature.TX_PIGGYBACK;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.apache.ignite.internal.client.proto.ProtocolBitmaskFeature;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests that after handshake client and servers mutual features are in expected state.
 */
public class FeatureCompatibilityTest extends BaseIgniteAbstractTest {
    private TestServer testServer;

    private FakeIgnite ignite;

    private IgniteClient client;

    private void startServer(@Nullable BitSet features) {
        ignite = new FakeIgnite("server-1", new TestHybridClock(System::currentTimeMillis));
        testServer = new TestServer(0, ignite, reqId -> false, null, "server-1", UUID.randomUUID(), null, null, true, features);

        client = IgniteClient.builder().addresses("127.0.0.1:" + testServer.port()).build();
    }

    private void stopServer() throws Exception {
        closeAll(client, testServer);
    }

    @Test
    public void testDirectMappingEnabled() throws Exception {
        startServer(null);

        try {
            ReliableChannel ch = ((TcpIgniteClient) client).channel();

            ClientChannel ch0 = ch.getChannelAsync(null).join();

            assertTrue(ch0.protocolContext().allFeaturesSupported(TX_DIRECT_MAPPING, TX_DELAYED_ACKS, TX_PIGGYBACK));
        } finally {
            stopServer();
        }
    }

    @Test
    public void testDirectMappingDisabled() throws Exception {
        BitSet features = new BitSet(ProtocolBitmaskFeature.values().length);
        features.set(TX_DIRECT_MAPPING.featureId());
        startServer(features);

        try {
            ReliableChannel ch = ((TcpIgniteClient) client).channel();

            ClientChannel ch0 = ch.getChannelAsync(null).join();

            assertFalse(ch0.protocolContext().isFeatureSupported(TX_DIRECT_MAPPING));
            assertFalse(ch0.protocolContext().isFeatureSupported(TX_DELAYED_ACKS));
            assertFalse(ch0.protocolContext().isFeatureSupported(TX_PIGGYBACK));
        } finally {
            stopServer();
        }
    }
}
