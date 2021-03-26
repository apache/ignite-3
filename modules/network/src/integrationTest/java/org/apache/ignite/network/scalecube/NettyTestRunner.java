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

package org.apache.ignite.network.scalecube;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.ignite.network.TestMessage;
import org.apache.ignite.network.TestMessageSerializationFactory;
import org.apache.ignite.network.internal.netty.NettyClient;
import org.apache.ignite.network.internal.netty.NettySender;
import org.apache.ignite.network.internal.netty.NettyServer;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.message.NetworkMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Deprecated
// Only for WIP purposes
public class NettyTestRunner {

    @Test
    public void test() throws ExecutionException, InterruptedException {
        var ref = new Object() {
            NetworkMessage res = null;
        };

        CountDownLatch latch = new CountDownLatch(1);

        int port = 1234;

        BiConsumer<InetSocketAddress, NetworkMessage> listener = (addr, msg) -> {
            ref.res = msg;
            latch.countDown();
        };

        TestMessageSerializationFactory tProv = new TestMessageSerializationFactory();

        final MessageSerializationRegistry registry = new MessageSerializationRegistry();
        registry.registerFactory(TestMessage.TYPE, tProv);

        final NettyServer server = new NettyServer(port, channel -> {}, listener, registry);
        server.start().get();

        final NettyClient client = new NettyClient("localhost", port, registry, listener);

        StringBuilder message = new StringBuilder("");

        for (int i = 0; i < 950; i++)
            message.append("f");

        Map<Integer, String> someMap = new HashMap<>();

        for (int i = 0; i < 26; i++)
            someMap.put(i, "" + (char) ('a' + i));

        NettySender sender = client.start().get();

        TestMessage msg = new TestMessage(message.toString(), someMap);
        sender.send(msg);
        latch.await(3, TimeUnit.SECONDS);
        assertEquals(msg, ref.res);
    }
}
