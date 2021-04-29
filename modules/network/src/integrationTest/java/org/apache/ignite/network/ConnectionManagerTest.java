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

package org.apache.ignite.network;

import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.network.internal.netty.ConnectionManager;
import org.apache.ignite.network.internal.netty.NettySender;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link ConnectionManager}.
 */
public class ConnectionManagerTest {
    /** Started connection managers. */
    private final List<ConnectionManager> startedManagers = new ArrayList<>();

    /** */
    @AfterEach
    void tearDown() {
        startedManagers.forEach(ConnectionManager::stop);

        startedManagers.clear();
    }

    /**
     * Tests that a message is sent successfuly using ConnectionManager.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSentSuccessfully() throws Exception {
        String msgText = "test";

        var latch = new CountDownLatch(1);

        int port1 = 4000;
        int port2 = 4001;

        var manager1 = startManager(port1);
        var manager2 = startManager(port2);

        manager2.addListener((address, message) -> {
            if (message instanceof TestMessage && msgText.equals(((TestMessage) message).msg()))
                latch.countDown();
        });

        NettySender sender = manager1.channel(address(port2)).get();

        TestMessage testMessage = new TestMessage(msgText, new HashMap<>());

        sender.send(testMessage).join();

        latch.await(3, TimeUnit.SECONDS);
    }

    /**
     * Tests that after a channel was closed, a new channel is opened upon a request.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testCanReconnectAfterFail() throws Exception {
        String msgText = "test";

        var latch = new CountDownLatch(1);

        int port1 = 4000;
        int port2 = 4001;

        var manager1 = startManager(port1);
        var manager2 = startManager(port2);

        NettySender sender = manager1.channel(address(port2)).get();

        TestMessage testMessage = new TestMessage(msgText, new HashMap<>());

        manager2.stop();

        final var finalSender = sender;

        assertThrows(ClosedChannelException.class, () -> {
            try {
                finalSender.send(testMessage).join();
            }
            catch (CompletionException e) {
                throw e.getCause();
            }
        });

        manager2 = startManager(port2);

        manager2.addListener((address, message) -> {
            if (message instanceof TestMessage && msgText.equals(((TestMessage) message).msg()))
                latch.countDown();
        });

        sender = manager1.channel(address(port2)).get();

        sender.send(testMessage).join();
    }

    /**
     * Create an unresolved {@link InetSocketAddress} with "localhost" as a host.
     *
     * @param port Port.
     * @return Address.
     */
    private InetSocketAddress address(int port) {
        return InetSocketAddress.createUnresolved("localhost", port);
    }

    /**
     * Create and start a {@link ConnectionManager} adding it to the {@link #startedManagers} list.
     *
     * @param port Port for the {@link ConnectionManager#server}.
     * @return Connection manager.
     */
    private ConnectionManager startManager(int port) {
        var registry = new MessageSerializationRegistry()
            .registerFactory(TestMessage.TYPE, new TestMessageSerializationFactory());

        var manager = new ConnectionManager(port, registry);

        manager.start();

        startedManagers.add(manager);

        return manager;
    }
}
