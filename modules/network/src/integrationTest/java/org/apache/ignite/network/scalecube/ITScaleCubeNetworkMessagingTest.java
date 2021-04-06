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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkContext;
import org.apache.ignite.network.NetworkFactory;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.network.message.MessageMapperProviders;
import org.apache.ignite.network.message.NetworkMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
class ITScaleCubeNetworkMessagingTest {
    /** */
    private static final MessageMapperProviders TEST_MESSAGE_MAPPER_PROVIDERS =
        new MessageMapperProviders()
            .registerProvider(TestMessage.TYPE, new TestMessageMapperProvider())
            .registerProvider(TestRequest.TYPE, new TestRequestMapperProvider())
            .registerProvider(TestResponse.TYPE, new TestResponseMapperProvider());

    /** */
    private static final NetworkFactory NETWORK_FACTORY = new ScaleCubeNetworkFactory();

    /** */
    private final Map<String, NetworkMessage> messageStorage = new ConcurrentHashMap<>();

    /** */
    private final List<Network> startedMembers = new ArrayList<>();

    /** */
    @AfterEach
    public void afterEach() {
        startedMembers.forEach(Network::shutdown);
    }

    /**
     * Test sending and receiving messages.
     */
    @Test
    public void messageWasSentToAllMembersSuccessfully() throws Exception {
        //Given: Three started member which are gathered to cluster.
        List<String> addresses = List.of("localhost:3344", "localhost:3345", "localhost:3346");

        CountDownLatch latch = new CountDownLatch(3);

        Network alice = startNetwork("Alice", 3344, addresses);
        Network bob = startNetwork("Bob", 3345, addresses);
        Network carol = startNetwork("Carol", 3346, addresses);

        NetworkMessageHandler messageWaiter = (message, sender, correlationId) -> latch.countDown();

        alice.getMessagingService().addMessageHandler(messageWaiter);
        bob.getMessagingService().addMessageHandler(messageWaiter);
        carol.getMessagingService().addMessageHandler(messageWaiter);

        TestMessage testMessage = new TestMessage("Message from Alice");

        //When: Send one message to all members in cluster.
        for (NetworkMember member : alice.getTopologyService().allMembers()) {
            System.out.println("SEND : " + member);

            alice.getMessagingService().weakSend(member, testMessage);
        }

        boolean done = latch.await(3, TimeUnit.SECONDS);
        assertTrue(done);

        //Then: All members successfully received message.
        assertThat(getLastMessage(alice), is(testMessage));
        assertThat(getLastMessage(bob), is(testMessage));
        assertThat(getLastMessage(carol), is(testMessage));
    }

    /** */
    private NetworkMessage getLastMessage(Network network) {
        return messageStorage.get(network.getContext().getName());
    }

    /** */
    private Network startNetwork(String name, int port, List<String> addresses) {
        var context = new NetworkContext(name, port, addresses, TEST_MESSAGE_MAPPER_PROVIDERS);

        Network network = NETWORK_FACTORY.createNetwork(context);
        System.out.println("-----" + name + " started");

        network.getMessagingService().addMessageHandler((message, sender, correlationId) -> {
            messageStorage.put(name, message);

            System.out.println(name + " handled messages : " + message);
        });

        network.getTopologyService().addEventHandler(new TopologyEventHandler() {
            @Override public void onAppeared(NetworkMember member) {
                System.out.println(name + " found member : " + member);
            }

            @Override public void onDisappeared(NetworkMember member) {
                System.out.println(name + " lost member : " + member);
            }
        });

        network.start();

        startedMembers.add(network);

        return network;
    }
}
