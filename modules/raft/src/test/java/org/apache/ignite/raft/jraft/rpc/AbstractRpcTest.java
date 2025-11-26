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
package org.apache.ignite.raft.jraft.rpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.messages.TestRaftMessagesFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 */
public abstract class AbstractRpcTest {
    protected PeerId peerId;

    private RpcServer<?> server;

    private final List<RpcClient> clients = new ArrayList<>();

    private final TestRaftMessagesFactory msgFactory = new TestRaftMessagesFactory();

    @BeforeEach
    public void setup() {
        server = createServer();

        server.registerProcessor(new Request1RpcProcessor());
        server.registerProcessor(new Request2RpcProcessor());
        server.init(null);

        peerId = new PeerId(server.consistentId());
    }

    @AfterEach
    public void tearDown() {
        clients.forEach(RpcClient::shutdown);

        server.shutdown();
    }

    /**
     * @return The server.
     */
    public abstract RpcServer<?> createServer();

    /**
     * @return The client.
     */
    private RpcClient createClient() {
        RpcClient client = createClient0();

        client.init(null);

        clients.add(client);

        return client;
    }

    public abstract RpcClient createClient0();

    @Test
    public void testConnection() {
        RpcClient client = createClient();

        assertTrue(client.checkConnection(peerId));
    }

    @Test
    public void testAsyncProcessing() throws Exception {
        RpcClient client = createClient();

        CountDownLatch l1 = new CountDownLatch(1);
        AtomicReference<TestMessages.Response1> resp1 = new AtomicReference<>();
        client.invokeAsync(peerId, msgFactory.request1().build(), new InvokeContext(), (InvokeCallbackInCommonPool) (result, err) -> {
            resp1.set((TestMessages.Response1) result);
            l1.countDown();
        }, 5000);
        l1.await(5_000, TimeUnit.MILLISECONDS);
        assertNotNull(resp1);

        CountDownLatch l2 = new CountDownLatch(1);
        AtomicReference<TestMessages.Response2> resp2 = new AtomicReference<>();
        client.invokeAsync(peerId, msgFactory.request2().build(), new InvokeContext(), (InvokeCallbackInCommonPool) (result, err) -> {
            resp2.set((TestMessages.Response2) result);
            l2.countDown();
        }, 5000);
        l2.await(5_000, TimeUnit.MILLISECONDS);
        assertNotNull(resp2);
    }

    @Test
    public void testDisconnect() {
        RpcClient client1 = createClient();
        RpcClient client2 = createClient();

        assertTrue(client1.checkConnection(peerId));
        assertTrue(client2.checkConnection(peerId));

        server.shutdown();

        assertTrue(waitForTopology(client1, 2, 5_000));
        assertTrue(waitForTopology(client2, 2, 5_000));

        assertFalse(client1.checkConnection(peerId));
        assertFalse(client2.checkConnection(peerId));
    }

    @Test
    public void testRecordedAsync() throws Exception {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(peerId));

        CountDownLatch l = new CountDownLatch(2);

        client1.invokeAsync(peerId, msgFactory.request1().build(), null, (InvokeCallbackInCommonPool) (result, err) -> l.countDown(), 500);
        client1.invokeAsync(peerId, msgFactory.request2().build(), null, (InvokeCallbackInCommonPool) (result, err) -> l.countDown(), 500);

        l.await();

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(4, recorded.size());
    }

    @Test
    public void testRecordedAsyncTimeout() {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(peerId));

        try {
            TestMessages.Request1 request = msgFactory.request1().val(10_000).build();

            CompletableFuture<Object> fut = new CompletableFuture<>();

            client1.invokeAsync(peerId, request, null, (InvokeCallbackInCommonPool) (result, err) -> {
                if (err == null)
                    fut.complete(result);
                else
                    fut.completeExceptionally(err);
            }, 500);

            fut.get(); // Should throw timeout exception.

            fail();
        }
        catch (Exception ignored) {
            // Expected.
        }

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(1, recorded.size());
        assertTrue(recorded.poll()[0] instanceof TestMessages.Request1);
    }

    @Test
    public void testBlockedAsync() throws Exception {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.blockMessages((msg, id) -> msg instanceof TestMessages.Request1);

        assertTrue(client1.checkConnection(peerId));

        CompletableFuture<Object> resp = new CompletableFuture<>();

        client1.invokeAsync(peerId, msgFactory.request1().build(), null,
                (InvokeCallbackInCommonPool) (result, err) -> resp.complete(result), 30_000);

        Thread.sleep(500);

        Queue<Object[]> msgs = client1.blockedMessages();

        assertEquals(1, msgs.size());

        assertFalse(resp.isDone());

        client1.stopBlock();

        resp.get(5_000, TimeUnit.MILLISECONDS);
    }

    /** */
    private class Request1RpcProcessor implements RpcProcessor<TestMessages.Request1> {
        /** {@inheritDoc} */
        @Override public void handleRequest(RpcContext rpcCtx, TestMessages.Request1 request) {
            if (request.val() == 10_000)
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

            TestMessages.Response1 resp1 = msgFactory.response1().val(request.val() + 1).build();
            rpcCtx.sendResponse(resp1);
        }

        /** {@inheritDoc} */
        @Override public String interest() {
            return TestMessages.Request1.class.getName();
        }
    }

    /** */
    private class Request2RpcProcessor implements RpcProcessor<TestMessages.Request2> {
        /** {@inheritDoc} */
        @Override public void handleRequest(RpcContext rpcCtx, TestMessages.Request2 request) {
            TestMessages.Response2 resp2 = msgFactory.response2().val(request.val() + 1).build();
            rpcCtx.sendResponse(resp2);
        }

        /** {@inheritDoc} */
        @Override public String interest() {
            return TestMessages.Request2.class.getName();
        }
    }

    /**
     * The callback is used a common pool to execute.
     */
    private interface InvokeCallbackInCommonPool extends InvokeCallback {
        @Override
        default Executor executor() {
            return ForkJoinPool.commonPool();
        }
    }

    /**
     * @param client The client.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     */
    protected abstract boolean waitForTopology(RpcClient client, int expected, long timeout);
}
