package org.apache.ignite.raft.jraft.rpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.util.Endpoint;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * TODO add test for localconn.close, timeouts.
 */
public abstract class AbstractRpcTest {
    private Endpoint endpoint;
    private RpcServer server;

    @Before
    public void setup() {
        endpoint = new Endpoint("localhost", 1000);
        server = createServer(endpoint);
        server.registerProcessor(new Request1RpcProcessor());
        server.registerProcessor(new Request2RpcProcessor());
        server.init(null);
    }

    public abstract RpcServer createServer(Endpoint endpoint);

    public abstract RpcClient createClient();

    @After
    public void teardown() {
        server.shutdown();

        RpcClientEx.onConnCreated[0] = null;
    }

    @Test
    public void testConnection() {
        RpcClient client = createClient();

        assertFalse(client.checkConnection(endpoint));

        assertTrue(client.checkConnection(endpoint, true));
    }

    @Test
    public void testSyncProcessing() throws RemotingException, InterruptedException {
        RpcClient client = createClient();
        Response1 resp1 = (Response1) client.invokeSync(endpoint, new Request1(), new InvokeContext(), 5000);
        assertNotNull(resp1);

        Response2 resp2 = (Response2) client.invokeSync(endpoint, new Request2(), new InvokeContext(), 5000);
        assertNotNull(resp2);
    }

    @Test
    public void testAsyncProcessing() throws RemotingException, InterruptedException {
        RpcClient client = createClient();

        CountDownLatch l1 = new CountDownLatch(1);
        AtomicReference<Response1> resp1 = new AtomicReference<>();
        client.invokeAsync(endpoint, new Request1(), new InvokeContext(), (result, err) -> {
            resp1.set((Response1) result);
            l1.countDown();
        }, 5000);
        l1.await(5_000, TimeUnit.MILLISECONDS);
        assertNotNull(resp1);

        CountDownLatch l2 = new CountDownLatch(1);
        AtomicReference<Response2> resp2 = new AtomicReference<>();
        client.invokeAsync(endpoint, new Request2(), new InvokeContext(), (result, err) -> {
            resp2.set((Response2) result);
            l2.countDown();
        }, 5000);
        l2.await(5_000, TimeUnit.MILLISECONDS);
        assertNotNull(resp2);
    }

    @Test
    public void testDisconnect1() {
        RpcClient client1 = createClient();
        RpcClient client2 = createClient();

        assertTrue(client1.checkConnection(endpoint, true));
        assertTrue(client2.checkConnection(endpoint, true));

        client1.shutdown();

        assertFalse(client1.checkConnection(endpoint));
        assertTrue(client2.checkConnection(endpoint));

        client2.shutdown();

        assertFalse(client1.checkConnection(endpoint));
        assertFalse(client2.checkConnection(endpoint));
    }

    @Test
    public void testDisconnect2() {
        RpcClient client1 = createClient();
        RpcClient client2 = createClient();

        assertTrue(client1.checkConnection(endpoint, true));
        assertTrue(client2.checkConnection(endpoint, true));

        server.shutdown();

        assertFalse(client1.checkConnection(endpoint));
        assertFalse(client2.checkConnection(endpoint));
    }

    @Test
    public void testRecordedSync() throws RemotingException, InterruptedException {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint, true));

        Response1 resp1 = (Response1) client1.invokeSync(endpoint, new Request1(), 500);
        Response2 resp2 = (Response2) client1.invokeSync(endpoint, new Request2(), 500);

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(4, recorded.size());
        assertTrue(recorded.poll()[0] instanceof Request1);
        assertTrue(recorded.poll()[0] instanceof Response1);
        assertTrue(recorded.poll()[0] instanceof Request2);
        assertTrue(recorded.poll()[0] instanceof Response2);
    }

    @Test
    public void testRecordedSyncTimeout() throws RemotingException, InterruptedException {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint, true));

        try {
            Request1 request = new Request1();
            request.val = 10_000;
            Response1 resp1 = (Response1) client1.invokeSync(endpoint, request, 500);

            fail();
        } catch (Exception e) {
            // Expected.
        }

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(1, recorded.size());
        assertTrue(recorded.poll()[0] instanceof Request1);
    }

    @Test
    public void testRecordedAsync() throws RemotingException, InterruptedException {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint, true));

        CountDownLatch l = new CountDownLatch(2);

        client1.invokeAsync(endpoint, new Request1(), null, (result, err) -> l.countDown(), 500);
        client1.invokeAsync(endpoint, new Request2(), null, (result, err) -> l.countDown(), 500);

        l.await();

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(4, recorded.size());
    }

    @Test
    public void testRecordedAsyncTimeout() throws RemotingException, InterruptedException {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.recordMessages((a, b) -> true);

        assertTrue(client1.checkConnection(endpoint, true));

        try {
            Request1 request = new Request1();
            request.val = 10_000;
            CompletableFuture fut = new CompletableFuture<>();

            client1.invokeAsync(endpoint, request, null, (result, err) -> {
                if (err == null)
                    fut.complete(result);
                else
                    fut.completeExceptionally(err);
            }, 500);

            fut.get(); // Should throw timeout exception.

            fail();
        } catch (Exception e) {
            // Expected.
        }

        Queue<Object[]> recorded = client1.recordedMessages();

        assertEquals(1, recorded.size());
        assertTrue(recorded.poll()[0] instanceof Request1);
    }

    @Test
    public void testBlockedSync() throws Exception {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.blockMessages((msg, id) -> msg instanceof Request1);

        assertTrue(client1.checkConnection(endpoint, true));

        Response2 resp2 = (Response2) client1.invokeSync(endpoint, new Request2(), 500);

        assertEquals(1, resp2.val);

        Future<Response1> resp = Utils.runInThread(() -> (Response1) client1.invokeSync(endpoint, new Request1(), 30_000));

        Thread.sleep(500);

        Queue<Object[]> msgs = client1.blockedMessages();

        assertEquals(1, msgs.size());

        assertFalse(resp.isDone());

        client1.stopBlock();

        resp.get(5_000, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testBlockedAsync() throws Exception {
        RpcClientEx client1 = (RpcClientEx) createClient();
        client1.blockMessages((msg, id) -> msg instanceof Request1);

        assertTrue(client1.checkConnection(endpoint, true));
//
//        Response2 resp2 = (Response2) client1.invokeSync(endpoint, new Request2(), 500);
//
//        assertEquals(1, resp2.val);

        CompletableFuture resp = new CompletableFuture();

        client1.invokeAsync(endpoint, new Request1(), new InvokeCallback() {
            @Override public void complete(Object result, Throwable err) {
                resp.complete(result);
            }
        }, 30_000);

        Thread.sleep(500);

        Queue<Object[]> msgs = client1.blockedMessages();

        assertEquals(1, msgs.size());

        assertFalse(resp.isDone());

        client1.stopBlock();

        resp.get(5_000, TimeUnit.MILLISECONDS);
    }

    private static class Request1RpcProcessor implements RpcProcessor<Request1> {
        @Override public void handleRequest(RpcContext rpcCtx, Request1 request) {
            if (request.val == 10_000)
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                    // No-op.
                }

            Response1 resp1 = new Response1();
            resp1.val = request.val + 1;
            rpcCtx.sendResponse(resp1);
        }

        @Override public String interest() {
            return Request1.class.getName();
        }
    }

    private static class Request2RpcProcessor implements RpcProcessor<Request2> {
        @Override public void handleRequest(RpcContext rpcCtx, Request2 request) {
            Response2 resp2 = new Response2();
            resp2.val = request.val + 1;
            rpcCtx.sendResponse(resp2);
        }

        @Override public String interest() {
            return Request2.class.getName();
        }
    }

    private static class Request1 implements Message {
        int val;
    }

    private static class Request2 implements Message {
        int val;
    }

    private static class Response1 implements Message {
        int val;
    }

    private static class Response2 implements Message {
        int val;
    }
}
