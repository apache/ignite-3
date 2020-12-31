package com.alipay.sofa.jraft.rpc;

import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RemotingException;
import com.alipay.sofa.jraft.rpc.impl.LocalRpcClient;
import com.alipay.sofa.jraft.rpc.impl.LocalRpcServer;
import com.alipay.sofa.jraft.util.Endpoint;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * TODO add test for localconn.close, timeouts.
 */
public class LocalRpcTest {
    private Endpoint endpoint;
    private LocalRpcServer server;

    @Before
    public void setup() {
        endpoint = PeerId.parsePeer("localhost:1000").getEndpoint();
        server = new LocalRpcServer(endpoint);
        server.registerProcessor(new Request1RpcProcessor());
        server.registerProcessor(new Request2RpcProcessor());
        server.init(null);
    }

    @After
    public void teardown() {
        server.shutdown();

        assertNull(LocalRpcServer.servers.get(endpoint));
    }

    @Test
    public void testStartStopServer() {
        assertNotNull(LocalRpcServer.servers.get(endpoint));
    }

    @Test
    public void testConnection() {
        LocalRpcClient client = new LocalRpcClient();

        assertFalse(client.checkConnection(endpoint));

        assertTrue(client.checkConnection(endpoint, true));
    }

    @Test
    public void testSyncProcessing() throws RemotingException, InterruptedException {
        RpcClient client = new LocalRpcClient();
        Response1 resp1 = (Response1) client.invokeSync(endpoint, new Request1(), new InvokeContext(), 5000);
        assertNotNull(resp1);

        Response2 resp2 = (Response2) client.invokeSync(endpoint, new Request2(), new InvokeContext(), 5000);
        assertNotNull(resp2);
    }

    @Test
    public void testAsyncProcessing() throws RemotingException, InterruptedException {
        RpcClient client = new LocalRpcClient();

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
        RpcClient client1 = new LocalRpcClient();
        RpcClient client2 = new LocalRpcClient();

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
        RpcClient client1 = new LocalRpcClient();
        RpcClient client2 = new LocalRpcClient();

        assertTrue(client1.checkConnection(endpoint, true));
        assertTrue(client2.checkConnection(endpoint, true));

        server.shutdown();

        assertFalse(client1.checkConnection(endpoint));
        assertFalse(client2.checkConnection(endpoint));
    }

    private static class Request1RpcProcessor implements RpcProcessor<Request1> {
        @Override public void handleRequest(RpcContext rpcCtx, Request1 request) {
            rpcCtx.sendResponse(new Response1());
        }

        @Override public String interest() {
            return Request1.class.getName();
        }
    }

    private static class Request2RpcProcessor implements RpcProcessor<Request2> {
        @Override public void handleRequest(RpcContext rpcCtx, Request2 request) {
            rpcCtx.sendResponse(new Response2());
        }

        @Override public String interest() {
            return Request2.class.getName();
        }
    }

    private static class Request1 implements Message {
    }

    private static class Request2 implements Message {
    }

    private static class Response1 implements Message {
    }

    private static class Response2 implements Message {
    }
}
