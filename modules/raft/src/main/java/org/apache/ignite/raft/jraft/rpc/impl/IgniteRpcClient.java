package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.raft.jraft.ReplicatorGroup;
import org.apache.ignite.raft.jraft.error.InvokeTimeoutException;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.RpcOptions;
import org.apache.ignite.raft.jraft.rpc.InvokeCallback;
import org.apache.ignite.raft.jraft.rpc.InvokeContext;
import org.apache.ignite.raft.jraft.rpc.RpcClientEx;
import org.apache.ignite.raft.jraft.rpc.RpcUtils;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IgniteRpcClient implements RpcClientEx {
    private static final Logger LOG                    = LoggerFactory.getLogger(IgniteRpcClient.class);

    public volatile ReplicatorGroup replicatorGroup = null;

    private volatile BiPredicate<Object, String> recordPred;
    private BiPredicate<Object, String> blockPred;

    private LinkedBlockingQueue<Object[]> blockedMsgs = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Object[]> recordedMsgs = new LinkedBlockingQueue<>();

    private final ClusterService service;

    private final boolean reuse;

    /**
     * @param service The service.
     * @param reuse {@code True} to reuse already started service.
     */
    public IgniteRpcClient(ClusterService service, boolean reuse) {
        this.service = service;
        this.reuse = reuse;

        if (!reuse)
            service.start();
    }

    public ClusterService clusterService() {
        return service;
    }

    @Override public boolean checkConnection(Endpoint endpoint) {
        return service.topologyService().allMembers().stream().anyMatch(n -> n.host().equals(endpoint.getIp()) && n.port() == endpoint.getPort());
    }

    @Override public void registerConnectEventListener(ReplicatorGroup replicatorGroup) {
        this.replicatorGroup = replicatorGroup;
    }

    @Override public Object invokeSync(Endpoint endpoint, Object request, InvokeContext ctx, long timeoutMs) throws InterruptedException, RemotingException {
        if (!checkConnection(endpoint))
            throw new RemotingException("Server is dead " + endpoint);

        CompletableFuture<Object> fut = new CompletableFuture();

        // Future hashcode used as corellation id.
        if (recordPred != null && recordPred.test(request, endpoint.toString()))
            recordedMsgs.add(new Object[]{request, endpoint.toString(), fut.hashCode(), System.currentTimeMillis(), null});

        boolean wasBlocked;

        synchronized (this) {
            wasBlocked = blockPred != null && blockPred.test(request, endpoint.toString());

            if (wasBlocked)
                blockedMsgs.add(new Object[]{request, endpoint.toString(), fut.hashCode(), System.currentTimeMillis(), (Runnable) () -> send(endpoint, request, fut)});
        }

        if (!wasBlocked)
            send(endpoint, request, fut);

        try {
            return fut.whenComplete((res, err) -> {
                if (err == null && recordPred != null && recordPred.test(res, this.toString()))
                    recordedMsgs.add(new Object[]{res, this.toString(), fut.hashCode(), System.currentTimeMillis(), null});
            }).get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw new RemotingException(e);
        } catch (TimeoutException e) {
            throw new InvokeTimeoutException();
        }
    }

    @Override public void invokeAsync(Endpoint endpoint, Object request, InvokeContext ctx, InvokeCallback callback, long timeoutMs) throws InterruptedException, RemotingException {
        if (!checkConnection(endpoint))
            throw new RemotingException("Server is dead " + endpoint);

        CompletableFuture<Object> fut = new CompletableFuture<>();

        fut.orTimeout(timeoutMs, TimeUnit.MILLISECONDS).
            whenComplete((res, err) -> {
                assert !(res == null && err == null) : res + " " + err;

                if (err == null && recordPred != null && recordPred.test(res, this.toString()))
                    recordedMsgs.add(new Object[]{res, this.toString(), fut.hashCode(), System.currentTimeMillis(), null});

                if (err instanceof ExecutionException)
                    err = new RemotingException(err); // TODO asch use IgniteException.
                else if (err instanceof TimeoutException) // Translate timeout exception.
                    err = new InvokeTimeoutException();

                Throwable finalErr = err;

                RpcUtils.runInThread(() -> callback.complete(res, finalErr)); // Avoid deadlocks if a closure has completed in the same thread.
            });

        // Future hashcode used as corellation id.
        if (recordPred != null && recordPred.test(request, endpoint.toString()))
            recordedMsgs.add(new Object[]{request, endpoint.toString(), fut.hashCode(), System.currentTimeMillis(), null});

        synchronized (this) {
            if (blockPred != null && blockPred.test(request, endpoint.toString())) {
                blockedMsgs.add(new Object[]{request, endpoint.toString(), fut.hashCode(), System.currentTimeMillis(), new Runnable() {
                    @Override public void run() {
                        send(endpoint, request, fut);
                    }
                }});

                return;
            }
        }

        send(endpoint, request, fut);
    }

    public void send(Endpoint endpoint, Object request, CompletableFuture<Object> fut) {
        CompletableFuture<NetworkMessage> fut0 = service.messagingService().invoke(endpoint.toString(), (NetworkMessage) request, 5000);

        fut0.whenComplete(new BiConsumer<NetworkMessage, Throwable>() {
            @Override public void accept(NetworkMessage resp, Throwable err) {
                if (err != null)
                    fut.completeExceptionally(err);
                else
                    fut.complete(resp);
            }
        });
    }

    @Override public boolean init(RpcOptions opts) {
        return true;
    }

    @Override public void shutdown() {
        try {
            if (!reuse)
                service.shutdown();
        }
        catch (Exception e) {
            throw new IgniteInternalException(e);
        }
    }

    @Override public void blockMessages(BiPredicate<Object, String> predicate) {
        this.blockPred = predicate;
    }

    @Override public void stopBlock() {
        ArrayList<Object[]> msgs = new ArrayList<>();

        synchronized (this) {
            blockedMsgs.drainTo(msgs);


            blockPred = null;
        }

        for (Object[] msg : msgs) {
            Runnable r = (Runnable) msg[4];

            r.run();
        }
    }

    @Override public void recordMessages(BiPredicate<Object, String> predicate) {
        this.recordPred = predicate;
    }

    @Override public void stopRecord() {
        this.recordPred = null;
    }

    @Override public Queue<Object[]> recordedMessages() {
        return recordedMsgs;
    }

    @Override public Queue<Object[]> blockedMessages() {
        return blockedMsgs;
    }
}
