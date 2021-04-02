package org.apache.ignite.raft.server.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.network.MessageHandlerHolder;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkClusterFactory;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.scalecube.ScaleCubeMemberResolver;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.exception.RaftException;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.RaftErrorResponse;
import org.apache.ignite.raft.client.service.CommandFuture;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.apache.ignite.raft.server.RaftServerOptions;
import org.apache.ignite.raft.server.RaftServer;

public class RaftServerImpl implements RaftServer {
    private static LogWrapper LOG = new LogWrapper(RaftServerImpl.class);

    private NetworkCluster member;

    private ConcurrentMap<String, RaftGroupCommandListener> listeners = new ConcurrentHashMap<>();

    private RaftServerOptions options;

    @Override public NetworkMember networkMember() {
        return member.localMember();
    }

    @Override public void setListener(String groupId, RaftGroupCommandListener lsnr) {
        listeners.put(groupId, lsnr);
    }

    @Override public void clearListener(String groupId) {
        listeners.remove(groupId);
    }

    private BlockingQueue<CommandFutureEx<ReadCommand>> readQueue;

    private BlockingQueue<CommandFutureEx<WriteCommand>> writeQueue;

    private Thread readWorker;

    private Thread writeWorker;

    @Override public synchronized void init(RaftServerOptions options) {
        Objects.requireNonNull(options.id);
        Objects.requireNonNull(options.clientMsgFactory);

        readQueue = new ArrayBlockingQueue<CommandFutureEx<ReadCommand>>(options.queueSize);
        writeQueue = new ArrayBlockingQueue<CommandFutureEx<WriteCommand>>(options.queueSize);

        this.options = options;

        member = new NetworkClusterFactory(options.id, options.localPort,
            options.members == null ? List.of() : new ArrayList<>(options.members))
            .startScaleCubeBasedCluster(new ScaleCubeMemberResolver(), new MessageHandlerHolder());

        member.addHandlersProvider(new NetworkHandlersProvider() {
            @Override public NetworkMessageHandler messageHandler() {
                return new NetworkMessageHandler() {
                    @Override public void onReceived(NetworkMessage msg) {
                        NetworkMember sender = msg.sender();

                        Object req = msg.data();

                        if (req instanceof GetLeaderRequest) {
                            GetLeaderResponse resp = options.clientMsgFactory.getLeaderResponse().leader(new Peer(member.localMember())).build();

                            member.send(sender, resp, msg.corellationId());
                        }
                        else if (req instanceof ActionRequest) {
                            ActionRequest req0 = (ActionRequest) req;

                            RaftGroupCommandListener lsnr = listeners.get(req0.groupId());

                            if (lsnr == null) {
                                sendError(sender, msg.corellationId(), RaftErrorCode.ILLEGAL_STATE);

                                return;
                            }

                            if (req0.command() instanceof ReadCommand) {
                                handleActionRequest(sender, req0, msg.corellationId(), readQueue, lsnr);
                            }
                            else {
                                handleActionRequest(sender, req0, msg.corellationId(), writeQueue, lsnr);
                            }
                        }
                        else {
                            LOG.warn("Unsupported message class " + req.getClass().getName());
                        }
                    }
                };
            }
        });

        readWorker = new Thread(() -> processQueue(readQueue, (l, i) -> l.onRead(i)), "read-cmd-worker#" + options.id);
        readWorker.setDaemon(true);
        readWorker.start();

        writeWorker = new Thread(() -> processQueue(writeQueue, (l, i) -> l.onWrite(i)), "write-cmd-worker#" + options.id);
        writeWorker.setDaemon(true);
        writeWorker.start();

        LOG.info("Started replication server [id=" + options.id + ", localPort=" + options.localPort + ']');
    }

    @Override public synchronized void destroy() throws Exception {
        Objects.requireNonNull(member);

        member.shutdown();

        readWorker.interrupt();
        readWorker.join();

        writeWorker.interrupt();
        writeWorker.join();

        LOG.info("Stopped replication server [id=" + options.id + ", localPort=" + options.localPort + ']');
    }

    private <T extends Command> void handleActionRequest(
        NetworkMember sender,
        ActionRequest req,
        String corellationId,
        BlockingQueue<CommandFutureEx<T>> queue,
        RaftGroupCommandListener lsnr
    ) {
        CompletableFuture fut0 = new CompletableFuture();

        if (!queue.offer(new CommandFutureEx<T>() {
            @Override public RaftGroupCommandListener listener() {
                return lsnr;
            }

            @Override public T command() {
                return (T) req.command();
            }

            @Override public CompletableFuture future() {
                return fut0;
            }
        })) {
            // Queue out of capacity.
            sendError(sender, corellationId, RaftErrorCode.BUSY);

            return;
        }

        fut0.thenApply(res -> {
            member.send(sender, options.clientMsgFactory.actionResponse().result(res).build(), corellationId);

            return null;
        });
    }

    private <T extends Command> void processQueue(
        BlockingQueue<CommandFutureEx<T>> queue,
        BiConsumer<RaftGroupCommandListener, Iterator<CommandFuture<T>>> clo
    ) {
        while(!Thread.interrupted()) {
            try {
                CommandFutureEx<T> cmdFut = queue.take();

                RaftGroupCommandListener lsnr = cmdFut.listener();

                if (lsnr == null)
                    cmdFut.future().completeExceptionally(new RaftException(RaftErrorCode.ILLEGAL_STATE));
                else {
                    List<CommandFuture<T>> cmdFut1 = List.of(cmdFut);

                    Iterator<CommandFuture<T>> iter = cmdFut1.iterator();

                    clo.accept(lsnr, iter);
                }
            }
            catch (InterruptedException e) {
                return;
            }
        }
    }

    private void sendError(NetworkMember sender, String corellationId, RaftErrorCode errorCode) {
        RaftErrorResponse resp = options.clientMsgFactory.raftErrorResponse().errorCode(errorCode).build();

        member.send(sender, resp, corellationId);
    }

    private interface CommandFutureEx<T extends Command> extends CommandFuture<T> {
        RaftGroupCommandListener listener();
    }
}
