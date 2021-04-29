package org.apache.ignite.raft.server.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ElectionPriority;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.RaftErrorResponse;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.util.BytesUtil;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.JDKMarshaller;
import org.apache.ignite.raft.server.RaftNode;
import org.apache.ignite.raft.server.RaftServer;
import org.jetbrains.annotations.Nullable;

public class JRaftServerImpl implements RaftServer {
    private static final IgniteLogger LOG = IgniteLogger.forClass(JRaftServerImpl.class);

    private final ClusterService service;
    private final boolean reuse;
    private final String dataPath;
    private final RaftClientMessageFactory clientMsgFactory;

    private IgniteRpcServer rpcServer;

    private ConcurrentMap<String, RaftGroupService> groups = new ConcurrentHashMap<>();

    private final NodeManager nodeManager;

    public JRaftServerImpl(ClusterService service, String dataPath, RaftClientMessageFactory factory, boolean reuse) {
        this.service = service;
        this.reuse = reuse;
        this.dataPath = dataPath;
        this.clientMsgFactory = factory;
        this.nodeManager = new NodeManager();

        assert !reuse || service.topologyService().localMember() != null;

        // RAFT client messages. TODO asch refactor to processors
        service.messagingService().addMessageHandler((message, sender, correlationId) -> {
            if (message instanceof GetLeaderRequest) {
                GetLeaderRequest req0 = (GetLeaderRequest) message;

                RaftGroupService lsnr = groups.get(req0.groupId());

                if (lsnr == null) {
                    sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);

                    return;
                }

                PeerId leaderId = lsnr.getRaftNode().getLeaderId();

                if (leaderId == null) {
                    sendError(sender, correlationId, RaftErrorCode.NO_LEADER);

                    return;
                }

                // Find by host and port.
                Peer leader0 = new Peer(leaderId.getEndpoint().toString());

                GetLeaderResponse resp = clientMsgFactory.getLeaderResponse().leader(leader0).build();

                service.messagingService().send(sender, resp, correlationId);
            }
            else if (message instanceof ActionRequest) {
                ActionRequest<?> req0 = (ActionRequest<?>) message;

                RaftGroupService svc = groups.get(req0.groupId());

                if (svc == null) {
                    sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);

                    return;
                }

                if (req0.command() instanceof WriteCommand) {
                    svc.getRaftNode().apply(new Task(ByteBuffer.wrap(JDKMarshaller.DEFAULT.marshall(req0.command())), new MyClosure<>(req0.command()) {
                        @Override public void success(Object res) {
                            var msg = clientMsgFactory.actionResponse().result(res).build();
                            service.messagingService().send(sender, msg, correlationId);
                        }

                        @Override public void failure(Throwable err) {
                            sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);
                        }
                    }));
                }
                else {
                    // TODO asch purpose of request context ?
                    svc.getRaftNode().readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                        @Override public void run(Status status, long index, byte[] reqCtx) {
                            if (status.isOk()) {
                                DelegatingStateMachine fsm = (DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

                                List<CommandClosure<ReadCommand>> l = new ArrayList<>(1);
                                l.add(new CommandClosure<ReadCommand>() {
                                    @Override public ReadCommand command() {
                                        return (ReadCommand) req0.command();
                                    }

                                    @Override public void success(Object res) {
                                        var msg = clientMsgFactory.actionResponse().result(res).build();
                                        service.messagingService().send(sender, msg, correlationId);
                                    }

                                    @Override public void failure(Throwable err) {
                                        sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);
                                    }
                                });

                                fsm.listener.onRead(l.iterator());
                            }
                            else {
                                // TODO asch state machine error.
                                sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);
                            }
                        }
                    });
                }

//                if (req0.command() instanceof ReadCommand)
//                    handleActionRequest(sender, req0, correlationId, readQueue, svc);
//                else
//                    handleActionRequest(sender, req0, correlationId, writeQueue, svc);
            }
            else {
                LOG.warn("Unsupported message class " + message.getClass().getName());
            }
        });

        rpcServer = new IgniteRpcServer(service, reuse, nodeManager);
        rpcServer.init(null);
    }

    @Override public ClusterService clusterService() {
        return service;
    }

    @Override public synchronized RaftNode startRaftNode(String groupId, RaftGroupCommandListener lsnr, @Nullable List<Peer> initialConf) {
        if (groups.containsKey(groupId))
            return groups.get(groupId);

        final NodeOptions nodeOptions = new NodeOptions();

        ClusterNode clusterNode = service.topologyService().localMember();

        Endpoint endpoint = new Endpoint(clusterNode.host(), clusterNode.port());

        final String serverDataPath = this.dataPath + File.separator + groupId + "_" + endpoint.toString().replace(':', '_');
        new File(serverDataPath).mkdirs();

        nodeOptions.setLogUri(serverDataPath + File.separator + "logs");
        nodeOptions.setRaftMetaUri(serverDataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(serverDataPath + File.separator + "snapshot");

        nodeOptions.setFsm(new DelegatingStateMachine(lsnr));

        if (initialConf != null) {
            List<PeerId> mapped = initialConf.stream().map(p -> {
                return PeerId.fromPeer(p);
            }).collect(Collectors.toList());

            nodeOptions.setInitialConf(new Configuration(mapped, null));
        }

        IgniteRpcClient client = new IgniteRpcClient(service, true);

        nodeOptions.setRpcClient(client);

        final RaftGroupService server = new RaftGroupService(groupId, new PeerId(endpoint, 0, ElectionPriority.DISABLED),
            nodeOptions, rpcServer, nodeManager, true);

        server.start(false);

        groups.put(groupId, server);

        return server;
    }

    @Override public void shutdown() throws Exception {
        rpcServer.shutdown();

        for (RaftGroupService groupService : groups.values())
            groupService.shutdown();
    }

    /** */
    private static class DelegatingStateMachine extends StateMachineAdapter {
        private final RaftGroupCommandListener listener;

        DelegatingStateMachine(RaftGroupCommandListener listener) {
            this.listener = listener;
        }

        @Override public void onApply(Iterator iter) {
            listener.onWrite(new java.util.Iterator<CommandClosure<WriteCommand>>() {
                @Override public boolean hasNext() {
                    return iter.hasNext();
                }

                @Override public CommandClosure<WriteCommand> next() {
                    @Nullable CommandClosure<WriteCommand> done = (CommandClosure<WriteCommand>) iter.done();
                    ByteBuffer data = iter.getData();

                    return new CommandClosure<WriteCommand>() {
                        @Override public WriteCommand command() {
                            return JDKMarshaller.DEFAULT.unmarshall(data.array());
                        }

                        @Override public void success(Object res) {
                            if (done != null)
                                done.success(res);

                            iter.next();
                        }

                        @Override public void failure(Throwable err) {
                            if (done != null)
                                done.failure(err);

                            iter.setErrorAndRollback(1, new Status(RaftError.ESTATEMACHINE, err.getMessage()));
                        }
                    };
                }
            });
        }
    }

    private void sendError(ClusterNode sender, String corellationId, RaftErrorCode errorCode) {
        RaftErrorResponse resp = clientMsgFactory.raftErrorResponse().errorCode(errorCode).build();

        service.messagingService().send(sender, resp, corellationId);
    }

    private abstract class MyClosure<T extends Command> implements Closure, CommandClosure<T> {
        private final T command;

        MyClosure(T command) {
            this.command = command;
        }

        @Override public T command() {
            return command;
        }

        @Override public void run(Status status) {
            assert false;
        }
    }
}
