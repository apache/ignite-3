package org.apache.ignite.raft.server.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
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
import org.apache.ignite.raft.jraft.JRaftServiceFactory;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.PingRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.GetFileRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.InstallSnapshotRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.ReadIndexRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.RequestVoteRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.TimeoutNowRequestProcessor;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.RaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.impl.LocalRaftMetaStorage;
import org.apache.ignite.raft.jraft.storage.impl.RocksDBLogStorage;
import org.apache.ignite.raft.jraft.storage.snapshot.local.LocalSnapshotStorage;
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

    private ConcurrentMap<String, NodeImpl> groups = new ConcurrentHashMap<>();

    /** The manager of raft nodes belonging to this server. */
    private NodeManager nodeManager; // TODO asch Use either groups or nodemanager.

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

                NodeImpl lsnr = groups.get(req0.groupId());

                if (lsnr == null) {
                    sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);

                    return;
                }

                PeerId leaderId = lsnr.getLeaderId();

                if (leaderId == null) {
                    sendError(sender, correlationId, RaftErrorCode.NO_LEADER);

                    return;
                }

                // Find by host and port.
                Peer leader0 = new Peer(service.topologyService().allMembers().stream().
                    filter(m -> m.host().equals(leaderId.getIp()) && m.port() == leaderId.getPort()).findFirst().orElse(null));

                GetLeaderResponse resp = clientMsgFactory.getLeaderResponse().leader(leader0).build();

                service.messagingService().send(sender, resp, correlationId);
            }
            else if (message instanceof ActionRequest) {
                ActionRequest<?> req0 = (ActionRequest<?>) message;

                NodeImpl node = groups.get(req0.groupId());

                if (node == null) {
                    sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);

                    return;
                }

                if (req0.command() instanceof WriteCommand) {
                    node.apply(new Task(ByteBuffer.wrap(JDKMarshaller.DEFAULT.marshall(req0.command())), new MyClosure<>(req0.command()) {
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
                    node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                        @Override public void run(Status status, long index, byte[] reqCtx) {
                            if (status.isOk()) {
                                DelegatingStateMachine fsm = (DelegatingStateMachine) node.getOptions().getFsm();

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
//                    handleActionRequest(sender, req0, correlationId, readQueue, node);
//                else
//                    handleActionRequest(sender, req0, correlationId, writeQueue, node);
            }
            else {
                LOG.warn("Unsupported message class " + message.getClass().getName());
            }
        });

        rpcServer = new IgniteRpcServer(service, reuse, nodeManager);
    }

    @Override public ClusterService clusterService() {
        return service;
    }

    @Override public RaftNode startRaftNode(String groupId, RaftGroupCommandListener lsnr, @Nullable List<Peer> initialConf) {
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
                return new PeerId(new Endpoint(p.getNode().host(), p.getNode().port()), 0);
            }).collect(Collectors.toList());

            nodeOptions.setInitialConf(new Configuration(mapped, null));
        }

        IgniteRpcClient client = new IgniteRpcClient(service, true);

        nodeOptions.setRpcClient(client);

        final RaftGroupService server = new RaftGroupService(groupId, new PeerId(endpoint, 0, ElectionPriority.DISABLED),
            nodeOptions, rpcServer, true);

        NodeImpl node = (NodeImpl) server.start(false);

        node.setPeer(new Peer(clusterNode));

        nodeManager.add(node);

        groups.put(groupId, node);

        return node;
    }

    public NodeImpl node(String groupId) {
        return groups.get(groupId);
    }

    @Override public void shutdown() throws Exception {
        if (!reuse) {
            rpcServer.shutdown();

            for (NodeImpl node : groups.values()) {
                nodeManager.remove(node);

                node.shutdown();
                node.join();
            }
        }
    }

    private class DelegatingStateMachine extends StateMachineAdapter {
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
