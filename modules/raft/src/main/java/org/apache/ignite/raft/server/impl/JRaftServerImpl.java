package org.apache.ignite.raft.server.impl;

import java.io.File;
import java.nio.ByteBuffer;
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
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.util.Endpoint;
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

    public JRaftServerImpl(ClusterService service, String dataPath, RaftClientMessageFactory factory, boolean reuse) {
        this.service = service;
        this.reuse = reuse;
        this.dataPath = dataPath;
        this.clientMsgFactory = factory;

        assert service.topologyService().localMember() != null;

        // RAFT client messages.
        service.messagingService().addMessageHandler((message, sender, correlationId) -> {
            if (message instanceof GetLeaderRequest) {
                GetLeaderRequest req0 = (GetLeaderRequest) message;

                NodeImpl lsnr = groups.get(req0.groupId());

                if (lsnr == null) {
                    sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);

                    return;
                }

                // TODO asch handle no leader.
                PeerId leaderId = lsnr.getLeaderId();

                // Find by host and port
                Peer leader0 = new Peer(service.topologyService().allMembers().stream().
                    filter(m -> m.host().equals(leaderId.getIp()) && m.port() == leaderId.getPort()).findFirst().orElse(null));

                GetLeaderResponse resp = clientMsgFactory.getLeaderResponse().leader(leader0).build();

                service.messagingService().send(sender, resp, correlationId);
            }
            else if (message instanceof ActionRequest) {
                ActionRequest<?> req0 = (ActionRequest<?>) message;

                NodeImpl lsnr = groups.get(req0.groupId());

                if (lsnr == null) {
                    sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);

                    return;
                }

                if (req0.command() instanceof WriteCommand) {
                    lsnr.apply(new Task(null, new MyClosure<>(req0.command()) {
                        @Override public void success(Object res) {
                            var msg = clientMsgFactory.actionResponse().result(res).build();
                            service.messagingService().send(sender, msg, correlationId);
                        }

                        @Override public void failure(Throwable err) {
                            sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);
                        }
                    }));
                }

//                if (req0.command() instanceof ReadCommand)
//                    handleActionRequest(sender, req0, correlationId, readQueue, lsnr);
//                else
//                    handleActionRequest(sender, req0, correlationId, writeQueue, lsnr);
            }
            else {
                LOG.warn("Unsupported message class " + message.getClass().getName());
            }
        });

        rpcServer = new IgniteRpcServer(service, reuse);
    }

    @Override public ClusterService clusterService() {
        return service;
    }

    @Override public void startRaftGroup(String groupId, RaftGroupCommandListener lsnr, @Nullable List<Peer> initialConf) {
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

        final RaftGroupService server = new RaftGroupService(groupId, new PeerId(endpoint, 0, ElectionPriority.DISABLED),
            nodeOptions, rpcServer, true);

        NodeImpl node = (NodeImpl) server.start(false);

        groups.put(groupId, node);
    }

    @Override public void shutdown() {
        if (!reuse)
            rpcServer.shutdown();
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
                    iter.next();

                    @Nullable Closure done = iter.done();

                    if (done != null) // This is a leader.
                        return (CommandClosure<WriteCommand>) done;

                    return new CommandClosure<WriteCommand>() {
                        @Override public WriteCommand command() {
                            ByteBuffer data = iter.getData();

                            return null; // To BO.
                        }

                        @Override public void success(Object res) {
                            // No-op.
                        }

                        @Override public void failure(Throwable err) {
                            iter.setErrorAndRollback(1, new Status(-1, err.getMessage()));
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
