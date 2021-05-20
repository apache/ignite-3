package org.apache.ignite.raft.server.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.ElectionPriority;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.JDKMarshaller;
import org.apache.ignite.raft.server.RaftServer;
import org.jetbrains.annotations.Nullable;

/** */
public class JRaftServerImpl implements RaftServer {
    private static final IgniteLogger LOG = IgniteLogger.forClass(JRaftServerImpl.class);

    private final ClusterService service;
    private final boolean reuse;
    private final String dataPath;
    private final RaftClientMessageFactory clientMsgFactory;

    private IgniteRpcServer rpcServer;

    private ConcurrentMap<String, RaftGroupService> groups = new ConcurrentHashMap<>();

    private final NodeManager nodeManager;

    /**
     * @param service Cluster service.
     * @param dataPath Data path.
     * @param factory The factory.
     * @param reuse {@code True} to reuse cluster service (do not manage lifecyle)
     */
    public JRaftServerImpl(ClusterService service, String dataPath, RaftClientMessageFactory factory, boolean reuse) {
        this.service = service;
        this.reuse = reuse;
        this.dataPath = dataPath;
        this.clientMsgFactory = factory;
        this.nodeManager = new NodeManager();

        assert !reuse || service.topologyService().localMember() != null;

        rpcServer = new IgniteRpcServer(service, reuse, nodeManager);
        rpcServer.init(null);
    }

    /** {@inheritDoc} */
    @Override public ClusterService clusterService() {
        return service;
    }

    /**
     * @param groupId Group id.
     * @return The path to persistence folder.
     */
    @Override public String getServerDataPath(String groupId) {
        ClusterNode clusterNode = service.topologyService().localMember();

        Endpoint endpoint = new Endpoint(clusterNode.host(), clusterNode.port());

        return this.dataPath + File.separator + groupId + "_" + endpoint.toString().replace(':', '_');
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean startRaftNode(String groupId, RaftGroupCommandListener lsnr, @Nullable List<Peer> initialConf) {
        if (groups.containsKey(groupId))
            return false;

        final NodeOptions nodeOptions = new NodeOptions();

        ClusterNode clusterNode = service.topologyService().localMember();
        Endpoint endpoint = new Endpoint(clusterNode.host(), clusterNode.port());

        final String serverDataPath = getServerDataPath(groupId);
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

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopRaftNode(String groupId) {
        RaftGroupService svc = groups.remove(groupId);

        boolean stopped = svc != null;

        if (stopped)
            svc.shutdown();

        return stopped;
    }

    /** {@inheritDoc} */
    @Override public void shutdown() throws Exception {
        for (RaftGroupService groupService : groups.values())
            groupService.shutdown();

        rpcServer.shutdown();
    }

    /** */
    public static class DelegatingStateMachine extends StateMachineAdapter {
        private final RaftGroupCommandListener listener;

        /**
         * @param listener The listener.
         */
        DelegatingStateMachine(RaftGroupCommandListener listener) {
            this.listener = listener;
        }

        public RaftGroupCommandListener getListener() {
            return listener;
        }

        /** {@inheritDoc} */
        @Override public void onApply(Iterator iter) {
            try {
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

                            @Override public void result(Object res) {
                                if (done != null)
                                    done.result(res);

                                iter.next();
                            }
                        };
                    }
                });
            }
            catch (Exception err) {
                // TODO asch write a test.
                Status st = new Status(RaftError.ESTATEMACHINE, err.getMessage());

                iter.done().run(st);

                iter.setErrorAndRollback(1, st);
            }
        }

        /** {@inheritDoc} */
        @Override public void onSnapshotSave(SnapshotWriter writer, Closure done) {
            listener.onSnapshotSave(writer.getPath(), new Consumer<Boolean>() {
                @Override public void accept(Boolean res) {
                    if (res == Boolean.TRUE) {
                        done.run(Status.OK());
                    }
                    else {
                        done.run(new Status(RaftError.EIO, "Fail to save snapshot to %s", writer.getPath()));
                    }
                }
            });
        }

        /** {@inheritDoc} */
        @Override public boolean onSnapshotLoad(SnapshotReader reader) {
            return listener.onSnapshotLoad(reader.getPath());
        }
    }
}
