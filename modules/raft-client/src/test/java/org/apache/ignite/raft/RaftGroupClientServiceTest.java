package org.apache.ignite.raft;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.raft.closure.RpcResponseClosureAdapter;
import org.apache.ignite.raft.rpc.CustomRequest;
import org.apache.ignite.raft.rpc.CustomResponse;
import org.apache.ignite.raft.rpc.Message;
import org.apache.ignite.raft.rpc.MessageBuilderFactory;
import org.apache.ignite.raft.rpc.RpcOptions;
import org.apache.ignite.raft.rpc.RpcRequests;
import org.apache.ignite.raft.rpc.RpcRequests.ErrorResponse;
import org.apache.ignite.raft.rpc.TestCustomRequestProcessor;
import org.apache.ignite.raft.rpc.TestGetLeaderRequestProcessor;
import org.apache.ignite.raft.rpc.TestPingRequestProcessor;
import org.apache.ignite.raft.rpc.impl.LocalRpcServer;
import org.apache.ignite.raft.service.RaftGroupClientServiceImpl;
import org.apache.ignite.raft.service.RouteTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RaftGroupClientServiceTest {
    private static final System.Logger LOG = System.getLogger(RaftGroupClientServiceTest.class.getName());

    private static final Configuration initConf = new Configuration(Arrays.asList(
        new PeerId("127.0.0.1", 8080),
        new PeerId("127.0.0.1", 8081),
        new PeerId("127.0.0.1", 8082)
    ));

    private Set<LocalRpcServer> srvs = new HashSet<>();

    @Before
    public void beforeTest() {
        for (PeerId peer : initConf.getPeers()) {
            LocalRpcServer srv = new LocalRpcServer(peer.getEndpoint());
            srv.registerProcessor(new TestPingRequestProcessor());
            srv.registerProcessor(new TestGetLeaderRequestProcessor());
            srv.registerProcessor(new TestCustomRequestProcessor());
            srv.init(new RpcOptions());
            srvs.add(srv);
        }
    }

    @After
    public void afterTest() {
        for (LocalRpcServer srv : srvs) {
            srv.shutdown();
        }

        srvs.clear();
    }

    @Test
    public void testCustom() throws TimeoutException, InterruptedException, ExecutionException {
        final RaftGroupClientServiceImpl cliClientService = new RaftGroupClientServiceImpl();
        cliClientService.init(new RpcOptions());

        Future<Message> resp = cliClientService.sendCustom(initConf.getPeers().get(0).getEndpoint(), new CustomRequest(), null);

        assertTrue(resp.get() instanceof CustomResponse);
    }

    @Test
    public void testPing() throws TimeoutException, InterruptedException, ExecutionException {
        String groupId = "unittest";

        RouteTable.getInstance().updateConfiguration(groupId, initConf);

        final RaftGroupClientServiceImpl cliClientService = new RaftGroupClientServiceImpl();
        cliClientService.init(new RpcOptions());

        RpcRequests.PingRequest.Builder builder = MessageBuilderFactory.DEFAULT.createPingRequest();
        builder.setSendTimestamp(System.currentTimeMillis());
        RpcRequests.PingRequest req = builder.build();

        AtomicReference<Status> ref = new AtomicReference<>();

        RpcResponseClosureAdapter<ErrorResponse> done = new RpcResponseClosureAdapter<>() {
            @Override public void run(Status status) {
                ref.set(status);
            }
        };

        Future<Message> resp = cliClientService.ping(initConf.getPeers().get(0).getEndpoint(), req, done);

        ErrorResponse msg = (ErrorResponse) resp.get();

        assertEquals(RaftError.SUCCESS.getNumber(), msg.getErrorCode());
        assertTrue(ref.get().isOk());
    }

    @Test
    public void testRefreshLeader() throws TimeoutException, InterruptedException {
        List<PeerId> peers = Arrays.asList(
            new PeerId("127.0.0.1", 8080),
            new PeerId("127.0.0.1", 8081),
            new PeerId("127.0.0.1", 8082)
        );

        Configuration initConf = new Configuration(peers);

        String groupId = "unittest";

        RouteTable.getInstance().updateConfiguration(groupId, initConf);

        final RaftGroupClientServiceImpl raftGroupClientService = new RaftGroupClientServiceImpl();
        raftGroupClientService.init(new RpcOptions());

        if (!RouteTable.getInstance().refreshLeader(raftGroupClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);

        LOG.log(System.Logger.Level.INFO, "Leader is " + leader);
    }
}
