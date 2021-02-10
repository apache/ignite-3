package org.apache.ignite.raft;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.raft.rpc.RpcOptions;
import org.apache.ignite.raft.service.CliClientServiceImpl;
import org.apache.ignite.raft.service.RouteTable;
import org.junit.Test;

public class RaftClientTest {
    private static final System.Logger LOG = System.getLogger(RaftClientTest.class.getName());

    @Test
    public void testClient() throws TimeoutException, InterruptedException {
        List<PeerId> peers = Arrays.asList(
            new PeerId("127.0.0.1", 8080),
            new PeerId("127.0.0.1", 8081),
            new PeerId("127.0.0.1", 8082)
        );

        Configuration initConf = new Configuration(peers);

        String groupId = "unittest";

        RouteTable.getInstance().updateConfiguration(groupId, initConf);

        final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
        cliClientService.init(new RpcOptions());

        if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
            throw new IllegalStateException("Refresh leader failed");
        }

        final PeerId leader = RouteTable.getInstance().selectLeader(groupId);

        LOG.log(System.Logger.Level.INFO, "Leader is " + leader);
    }
}
