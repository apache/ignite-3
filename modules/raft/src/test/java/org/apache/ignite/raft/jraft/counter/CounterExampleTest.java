package org.apache.ignite.raft.jraft.counter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.raft.jraft.RouteTable;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.counter.rpc.IncrementAndGetRequest;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.CliOptions;
import org.apache.ignite.raft.jraft.rpc.InvokeCallback;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.cli.CliClientServiceImpl;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

/**
 *
 */
public class CounterExampleTest {
    static final Logger LOG = LoggerFactory.getLogger(CounterExampleTest.class);

    @Rule
    public TestName testName = new TestName();
    private String dataPath;

    @Before
    public void setup() throws Exception {
        LOG.info(">>>>>>>>>>>>>>> Start test method: " + this.testName.getMethodName());
        this.dataPath = TestUtils.mkTempDir();
        new File(this.dataPath).mkdirs();
    }

    @After
    public void teardown() throws Exception {
        assertTrue(Utils.delete(new File(this.dataPath)));

        LOG.info(">>>>>>>>>>>>>>> End test method: " + this.testName.getMethodName());
    }

    @Test
    public void testCounter() throws IOException, InterruptedException, TimeoutException, RemotingException {
        try {
            List<PeerId> peers = Arrays.asList(
                new PeerId(TestUtils.getMyIp(), 8080),
                new PeerId(TestUtils.getMyIp(), 8081),
                new PeerId(TestUtils.getMyIp(), 8082)
            );

            Configuration initConf = new Configuration(peers);

            String groupId = "counter";

            List<CounterServer> servers = new ArrayList<>(peers.size());

            // Create initial topology.
            for (PeerId peer : peers) {
                servers.add(CounterServer.start(dataPath, groupId, peer, initConf));
            }

            LOG.info("Waiting for leader election");

            Thread.sleep(2000);

            RouteTable.getInstance().updateConfiguration(groupId, initConf);

            IgniteRpcServer rpcServer = (IgniteRpcServer) servers.get(0).raftGroupService().getRpcServer();

            // Reuse cluster service.
            final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
            CliOptions rpcOptions = new CliOptions();
            rpcOptions.setRpcClient(new IgniteRpcClient(rpcServer.clusterService(), true));
            cliClientService.init(rpcOptions);

            if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
                throw new IllegalStateException("Refresh leader failed");
            }

            final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
            LOG.info("Leader is " + leader);
            final int n = 1000;
            final CountDownLatch latch = new CountDownLatch(n);
            final long start = System.currentTimeMillis();
            for (int i = 0; i < n; i++) {
                incrementAndGet(cliClientService, leader, i, latch);
            }
            latch.await();
            LOG.info(n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.");
        } finally {
            CounterServer.stopAll();
        }
    }

    private static void incrementAndGet(final CliClientServiceImpl cliClientService, final PeerId leader,
                                        final long delta, CountDownLatch latch) throws RemotingException, InterruptedException {
        final IncrementAndGetRequest request = new IncrementAndGetRequest();
        request.setDelta(delta);

        cliClientService.getRpcClient().invokeAsync(leader.getEndpoint(), request, new InvokeCallback() {
            @Override
            public void complete(Object result, Throwable err) {
                if (err == null) {
                    latch.countDown();
                    LOG.info("incrementAndGet result:" + result);
                } else {
                    LOG.error("Failed to comnplete operation", err);
                    latch.countDown();
                }
            }
        }, 5000);
    }
}
