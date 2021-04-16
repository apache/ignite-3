package org.apache.ignite.raft.jraft.counter;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.raft.jraft.RouteTable;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.NodeTest;
import org.apache.ignite.raft.jraft.counter.rpc.IncrementAndGetRequest;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RemotingException;
import org.apache.ignite.raft.jraft.option.CliOptions;
import org.apache.ignite.raft.jraft.rpc.InvokeCallback;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CounterExampleTest {
    static final Logger LOG = LoggerFactory.getLogger(NodeTest.class);
    @Rule
    public TestName testName = new TestName();
    private String dataPath;

    @Before
    public void setup() throws Exception {
        System.out.println(">>>>>>>>>>>>>>> Start test method: " + this.testName.getMethodName());
        this.dataPath = TestUtils.mkTempDir();
        new File(this.dataPath).mkdirs();
        assertEquals(NodeImpl.GLOBAL_NUM_NODES.get(), 0);
    }

    @After
    public void teardown() throws Exception {
        if (NodeImpl.GLOBAL_NUM_NODES.get() > 0) {
            Thread.sleep(5000);
            assertEquals(0, NodeImpl.GLOBAL_NUM_NODES.get());
        }
        assertTrue(Utils.delete(new File(this.dataPath)));

        System.out.println(">>>>>>>>>>>>>>> End test method: " + this.testName.getMethodName());
    }

    @Test
    public void testCounter() throws IOException, InterruptedException, TimeoutException, RemotingException {
        try {
            List<PeerId> peers = Arrays.asList(
                new PeerId("127.0.0.1", 8080),
                new PeerId("127.0.0.1", 8081),
                new PeerId("127.0.0.1", 8082)
            );

            Configuration initConf = new Configuration(peers);

            String groupId = "counter";

            // Create initial topology.
            for (PeerId peer : peers)
                CounterServer.start(dataPath, groupId, peer, initConf);

            LOG.info("Waiting for leader election");

            Thread.sleep(2000);

            RouteTable.getInstance().updateConfiguration(groupId, initConf);

            final CliClientServiceImpl cliClientService = new CliClientServiceImpl();
            cliClientService.init(new CliOptions());

            if (!RouteTable.getInstance().refreshLeader(cliClientService, groupId, 1000).isOk()) {
                throw new IllegalStateException("Refresh leader failed");
            }

            final PeerId leader = RouteTable.getInstance().selectLeader(groupId);
            System.out.println("Leader is " + leader);
            final int n = 1000;
            final CountDownLatch latch = new CountDownLatch(n);
            final long start = System.currentTimeMillis();
            for (int i = 0; i < n; i++) {
                incrementAndGet(cliClientService, leader, i, latch);
            }
            latch.await();
            System.out.println(n + " ops, cost : " + (System.currentTimeMillis() - start) + " ms.");
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
                    System.out.println("incrementAndGet result:" + result);
                } else {
                    err.printStackTrace();
                    latch.countDown();
                }
            }

            @Override
            public Executor executor() {
                return null;
            }
        }, 5000);
    }
}
