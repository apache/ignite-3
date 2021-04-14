package org.apache.ignite.raft.server;

import java.io.File;
import java.util.List;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.server.impl.JRaftServerImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import static org.junit.Assert.assertTrue;

public class ITJRaftCounterServerTest extends RaftCounterServerAbstractTest {
    private String dataPath;

    @BeforeEach
    @Override void before(TestInfo testInfo) {
        this.dataPath = TestUtils.mkTempDir();

        super.before(testInfo);

        JRaftServerImpl server0 = (JRaftServerImpl) server;

        NodeImpl node = server0.node(COUNTER_GROUP_ID_0);

        // Wait for leader.
        while(node.getLeaderId() == null)
            Thread.onSpinWait();
    }

    @AfterEach
    @Override void after() throws Exception {
        super.after();

        assertTrue(Utils.delete(new File(this.dataPath)));
    }

    /** {@inheritDoc} */
    @Override protected RaftServer createServer() {
        ClusterService service = clusterService(SERVER_ID, PORT, List.of());

        return new JRaftServerImpl(service, dataPath, FACTORY, false);
    }
}
