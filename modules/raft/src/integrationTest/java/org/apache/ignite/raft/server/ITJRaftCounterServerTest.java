package org.apache.ignite.raft.server;

import java.util.List;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.server.impl.JRaftServerImpl;

class ITJRaftCounterServerTest extends RaftCounterServerAbstractTest {
    /** {@inheritDoc} */
    @Override protected RaftServer createServer() {
        ClusterService service = clusterService(SERVER_ID, PORT, List.of());

        return new JRaftServerImpl(service, null, FACTORY, false);
    }
}
