package org.apache.ignite.raft.server;

import java.util.List;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.server.impl.SimpleRaftServerImpl;

class ITSimpleCounterServerTest extends RaftCounterServerAbstractTest {
    /** {@inheritDoc} */
    @Override protected RaftServer createServer() {
        ClusterService service = clusterService(SERVER_ID, PORT, List.of());

        return new SimpleRaftServerImpl(service, null, FACTORY, false);
    }
}
