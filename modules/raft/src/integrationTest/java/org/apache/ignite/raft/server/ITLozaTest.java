package org.apache.ignite.raft.server;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.server.RaftServerAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extensions;

import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(WorkDirectoryExtension.class)
public class ITLozaTest extends RaftServerAbstractTest {

    @WorkDirectory
    private Path dataPath;

    private Loza loza;

    private RaftGroupService raftGroupService;

    @BeforeEach
    private void setUp() throws ExecutionException, InterruptedException {
        var addr = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = clusterService("server", PORT, List.of(addr), true);
        waitForTopology(service, 1, 100000);

        loza = new Loza(service, dataPath);
        loza.start();
        raftGroupService = loza.prepareRaftGroup("groupId", Arrays.asList(
            service.topologyService().localMember()), new RaftGroupListener() {
            @Override public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {

            }

            @Override public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {

            }

            @Override public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {

            }

            @Override public boolean onSnapshotLoad(Path path) {
                return false;
            }

            @Override public void onShutdown() {

            }
        }).get();
    }

    private boolean waitForTopology(ClusterService cluster, int exp, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cluster.topologyService().allMembers().size() >= exp)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    @Test
    public void testLeaderAfterStart() throws ExecutionException, InterruptedException {
       assertNotNull(raftGroupService.leader());
    }

    @Test
    public void testRefreshMembers() throws ExecutionException, InterruptedException {
        raftGroupService.refreshMembers(false).get();
        assertTrue(!raftGroupService.peers().isEmpty());
    }
}
