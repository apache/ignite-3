package org.apache.ignite.raft.server;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(WorkDirectoryExtension.class)
public class ITLozaTest extends RaftServerAbstractTest{
    private static final String GROUP_NAME = "raft_group";

    private static final String NODE_NAME = "node1";
    /** */
    @WorkDirectory
    private Path dataPath;

    private Loza loza1;
    /**
     * The client port offset.
     */
    private static final int CLIENT_PORT = 6003;

    /**
     * Clients list.
     */
    private final List<RaftGroupService> clients = new ArrayList<>();

    /** Test node. */
    private final ClusterNode node = new ClusterNode(
        UUID.randomUUID().toString(),
        NODE_NAME,
        new NetworkAddress(getLocalAddress(), PORT)
    );

    /**
     * @param testInfo Test info.
     * @throws Exception If failed.
     */
    @BeforeEach
    void before(TestInfo testInfo) throws Exception {
        LOG.info(">>>> Starting test {}", testInfo.getTestMethod().orElseThrow().getName());

        var addr1 = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service1 = clusterService(
            node.name(), PORT, List.of(addr1), true);

        loza1 = new Loza(service1, dataPath);

        loza1.start();
    }

    /**
     * @throws Exception If failed.
     */
    @AfterEach
    @Override public void after(TestInfo testInfo) throws Exception {
        loza1.stop();

        super.after(testInfo);
    }


    @Test
    public void testThreadsStartRaftService() {
        RaftGroupService client1 = null;

        RaftGroupService client2 = null;

        try {
            client1 = startClient("2");

            client1.refreshLeader().get();
        }
        catch (ExecutionException | InterruptedException | TimeoutException e) {
            fail(e);
        }

        Set<Thread> threadsBefore = getAllCurrentThreads();

        Set<String> threadNamesBefore = threadsBefore.stream().map(Thread::getName).collect(Collectors.toSet());

        try {
            for (int i = 0; i < 100; i++) {
                RaftGroupService client = startClient(Integer.toString(i));

                client.refreshLeader().get();
            }
        }
        catch (ExecutionException | InterruptedException | TimeoutException e) {
            fail(e);
        }


        Set<Thread> threadsAfter = getAllCurrentThreads();

        Set<String> threadNamesAfter = threadsAfter.stream().map(Thread::getName).collect(Collectors.toSet());

        threadNamesAfter.removeAll(threadNamesBefore);

        assertEquals(threadsBefore.size(), threadsAfter.size(), "Difference: " + threadNamesAfter);
    }


    /**
     * @param groupId Group id.
     * @return The client.
     * @throws Exception If failed.
     */
    private RaftGroupService startClient(String groupId) throws ExecutionException, InterruptedException, TimeoutException {
        RaftGroupService client = loza1.prepareRaftGroup(groupId, List.of(node), new RaftGroupListener() {
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
        }).get(10, TimeUnit.SECONDS);

        clients.add(client);

        return client;
    }

    /**
     * Get a set of Disruptor threads for the well known JRaft services.
     *
     * @return Set of Disruptor threads.
     */
    @NotNull private Set<Thread> getAllCurrentThreads() {
        return Thread.getAllStackTraces().keySet().stream()
            .filter(th -> th.getName().contains("Raft")
            ).collect(Collectors.toSet());
    }
}
