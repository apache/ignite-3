package org.apache.ignite.raft.server;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
public class ITLozaTest extends RaftServerAbstractTest{
    private static final String NODE_NAME = "node1";

    /** */
    @WorkDirectory
    private Path dataPath;

    private Loza loza;

    /** Cluster service. */
    ClusterService service;

    /** Test node. */
    private final ClusterNode node = new ClusterNode(
        UUID.randomUUID().toString(),
        NODE_NAME,
        new NetworkAddress(getLocalAddress(), PORT)
    );

    /**
     * Tests that RaftGroupServiceImpl uses shared executor for retrying RaftGroupServiceImpl#sendWithRetry()
     */
    @Test
    public void testRaftServiceUsingSharedExecutor() throws Exception {
        var addr1 = new NetworkAddress(getLocalAddress(), PORT);

        service = spy(clusterService(node.name(), PORT, List.of(addr1), true));

        MessagingService messagingServiceMock = mock(MessagingService.class);

        when(service.messagingService()).thenReturn(messagingServiceMock);

        CompletableFuture<NetworkMessage> fut  = new CompletableFuture<>();

        AtomicInteger executorsInvocations = new AtomicInteger(0);

        when(messagingServiceMock.invoke((NetworkAddress)any(), any(), anyLong())).thenAnswer(mock -> {
            String threadName = Thread.currentThread().getName();

            assertTrue(threadName.contains(Loza.CLIENT_POOL_NAME) || threadName.contains("main"));

            if (threadName.contains(Loza.CLIENT_POOL_NAME))
                executorsInvocations.incrementAndGet();

            return fut;
        });

        // Need to complete exceptionally to enforce sendWithRetry to be scheduled with shared executor.
        fut.completeExceptionally(new Exception("Test exception", new IOException()));

        loza = new Loza(service, dataPath);

        loza.start();

        for (int i = 0; i < 5; i++) {
            try {
                startClient(Integer.toString(i));
            }
            catch (ExecutionException | InterruptedException | TimeoutException e) {
                if (!(e.getCause() instanceof TimeoutException))
                    fail(e);

                assertTrue(executorsInvocations.get() > 1);
            }

            executorsInvocations.set(0);
        }

        loza.stop();

        service.stop();
    }


    /**
     * @param groupId Group id.
     * @return The client.
     * @throws Exception If failed.
     */
    private RaftGroupService startClient(String groupId) throws ExecutionException, InterruptedException, TimeoutException {
        return loza.prepareRaftGroup(groupId, List.of(node), () ->  new RaftGroupListener() {
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
    }
}
