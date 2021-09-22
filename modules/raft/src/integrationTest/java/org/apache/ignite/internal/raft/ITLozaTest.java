/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft;

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
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.utils.ClusterServiceTestUtils;
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
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Loza} functionality.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
public class ITLozaTest {
    /** Node's name. */
    private static final String NODE_NAME = "node1";

    /** Network factory. */
    protected static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** Server port offset. */
    protected static final int PORT = 20010;

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /** */
    @WorkDirectory
    private Path dataPath;

    /** Test node. */
    private final ClusterNode node = new ClusterNode(
        UUID.randomUUID().toString(),
        NODE_NAME,
        new NetworkAddress(getLocalAddress(), PORT)
    );

    /**
     * @param groupId Raft group id.
     */
    private void startClient(String groupId, Loza loza) throws ExecutionException, InterruptedException, TimeoutException {
        loza.prepareRaftGroup(groupId, List.of(node), () -> new RaftGroupListener() {
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

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    protected ClusterService clusterService(String name, int port, List<NetworkAddress> servers) {
        var network = ClusterServiceTestUtils.clusterService(
            name,
            port,
            new StaticNodeFinder(servers),
            SERIALIZATION_REGISTRY,
            NETWORK_FACTORY
        );

        network.start();

        return network;
    }

    /**
     * Tests that RaftGroupServiceImpl uses shared executor for retrying RaftGroupServiceImpl#sendWithRetry()
     */
    @Test
    public void testRaftServiceUsingSharedExecutor() throws Exception {
        var addr1 = new NetworkAddress(getLocalAddress(), PORT);

        ClusterService service = spy(clusterService(node.name(), PORT, List.of(addr1)));

        MessagingService messagingServiceMock = mock(MessagingService.class);

        when(service.messagingService()).thenReturn(messagingServiceMock);

        CompletableFuture<NetworkMessage> fut = new CompletableFuture<>();

        AtomicInteger executorsInvocations = new AtomicInteger(0);

        when(messagingServiceMock.invoke((NetworkAddress)any(), any(), anyLong())).thenAnswer(mock -> {
            String threadName = Thread.currentThread().getName();

            assertTrue(threadName.contains(Loza.CLIENT_POOL_NAME) || threadName.contains("main"));

            if (threadName.contains(Loza.CLIENT_POOL_NAME))
                executorsInvocations.incrementAndGet();

            return fut;
        });

        // Need to complete exceptionally to enforce sendWithRetry to be scheduled with shared executor.
        // See RaftGroupServiceImpl#recoverable
        fut.completeExceptionally(new Exception("Test exception", new IOException()));

        Loza loza = new Loza(service, dataPath);

        loza.start();

        for (int i = 0; i < 5; i++) {
            try {
                startClient(Integer.toString(i), loza);
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
}
