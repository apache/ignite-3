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

import static org.apache.ignite.raft.jraft.test.TestUtils.getLocalAddress;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Loza} functionality.
 */
@ExtendWith(WorkDirectoryExtension.class)
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
        ClusterService service = spy(clusterService(node.name(), PORT, List.of(node.address())));

        MessagingService messagingServiceMock = spy(service.messagingService());

        when(service.messagingService()).thenReturn(messagingServiceMock);

        CompletableFuture<NetworkMessage> exception = CompletableFuture.failedFuture(new Exception(new IOException()));

        Loza loza = new Loza(service, dataPath);

        loza.start();

        for (int i = 0; i < 5; i++) {
            // return an error on first invocation
            doReturn(exception)
                // assert that a retry has been issued on the executor
                .doAnswer(invocation -> {
                    assertThat(Thread.currentThread().getName(), containsString(Loza.CLIENT_POOL_NAME));

                    return exception;
                })
                // finally call the real method
                .doCallRealMethod()
                .when(messagingServiceMock).invoke(any(NetworkAddress.class), any(), anyLong());

            startClient(Integer.toString(i), loza);

            verify(messagingServiceMock, times(3 * (i + 1)))
                .invoke(any(NetworkAddress.class), any(), anyLong());
        }

        loza.stop();

        service.stop();
    }
}
