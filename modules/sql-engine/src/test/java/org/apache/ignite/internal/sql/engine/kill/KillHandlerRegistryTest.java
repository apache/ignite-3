/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.kill;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.kill.messages.CancelOperationRequest;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KillHandlerRegistryImpl}.
 */
public class KillHandlerRegistryTest extends BaseIgniteAbstractTest {
    private static final SqlQueryMessagesFactory MESSAGES_FACTORY = new SqlQueryMessagesFactory();

    private static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(5);

    private static final List<ClusterNode> TOPOLOGY = List.of(
            newNode(1, "node1"),
            newNode(2, "node2"),
            newNode(3, "node3")
    );

    private MessagingService messagingService;

    @Test
    @SuppressWarnings({"ThrowableNotThrown", "DataFlowIssue"})
    public void testRegistration() {
        KillHandlerRegistryImpl registry = createRegistry(nodeName -> null);

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> registry.register(null),
                "handler"
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> registry.register(new TestKillHandler(null, null, true, null)),
                "type"
        );

        TestKillHandler nodeQueryKillHandler = new TestKillHandler(null, null, true, CancellableOperationType.QUERY);

        registry.register(nodeQueryKillHandler);

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> registry.register(new TestKillHandler(null, null, false, CancellableOperationType.QUERY)),
                "A handler for the specified type has already been registered [type=" + CancellableOperationType.QUERY.name()
        );

        TestKillHandler clusterWideTxKillHandler = new TestKillHandler(null, null, false, CancellableOperationType.TRANSACTION);

        registry.register(clusterWideTxKillHandler);

        assertThat(registry.handler(CancellableOperationType.QUERY), is(not(nodeQueryKillHandler)));
        assertThat(registry.handler(CancellableOperationType.TRANSACTION), is(clusterWideTxKillHandler));

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> registry.handler(CancellableOperationType.COMPUTE),
                "No handler is registered for the specified type "
                        + "[type=" + CancellableOperationType.COMPUTE.name() + ", local=false]."
        );
    }

    @Test
    public void testSuccessfulCancelOnLocalDoesNotProducesNetworkRequests() {
        KillHandlerRegistryImpl registry = createRegistry(nodeName -> {
            throw new IllegalStateException("Shouldn't be called");
        });

        registry.register(new TestKillHandler(true));
        OperationKillHandler handler = registry.handler(CancellableOperationType.QUERY);

        assertThat(await(handler.cancelAsync(UUID.randomUUID().toString())), is(true));

        verify(messagingService, never())
                .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testSuccessfulCancelOnRemote() {
        KillHandlerRegistryImpl registry = createRegistry(nodeName -> nodeName.equals(node(2).name()));

        registry.register(new TestKillHandler(false));
        OperationKillHandler handler = registry.handler(CancellableOperationType.QUERY);

        assertThat(handler.cancelAsync(UUID.randomUUID().toString()), willBe(true));

        verify(messagingService, times(TOPOLOGY.size() - 1))
                .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testCancelFinishesWithErrorOnLocalNode() {
        NullPointerException expected = new NullPointerException("Expected");

        KillHandlerRegistryImpl registry = createRegistry(nodeName -> true);
        registry.register(new TestKillHandler(null, expected, true, CancellableOperationType.QUERY));
        OperationKillHandler handler = registry.handler(CancellableOperationType.QUERY);

        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrows(expected.getClass(),
                () -> await(handler.cancelAsync(UUID.randomUUID().toString())),
                expected.getMessage()
        );

        verify(messagingService, never())
                .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testCancelFinishesWithErrorOnRemoteNode() {
        IllegalArgumentException expected = new IllegalArgumentException("No enum constant");

        // One remote node reports error, other remote node returns FALSE.
        {
            KillHandlerRegistryImpl registry = createRegistry(nodeName -> {
                if (nodeName.equals(node(1).name())) {
                    throw expected;
                }

                return false;
            });

            registry.register(new TestKillHandler(false));
            OperationKillHandler handler = registry.handler(CancellableOperationType.QUERY);

            //noinspection ThrowableNotThrown
            IgniteTestUtils.assertThrows(expected.getClass(),
                    () -> await(handler.cancelAsync(UUID.randomUUID().toString())),
                    expected.getMessage()
            );

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
        }

        // One remote node reports error, other remote node returns TRUE.
        {
            KillHandlerRegistryImpl registry = createRegistry(nodeName -> {
                if (nodeName.equals(node(1).name())) {
                    throw expected;
                }

                return true;
            });

            registry.register(new TestKillHandler(false));
            OperationKillHandler handler = registry.handler(CancellableOperationType.QUERY);

            // The remote error must be logged, but since the
            // operation is cancelled, the future completes successfully.
            assertThat(handler.cancelAsync(UUID.randomUUID().toString()), willBe(true));

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
        }
    }

    @Test
    public void testResponseTimeout() {
        // One remote node reports error, other remote node returns FALSE.
        {
            Function<String, Boolean> respHandler = nodeName -> {
                if (nodeName.equals(node(1).name())) {
                    try {
                        Thread.sleep(2_000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                return false;
            };

            KillHandlerRegistryImpl registry = createRegistry(respHandler, 200);

            registry.register(new TestKillHandler(false));
            OperationKillHandler handler = registry.handler(CancellableOperationType.QUERY);

            //noinspection ThrowableNotThrown
            IgniteTestUtils.assertThrows(TimeoutException.class,
                    () -> await(handler.cancelAsync(UUID.randomUUID().toString())),
                    null
            );

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
        }

        // One remote node reports error, other remote node returns TRUE.
        {
            Function<String, Boolean> respHandler = nodeName -> {
                if (nodeName.equals(node(1).name())) {
                    try {
                        Thread.sleep(2_000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                return true;
            };

            KillHandlerRegistryImpl registry = createRegistry(respHandler, 200);

            registry.register(new TestKillHandler(false));
            OperationKillHandler handler = registry.handler(CancellableOperationType.QUERY);

            // The remote error must be logged, but since the
            // operation is cancelled, the future completes successfully.
            assertThat(handler.cancelAsync(UUID.randomUUID().toString()), willBe(true));

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
        }
    }

    private KillHandlerRegistryImpl createRegistry(Function<String, Boolean> respHnd) {
        return createRegistry(respHnd, DEFAULT_TIMEOUT);
    }

    private KillHandlerRegistryImpl createRegistry(Function<String, Boolean> respHnd, long timeout) {
        ClusterNode initiator = node(0);

        messagingService = mock(MessagingService.class);
        TopologyService topologyService = mock(TopologyService.class);

        when(messagingService.invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong()))
                .thenAnswer(invocation -> {
                    return CompletableFuture.supplyAsync(() -> {
                        String nodeName = ((ClusterNode) invocation.getArgument(0)).name();

                        assertThat("Initiator shouldn't send messages to himself",
                                nodeName, not(Matchers.equalTo(initiator.name())));

                        try {
                            return MESSAGES_FACTORY.cancelOperationResponse().result(respHnd.apply(nodeName)).build();
                        } catch (Throwable t) {
                            return MESSAGES_FACTORY.cancelOperationResponse().error(t).build();
                        }
                    }).orTimeout(timeout, TimeUnit.MILLISECONDS);
                });

        when(topologyService.localMember()).thenReturn(initiator);
        when(topologyService.allMembers()).thenReturn(TOPOLOGY);

        return new KillHandlerRegistryImpl(topologyService, messagingService);
    }

    private static ClusterNode node(int idx) {
        return TOPOLOGY.get(idx);
    }

    private static LogicalNode newNode(int idx, String name) {
        return new LogicalNode(new UUID(0, idx), name, new NetworkAddress("localhost", 123));
    }

    private static class TestKillHandler implements OperationKillHandler {
        private final @Nullable Boolean localResult;
        private final @Nullable Throwable throwable;
        private final CancellableOperationType type;
        private final boolean local;

        private TestKillHandler(Boolean localResult) {
            this(localResult, null, true, CancellableOperationType.QUERY);
        }

        private TestKillHandler(
                @Nullable Boolean localResult,
                @Nullable Throwable t,
                boolean local,
                CancellableOperationType type
        ) {
            this.localResult = localResult;
            this.throwable = t;
            this.type = type;
            this.local = local;
        }

        @Override
        public boolean local() {
            return local;
        }

        @Override
        public CancellableOperationType type() {
            return type;
        }

        @Override
        public CompletableFuture<Boolean> cancelAsync(String ignored) {
            if (throwable == null) {
                return CompletableFuture.completedFuture(localResult);
            }

            return CompletableFuture.failedFuture(throwable);
        }
    }
}
