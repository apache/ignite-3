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

package org.apache.ignite.internal.sql.common.cancel;

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
import org.apache.ignite.internal.sql.common.cancel.api.CancellableOperationType;
import org.apache.ignite.internal.sql.common.cancel.api.OperationCancelHandler;
import org.apache.ignite.internal.sql.common.cancel.messages.CancelOperationRequest;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link CancelHandlerRegistryImpl}.
 */
public class CancelHandlerRegistryTest extends BaseIgniteAbstractTest {
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
        CancelHandlerRegistryImpl manager = createManager(nodeName -> null);

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> manager.register(CancellableOperationType.SQL_QUERY, null, true),
                "handler"
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> manager.register(null, v -> null, true),
                "type"
        );

        OperationCancelHandler nodeHandler = mock(OperationCancelHandler.class);

        manager.register(CancellableOperationType.SQL_QUERY, nodeHandler, false);

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> manager.register(CancellableOperationType.SQL_QUERY, nodeHandler, true),
                "A handler for the specified type has already been registered "
        );

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> manager.register(CancellableOperationType.SQL_QUERY, nodeHandler, false),
                "A handler for the specified type has already been registered "
        );

        manager.register(CancellableOperationType.TRANSACTION, nodeHandler, true);

        assertThat(manager.handler(CancellableOperationType.SQL_QUERY), is(nodeHandler));
        assertThat(manager.handler(CancellableOperationType.TRANSACTION), is(not(nodeHandler)));

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> manager.handler(CancellableOperationType.COMPUTE_JOB),
                "No registered handler [type=COMPUTE_JOB]."
        );
    }

    @Test
    public void testSuccessfulCancelOnLocalDoesNotProducesNetworkRequests() {
        CancelHandlerRegistryImpl manager = createManager(nodeName -> {
            throw new IllegalStateException("Shouldn't be called");
        });

        manager.register(CancellableOperationType.SQL_QUERY, new LocalHandler(true), true);
        OperationCancelHandler handler = manager.handler(CancellableOperationType.SQL_QUERY);

        assertThat(await(handler.cancelAsync(UUID.randomUUID())), is(true));

        verify(messagingService, never())
                .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testSuccessfulCancelOnRemote() {
        CancelHandlerRegistryImpl manager = createManager(nodeName -> nodeName.equals(node(2).name()));

        manager.register(CancellableOperationType.SQL_QUERY, new LocalHandler(false), true);
        OperationCancelHandler handler = manager.handler(CancellableOperationType.SQL_QUERY);

        assertThat(handler.cancelAsync(UUID.randomUUID()), willBe(true));

        verify(messagingService, times(TOPOLOGY.size() - 1))
                .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testCancelFinishesWithErrorOnLocalNode() {
        NullPointerException expected = new NullPointerException("Expected");

        CancelHandlerRegistryImpl manager = createManager(nodeName -> true);
        manager.register(CancellableOperationType.SQL_QUERY, new LocalHandler(expected), true);
        OperationCancelHandler handler = manager.handler(CancellableOperationType.SQL_QUERY);

        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrows(expected.getClass(),
                () -> await(handler.cancelAsync(UUID.randomUUID())),
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
            CancelHandlerRegistryImpl manager = createManager(nodeName -> {
                if (nodeName.equals(node(1).name())) {
                    throw expected;
                }

                return false;
            });

            manager.register(CancellableOperationType.SQL_QUERY, new LocalHandler(false), true);
            OperationCancelHandler handler = manager.handler(CancellableOperationType.SQL_QUERY);

            //noinspection ThrowableNotThrown
            IgniteTestUtils.assertThrows(expected.getClass(),
                    () -> await(handler.cancelAsync(UUID.randomUUID())),
                    expected.getMessage()
            );

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
        }

        // One remote node reports error, other remote node returns TRUE.
        {
            CancelHandlerRegistryImpl manager = createManager(nodeName -> {
                if (nodeName.equals(node(1).name())) {
                    throw expected;
                }

                return true;
            });

            manager.register(CancellableOperationType.SQL_QUERY, new LocalHandler(false), true);
            OperationCancelHandler handler = manager.handler(CancellableOperationType.SQL_QUERY);

            // The remote error must be logged, but since the
            // operation is cancelled, the future completes successfully.
            assertThat(handler.cancelAsync(UUID.randomUUID()), willBe(true));

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

            CancelHandlerRegistryImpl manager = createManager(respHandler, 200);

            manager.register(CancellableOperationType.SQL_QUERY, new LocalHandler(false), true);
            OperationCancelHandler handler = manager.handler(CancellableOperationType.SQL_QUERY);

            //noinspection ThrowableNotThrown
            IgniteTestUtils.assertThrows(TimeoutException.class,
                    () -> await(handler.cancelAsync(UUID.randomUUID())),
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

            CancelHandlerRegistryImpl manager = createManager(respHandler, 200);

            manager.register(CancellableOperationType.SQL_QUERY, new LocalHandler(false), true);
            OperationCancelHandler handler = manager.handler(CancellableOperationType.SQL_QUERY);

            // The remote error must be logged, but since the
            // operation is cancelled, the future completes successfully.
            assertThat(handler.cancelAsync(UUID.randomUUID()), willBe(true));

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
        }
    }

    private CancelHandlerRegistryImpl createManager(Function<String, Boolean> respHnd) {
        return createManager(respHnd, DEFAULT_TIMEOUT);
    }

    private CancelHandlerRegistryImpl createManager(Function<String, Boolean> respHnd, long timeout) {
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

        return new CancelHandlerRegistryImpl(topologyService, messagingService);
    }

    private static ClusterNode node(int idx) {
        return TOPOLOGY.get(idx);
    }

    private static LogicalNode newNode(int idx, String name) {
        return new LogicalNode(new UUID(0, idx), name, new NetworkAddress("localhost", 123));
    }

    private static class LocalHandler implements OperationCancelHandler {
        private final Boolean localResult;
        private final Throwable throwable;

        private LocalHandler(Boolean localResult) {
            this.localResult = localResult;
            this.throwable = null;
        }

        private LocalHandler(Throwable t) {
            this.localResult = null;
            this.throwable = t;
        }

        @Override
        public CompletableFuture<Boolean> cancelAsync(UUID ignored) {
            if (throwable == null) {
                return CompletableFuture.completedFuture(localResult);
            }

            return CompletableFuture.failedFuture(throwable);
        }
    }
}
