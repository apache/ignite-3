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
import static org.hamcrest.Matchers.instanceOf;
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
import org.apache.ignite.internal.sql.common.cancel.api.CancelableOperationType;
import org.apache.ignite.internal.sql.common.cancel.api.ClusterWideOperationCancelHandler;
import org.apache.ignite.internal.sql.common.cancel.api.NodeOperationCancelHandler;
import org.apache.ignite.internal.sql.common.cancel.api.OperationCancelHandler;
import org.apache.ignite.internal.sql.common.cancel.messages.CancelOperationRequest;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.util.CompletableFutures;
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
            new LogicalNode(nodeId(1), "node1", new NetworkAddress("localhost", 123)),
            new LogicalNode(nodeId(2), "node2", new NetworkAddress("localhost", 123)),
            new LogicalNode(nodeId(3), "node3", new NetworkAddress("localhost", 123))
    );

    private MessagingService messagingService;

    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testRegistration() {
        CancelHandlerRegistryImpl manager = createManager(nodeName -> null);

        ClusterWideOperationCancelHandler clusterWideHandler = mock(ClusterWideOperationCancelHandler.class);
        NodeOperationCancelHandler nodeHandler = mock(NodeOperationCancelHandler.class);

        manager.register(clusterWideHandler, CancelableOperationType.SQL_QUERY);

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> manager.register(nodeHandler, CancelableOperationType.SQL_QUERY),
                "A handler for the specified type has already been registered "
        );

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> manager.register(clusterWideHandler, CancelableOperationType.SQL_QUERY),
                "A handler for the specified type has already been registered "
        );

        manager.register(nodeHandler, CancelableOperationType.TRANSACTION);

        assertThat(manager.handler(CancelableOperationType.SQL_QUERY), is(clusterWideHandler));
        assertThat(manager.handler(CancelableOperationType.TRANSACTION), instanceOf(ClusterWideOperationCancelHandler.class));

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> manager.handler(CancelableOperationType.COMPUTE_JOB),
                "No registered handler [type=COMPUTE_JOB]."
        );
    }

    @Test
    @SuppressWarnings({"ThrowableNotThrown", "DataFlowIssue"})
    public void testRegistrationWithInvalidParams() {
        CancelHandlerRegistryImpl manager = createManager(nodeName -> null);

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> manager.register(null, CancelableOperationType.SQL_QUERY),
                "handler"
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> manager.register(v -> null, null),
                "type"
        );

        UnsupportedHandler unsupportedHandler = new UnsupportedHandler();

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> manager.register(unsupportedHandler, CancelableOperationType.SQL_QUERY),
                "Unsupported handler type [cls=" + unsupportedHandler.getClass().getName() + "]."
        );
    }

    @Test
    public void testSuccessfulCancelOnLocalDoesNotProducesNetworkRequests() {
        CancelHandlerRegistryImpl manager = createManager(nodeName -> {
            throw new IllegalStateException("Shouldn't be called");
        });

        manager.register(new LocalHandler(true), CancelableOperationType.SQL_QUERY);
        ClusterWideOperationCancelHandler handler = manager.handler(CancelableOperationType.SQL_QUERY);

        assertThat(await(handler.cancelAsync(UUID.randomUUID())), is(true));

        verify(messagingService, never())
                .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testSuccessfulCancelOnRemote() {
        CancelHandlerRegistryImpl manager = createManager(nodeName -> nodeName.equals(node(2).name()));

        manager.register(new LocalHandler(false), CancelableOperationType.SQL_QUERY);
        ClusterWideOperationCancelHandler handler = manager.handler(CancelableOperationType.SQL_QUERY);

        assertThat(handler.cancelAsync(UUID.randomUUID()), willBe(true));

        verify(messagingService, times(TOPOLOGY.size() - 1))
                .invoke(any(ClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testCancelFinishesWithErrorOnLocalNode() {
        NullPointerException expected = new NullPointerException("Expected");

        CancelHandlerRegistryImpl manager = createManager(nodeName -> true);
        manager.register(new LocalHandler(expected), CancelableOperationType.SQL_QUERY);
        ClusterWideOperationCancelHandler handler = manager.handler(CancelableOperationType.SQL_QUERY);

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

            manager.register(new LocalHandler(false), CancelableOperationType.SQL_QUERY);
            ClusterWideOperationCancelHandler handler = manager.handler(CancelableOperationType.SQL_QUERY);

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

            manager.register(new LocalHandler(false), CancelableOperationType.SQL_QUERY);
            ClusterWideOperationCancelHandler handler = manager.handler(CancelableOperationType.SQL_QUERY);

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

            manager.register(new LocalHandler(false), CancelableOperationType.SQL_QUERY);
            ClusterWideOperationCancelHandler handler = manager.handler(CancelableOperationType.SQL_QUERY);

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

            manager.register(new LocalHandler(false), CancelableOperationType.SQL_QUERY);
            ClusterWideOperationCancelHandler handler = manager.handler(CancelableOperationType.SQL_QUERY);

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
                            return MESSAGES_FACTORY.cancelOperationResponse().throwable(t).build();
                        }
                    }).orTimeout(timeout, TimeUnit.MILLISECONDS);
                });

        when(topologyService.localMember()).thenReturn(initiator);
        when(topologyService.allMembers()).thenReturn(TOPOLOGY);

        return new CancelHandlerRegistryImpl(topologyService, messagingService);
    }

    private static UUID nodeId(int id) {
        return new UUID(0, id);
    }

    private static ClusterNode node(int idx) {
        return TOPOLOGY.get(idx);
    }

    private static class LocalHandler implements NodeOperationCancelHandler {
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

    private static class UnsupportedHandler implements OperationCancelHandler {
        @Override
        public CompletableFuture<Boolean> cancelAsync(UUID objectId) {
            return CompletableFutures.falseCompletedFuture();
        }
    }
}
