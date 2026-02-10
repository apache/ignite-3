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

package org.apache.ignite.internal.sql.engine.exec.kill;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.sql.engine.api.kill.CancellableOperationType;
import org.apache.ignite.internal.sql.engine.api.kill.OperationKillHandler;
import org.apache.ignite.internal.sql.engine.message.CancelOperationRequest;
import org.apache.ignite.internal.sql.engine.message.SqlQueryMessagesFactory;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.NetworkAddress;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link KillCommandHandler}.
 */
public class KillCommandHandlerTest extends BaseIgniteAbstractTest {
    private static final SqlQueryMessagesFactory MESSAGES_FACTORY = new SqlQueryMessagesFactory();

    private static final long DEFAULT_TIMEOUT = TimeUnit.SECONDS.toMillis(5);

    private static final List<LogicalNode> TOPOLOGY = List.of(
            newNode(1, "node1"),
            newNode(2, "node2"),
            newNode(3, "node3")
    );

    private MessagingService messagingService;

    @Test
    @SuppressWarnings({"ThrowableNotThrown", "DataFlowIssue"})
    public void testRegistration() {
        KillCommandHandler cmdHandler = createCommandHandler(nodeName -> null);

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> cmdHandler.register(null),
                "handler"
        );

        IgniteTestUtils.assertThrows(
                NullPointerException.class,
                () -> cmdHandler.register(new TestKillHandler(null, null, true, null)),
                "type"
        );

        TestKillHandler nodeQueryKillHandler = new TestKillHandler(null, null, true, CancellableOperationType.QUERY);

        cmdHandler.register(nodeQueryKillHandler);

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> cmdHandler.register(new TestKillHandler(null, null, false, CancellableOperationType.QUERY)),
                "A handler for the specified type has already been registered [type=" + CancellableOperationType.QUERY.name()
        );

        TestKillHandler clusterWideTxKillHandler = new TestKillHandler(null, null, false, CancellableOperationType.TRANSACTION);

        cmdHandler.register(clusterWideTxKillHandler);

        assertThat(cmdHandler.handlerOrThrow(CancellableOperationType.QUERY, false), is(not(nodeQueryKillHandler)));
        assertThat(cmdHandler.handlerOrThrow(CancellableOperationType.TRANSACTION, false), is(clusterWideTxKillHandler));

        IgniteTestUtils.assertThrows(
                IllegalArgumentException.class,
                () -> cmdHandler.handle(newCmd(CancellableOperationType.COMPUTE)),
                "No handler is registered for the specified type "
                        + "[type=" + CancellableOperationType.COMPUTE.name() + ", local=false]."
        );
    }

    @Test
    public void testSuccessfulCancelOnLocalDoesNotProducesNetworkRequests() {
        KillCommandHandler cmdHandler = createCommandHandler(nodeName -> {
            throw new IllegalStateException("Shouldn't be called");
        });

        cmdHandler.register(new TestKillHandler(true));

        KillCommand killCommand = newCmd(CancellableOperationType.QUERY);

        assertThat(cmdHandler.handle(killCommand), willBe(true));

        verify(messagingService, never())
                .invoke(any(InternalClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    private static KillCommand newCmd(CancellableOperationType type) {
        return new KillCommand(UUID.randomUUID().toString(), type, false);
    }

    @Test
    public void testSuccessfulCancelOnRemote() {
        KillCommandHandler cmdHandler = createCommandHandler(nodeName -> nodeName.equals(node(2).name()));

        cmdHandler.register(new TestKillHandler(false));
        KillCommand killCommand = newCmd(CancellableOperationType.QUERY);

        assertThat(cmdHandler.handle(killCommand), willBe(true));

        verify(messagingService, times(TOPOLOGY.size() - 1))
                .invoke(any(InternalClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testCancelFinishesWithErrorOnLocalNode() {
        NullPointerException expected = new NullPointerException("Expected");

        KillCommandHandler cmdHandler = createCommandHandler(nodeName -> true);
        cmdHandler.register(new TestKillHandler(null, expected, true, CancellableOperationType.QUERY));
        KillCommand killCommand = newCmd(CancellableOperationType.QUERY);

        //noinspection ThrowableNotThrown
        IgniteTestUtils.assertThrows(expected.getClass(),
                () -> await(cmdHandler.handle(killCommand)),
                expected.getMessage()
        );

        verify(messagingService, never())
                .invoke(any(InternalClusterNode.class), any(CancelOperationRequest.class), anyLong());
    }

    @Test
    public void testCancelFinishesWithErrorOnRemoteNode() {
        IllegalArgumentException expected1 = new IllegalArgumentException("No enum constant");
        UnsupportedOperationException expected2 = new UnsupportedOperationException("Unsupported operation");

        // Two remote nodes report an error - all errors must be listed in an exception.
        {
            KillCommandHandler cmdHandler = createCommandHandler(nodeName -> {
                if (nodeName.equals(node(1).name())) {
                    throw expected1;
                }

                if (nodeName.equals(node(2).name())) {
                    throw expected2;
                }

                return false;
            });

            cmdHandler.register(new TestKillHandler(false));
            KillCommand killCommand = newCmd(CancellableOperationType.QUERY);

            String remoteErrPrefix = "Remote node returned an error while canceling the operation";

            Throwable err = assertThrowsExactly(IgniteInternalException.class,
                    () -> {
                        try {
                            cmdHandler.handle(killCommand).get(5, TimeUnit.SECONDS);
                        } catch (ExecutionException e) {
                            throw e.getCause();
                        }
                    },
                    remoteErrPrefix
            );

            // Determine which exception was thrown first.
            RuntimeException[] expected = err.getMessage().contains(TOPOLOGY.get(1).name())
                    ? new RuntimeException[]{expected1, expected2}
                    : new RuntimeException[]{expected2, expected1};

            assertThat(err.getCause(), instanceOf(expected[0].getClass()));
            assertThat(err.getCause().getMessage(), equalTo(expected[0].getMessage()));

            assertThat(err.getSuppressed(), arrayWithSize(1));

            Throwable suppressed = err.getSuppressed()[0];

            assertThat(suppressed, instanceOf(IgniteInternalException.class));
            assertThat(suppressed.getMessage(), containsString(remoteErrPrefix));
            assertThat(suppressed.getCause(), instanceOf(expected[1].getClass()));
            assertThat(suppressed.getCause().getMessage(), equalTo(expected[1].getMessage()));

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(InternalClusterNode.class), any(CancelOperationRequest.class), anyLong());
        }

        // One remote node reports error, other remote node returns TRUE.
        {
            KillCommandHandler cmdHandler = createCommandHandler(nodeName -> {
                if (nodeName.equals(node(1).name())) {
                    throw expected1;
                }

                return true;
            });

            cmdHandler.register(new TestKillHandler(false));
            KillCommand killCommand = newCmd(CancellableOperationType.QUERY);

            // The remote error must be logged, but since the
            // operation is cancelled, the future completes successfully.
            assertThat(cmdHandler.handle(killCommand), willBe(true));

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(InternalClusterNode.class), any(CancelOperationRequest.class), anyLong());
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

            KillCommandHandler cmdHandler = createCommandHandler(respHandler, 200);

            cmdHandler.register(new TestKillHandler(false));
            KillCommand killCommand = newCmd(CancellableOperationType.QUERY);

            //noinspection ThrowableNotThrown
            IgniteTestUtils.assertThrows(TimeoutException.class,
                    () -> await(cmdHandler.handle(killCommand)),
                    null
            );

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(InternalClusterNode.class), any(CancelOperationRequest.class), anyLong());
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

            KillCommandHandler cmdHandler = createCommandHandler(respHandler, 200);

            cmdHandler.register(new TestKillHandler(false));
            KillCommand killCommand = newCmd(CancellableOperationType.QUERY);

            // The remote error must be logged, but since the
            // operation is cancelled, the future completes successfully.
            assertThat(cmdHandler.handle(killCommand), willBe(true));

            verify(messagingService, times(TOPOLOGY.size() - 1))
                    .invoke(any(InternalClusterNode.class), any(CancelOperationRequest.class), anyLong());
        }
    }

    private KillCommandHandler createCommandHandler(Function<String, Boolean> respHnd) {
        return createCommandHandler(respHnd, DEFAULT_TIMEOUT);
    }

    private KillCommandHandler createCommandHandler(Function<String, Boolean> respHnd, long timeout) {
        InternalClusterNode initiator = node(0);

        messagingService = mock(MessagingService.class);
        LogicalTopologyService topologyService = mock(LogicalTopologyService.class);

        when(messagingService.invoke(any(InternalClusterNode.class), any(CancelOperationRequest.class), anyLong()))
                .thenAnswer(invocation -> {
                    return CompletableFuture.supplyAsync(() -> {
                        String nodeName = ((InternalClusterNode) invocation.getArgument(0)).name();

                        assertThat("Initiator shouldn't send messages to himself",
                                nodeName, not(Matchers.equalTo(initiator.name())));

                        try {
                            return MESSAGES_FACTORY.cancelOperationResponse().result(respHnd.apply(nodeName)).build();
                        } catch (Throwable t) {
                            return MESSAGES_FACTORY.cancelOperationResponse().error(t).build();
                        }
                    }).orTimeout(timeout, TimeUnit.MILLISECONDS);
                });
        LogicalTopologySnapshot logicalTop = new LogicalTopologySnapshot(1, TOPOLOGY);

        when(topologyService.localLogicalTopology()).thenReturn(logicalTop);

        return new KillCommandHandler(TOPOLOGY.get(0).name(), topologyService, messagingService);
    }

    private static InternalClusterNode node(int idx) {
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
