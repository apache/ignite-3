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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.framework.ClusterServiceFactory;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.trait.AllNodes;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils.PredicateMatcher;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher;
import org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterNodeImpl;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.util.NonReentrantLock;
import org.hamcrest.CustomMatcher;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify Outbox to Inbox interoperation.
 */
public class ExchangeExecutionTest extends AbstractExecutionTest<Object[]> {
    private static final String ROOT_NODE_NAME = "N1";
    private static final String ANOTHER_NODE_NAME = "N2";
    private static final List<String> NODE_NAMES = List.of(ROOT_NODE_NAME, ANOTHER_NODE_NAME);
    private static final ClusterNode ROOT_NODE =
            new ClusterNodeImpl(ROOT_NODE_NAME, ROOT_NODE_NAME, NetworkAddress.from("127.0.0.1:10001"));
    private static final ClusterNode ANOTHER_NODE =
            new ClusterNodeImpl(ANOTHER_NODE_NAME, ANOTHER_NODE_NAME, NetworkAddress.from("127.0.0.1:10002"));
    private static final int SOURCE_FRAGMENT_ID = 0;
    private static final int TARGET_FRAGMENT_ID = 1;
    private static final Comparator<Object[]> COMPARATOR = Comparator.comparingInt(o -> (Integer) o[0]);

    private static final Map<String, QueryTaskExecutor> executors = new HashMap<>();

    public static final CustomMatcher<Object[]> ODD_KEY_MATCHER = new PredicateMatcher<>(e -> ((int) (e[0])) % 2 != 0, "odd key");

    public static final CustomMatcher<Object[]> EVEN_KEY_MATCHER = new PredicateMatcher<>(e -> ((int) (e[0])) % 2 == 0, "even key");

    private final Map<String, MailboxRegistry> mailboxes = new HashMap<>();
    private final Map<String, ExchangeService> exchangeServices = new HashMap<>();
    private final ClusterServiceFactory serviceFactory = TestBuilders.clusterServiceFactory(List.of(ROOT_NODE_NAME, ANOTHER_NODE_NAME));

    @AfterAll
    static void tearDown() {
        executors.values().forEach(exec -> {
            try {
                exec.stop();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        executors.clear();
    }

    @ParameterizedTest(name = "rowCount={0}, prefetch={1}, ordered={2}")
    @MethodSource("testArgs")
    public void test(int rowCount, boolean prefetch, boolean ordered) {
        UUID queryId = UUID.randomUUID();

        List<Outbox<?>> sourceFragments = new ArrayList<>();

        int idx = 0;
        for (ClusterNode node : List.of(ROOT_NODE, ANOTHER_NODE)) {
            Outbox<?> outbox = createSourceFragment(
                    queryId,
                    node,
                    serviceFactory,
                    DataProvider.fromRow(new Object[]{idx, idx}, rowCount)
            );
            idx++;

            sourceFragments.add(outbox);
        }

        if (prefetch) {
            for (Outbox<?> outbox : sourceFragments) {
                await(outbox.context().submit(outbox::prefetch, outbox::onError));
            }
        }

        AsyncRootNode<Object[], Object[]> root = createRootFragment(
                queryId,
                -1,
                ROOT_NODE,
                NODE_NAMES,
                ordered,
                serviceFactory
        );

        int expectedRowCount = NODE_NAMES.size() * rowCount;

        BatchedResult<Object[]> res = await(root.requestNextAsync(expectedRowCount));

        assertEquals(expectedRowCount, res.items().size());

        if (ordered) {
            List<Object[]> expected = new ArrayList<>(res.items());
            expected.sort(COMPARATOR);

            assertEquals(expected, res.items());
        }
    }

    private static Stream<Arguments> testArgs() {
        List<Integer> sizes = List.of(
                // half of the batch size
                Math.min(Commons.IO_BATCH_SIZE / 2, 1),

                // full batch
                Commons.IO_BATCH_SIZE,

                // full batch + one extra row
                Commons.IO_BATCH_SIZE + 1,

                // several batches
                2 * Commons.IO_BATCH_SIZE + 1,

                // more than count of so called "in-flight" batches. In flight batches
                // are batches that have been sent but not yet acknowledged
                2 * Commons.IO_BATCH_SIZE * Commons.IO_BATCH_COUNT
        );

        List<Boolean> trueFalseList = List.of(true, false);

        List<Arguments> args = new ArrayList<>(2 * sizes.size());
        for (int size : sizes) {
            for (boolean prefetch : trueFalseList) {
                for (boolean ordered : trueFalseList) {
                    args.add(Arguments.of(size, prefetch, ordered));
                }
            }
        }

        return args.stream();
    }

    /**
     * This test verifies that races between batches from previous request and subtree rewinding are
     * handled properly.
     *
     * <p>Depending on {@link Commons#IO_BATCH_COUNT} inbox may request more than one batch. Lets assume
     * the following sequence of events:
     * <pre>
     *                          Inbox                                   Outbox
     *                            |--(1)---- request 4 batches ---------->|--[ at this point we schedule
     *                            |                                       |  [ a task to the executor
     *   1st batch may trigger ]--|<---------- send 1st batch -------(2)--|
     *   rewind for subtree    ]  |                                       |
     *                            |--(3)---- request next 4 batches ----->|--[ the task is added to the
     *                            |             with new context          |  [ queue, the first task is still
     *                            |                                       |  [ in progress
     *       all these batches ]--|<-------- send rest of batches ---(4)--|
     *      have to be ignored ]               from 1st request
     *
     * </pre>
     *
     * <p>The test verifies that batches from diagram above are ignored.
     */
    @Test
    public void racesBetweenRewindAndBatchesFromPreviousRequest() throws InterruptedException {
        UUID queryId = UUID.randomUUID();
        String dataNode1Name = "DATA_NODE_1";
        String dataNode2Name = "DATA_NODE_2";

        ClusterServiceFactory serviceFactory = TestBuilders.clusterServiceFactory(List.of(ROOT_NODE_NAME, dataNode1Name, dataNode2Name));

        TestDataProvider node1DataProvider = new TestDataProvider(3);
        ClusterNode dataNode1 = new ClusterNodeImpl(dataNode1Name, dataNode1Name, NetworkAddress.from("127.0.0.1:10001"));
        createSourceFragment(
                queryId,
                dataNode1,
                serviceFactory,
                node1DataProvider
        );

        TestDataProvider node2DataProvider = new TestDataProvider(3);
        ClusterNode dataNode2 = new ClusterNodeImpl(dataNode2Name, dataNode2Name, NetworkAddress.from("127.0.0.1:10002"));
        createSourceFragment(
                queryId,
                dataNode2,
                serviceFactory,
                node2DataProvider
        );

        RewindableAsyncRoot<Object[], Object[]> root = createRootFragment(
                queryId,
                1,
                ROOT_NODE,
                List.of(dataNode1Name, dataNode2Name),
                false,
                serviceFactory
        );

        try {
            // slow down one of the node for a while to make another to fulfil the query
            // and trigger the rewind
            node2DataProvider.pause();

            // this is arrow 1 from the sequence
            BatchedResult<Object[]> res = await(root.requestNextAsync(2));

            // We have to make sure that cursor was opened on node 2 before we proceed with test scenario.
            // Otherwise there is a chance that rewind will outran scan task spawned by first request message.
            // Problematic task sequence looks like this:
            //      [taskId: 1, request N rows on Node 2]
            //      [taskId: 2, rewind and request another N rows on Node2]
            //      [taskId: 3, <spawned by taskId: 1> scan the iterator and emit rows on Node 2]
            //
            // This is not a problem for general query execution since rewind will clean up state and drop
            // all collected rows so far, but TestDataProvider mutates its state on every invoke of `iterator()`
            // method, and this test verify certain invariants relates to this mutation
            assertTrue(waitForCondition(() -> node2DataProvider.awaitingResume() > 0, 1_000));

            assertThat(res.items(), hasSize(1));
            assertThat(res.items().get(0), equalTo(new Object[]{1, 0}));

            // now slow down another node because we don't want it to send valid
            // batch as response to rewind
            node1DataProvider.pause();

            // this is arrow 3 from the sequence
            await(root.rewind());

            // and here is arrow 4
            node2DataProvider.resume();

            res = await(root.requestNextAsync(2));

            assertThat(res.items(), hasSize(1));
            // expected value here is 10 because that is how DataProvider is implemented:
            // after every rewind the value is multiplied by 10
            assertThat(res.items().get(0), equalTo(new Object[]{1, 1}));
        } finally {
            node1DataProvider.resume();
            node2DataProvider.resume();
        }
    }

    /**
     * The test checks requests with correlates from different downstreams are handled properly.
     */
    @Test
    public void outboxRewindability() {
        UUID queryId = UUID.randomUUID();
        String dataNodeName = "DATA_NODE";

        ClusterServiceFactory serviceFactory = TestBuilders.clusterServiceFactory(List.of(ROOT_NODE_NAME, ANOTHER_NODE_NAME, dataNodeName));

        TestDataProvider nodeDataProvider = new TestDataProvider(1200);
        ClusterNode dataNode = new ClusterNodeImpl(dataNodeName, dataNodeName, NetworkAddress.from("127.0.0.1:10001"));

        createSourceFragmentMultiTarget(
                queryId,
                dataNode,
                serviceFactory,
                nodeDataProvider
        );

        RewindableAsyncRoot<Object[], Object[]> root1 = createRootFragment(
                queryId,
                0,
                ROOT_NODE,
                List.of(dataNodeName),
                false,
                serviceFactory
        );

        RewindableAsyncRoot<Object[], Object[]> root2 = createRootFragment(
                queryId,
                0,
                ANOTHER_NODE,
                List.of(dataNodeName),
                false,
                serviceFactory
        );

        // Request from root1 with shared state.
        await(root1.rewind());
        BatchedResult<Object[]> res = await(root1.requestNextAsync(300));

        assertThat(res.items(), hasSize(300));
        assertThat(res.items().get(0), equalTo(new Object[]{1, 0}));

        // Rewind root1.
        await(root1.rewind());
        res = await(root1.requestNextAsync(300));

        assertThat(res.items(), hasSize(300));
        assertThat(res.items().get(0), equalTo(new Object[]{1, 1}));

        // Continue from root1.
        res = await(root1.requestNextAsync(500));

        assertThat(res.items(), hasSize(300));
        assertThat(res.items().get(0), equalTo(new Object[]{601, 1}));

        // Request from root2 with shared state.
        await(root2.rewind());
        res = await(root2.requestNextAsync(300));

        assertThat(res.items(), hasSize(300));
        assertThat(res.items().get(0), equalTo(new Object[]{2, 2}));
    }

    /**
     * This test verifies that races between batches from previous requests and rewinds are handled properly,
     * when requested from different downstreams.
     */
    @Test
    public void racesBetweenRewindAndBatchesRequestWithCorrelates() {
        UUID queryId = UUID.randomUUID();
        String dataNodeName = "DATA_NODE";

        ClusterServiceFactory serviceFactory = TestBuilders.clusterServiceFactory(List.of(ROOT_NODE_NAME, ANOTHER_NODE_NAME, dataNodeName));

        TestDataProvider nodeDataProvider = new TestDataProvider(8000);
        ClusterNode dataNode = new ClusterNodeImpl(dataNodeName, dataNodeName, NetworkAddress.from("127.0.0.1:10001"));

        createSourceFragmentMultiTarget(
                queryId,
                dataNode,
                serviceFactory,
                nodeDataProvider
        );

        RewindableAsyncRoot<Object[], Object[]> root = createRootFragment(
                queryId,
                0,
                ROOT_NODE,
                List.of(dataNodeName),
                false,
                serviceFactory
        );

        RewindableAsyncRoot<Object[], Object[]> root2 = createRootFragment(
                queryId,
                0,
                ANOTHER_NODE,
                List.of(dataNodeName),
                false,
                serviceFactory
        );

        // Here, the both Root1 and Root2 nodes are "root" nodes just for simplicity. They represents the same physical plan node
        // with 'hash' distribution and they are parts of the same execution tree residing on different nodes.
        // In real world, these nodes will have a common real root node.
        //
        // We must fetch all either the available data from a root, or rewind from a root.
        // Otherwise, it leads to both root1ReqFut and root2ReqFut futures hanging, because one of roots stops requesting next batches,
        // while another root is waiting for rewind infinitely.
        // Seems, the only case when hanging is possible - the downstream contains e.g. a LimitNode and the limit has been hit, and
        // the query is about to finish.

        // Force root1 put correlates into the request.
        await(root.rewind());
        // Root1 starts fetching data.
        BatchedResult<Object[]> res = await(root.requestNextAsync(500));

        assertThat(res.items(), hasSize(500));
        assertThat(res.items().get(0), equalTo(new Object[]{1, 0}));
        assertThat(res.items(), everyItem(ODD_KEY_MATCHER));

        // Late root2 rewind, which forces root2 to put correlates into the request.
        assertThat(root2.rewind(), CompletableFutureMatcher.willSucceedFast());

        // Root1 continues fetching data.
        res = await(root.requestNextAsync(500));

        assertThat(res.items(), hasSize(500));
        assertThat(res.items().get(0), equalTo(new Object[]{1001, 0}));
        assertThat(res.items(), everyItem(ODD_KEY_MATCHER));

        // Root2 request should wait for rewind.
        CompletableFuture<BatchedResult<Object[]>> root2ReqFut = root2.requestNextAsync(500);
        assertThat(root2ReqFut, CompletableFutureExceptionMatcher.willTimeoutFast());

        // Root1 continues fetching the data.
        res = await(root.requestNextAsync(500));
        assertThat(res.items(), hasSize(500));
        assertThat(res.items().get(0), equalTo(new Object[]{2001, 0}));
        assertThat(res.items(), everyItem(ODD_KEY_MATCHER));

        // Rewind root1
        assertFalse(root2ReqFut.isDone());
        await(root.rewind());

        // Root1 request enqueued wait for rewind, root2 become active.
        CompletableFuture<BatchedResult<Object[]>> root1ReqFut = root.requestNextAsync(500);
        assertThat(root1ReqFut, CompletableFutureExceptionMatcher.willTimeoutFast());

        // Root2 can fetch the data.
        res = await(root2ReqFut);
        assertThat(res.items(), hasSize(500));
        assertThat(res.items().get(0), equalTo(new Object[]{2, 1}));
        assertThat(res.items(), everyItem(EVEN_KEY_MATCHER));

        // Root1 is still waiting for rewind.
        assertFalse(root1ReqFut.isDone());

        // Fetching all the data from root2.
        while (res.hasMore()) {
            res = await(root2.requestNextAsync(500));
            assertThat(res.items(), everyItem(EVEN_KEY_MATCHER));
        }

        // Now, root1 can fetch data.
        res = await(root1ReqFut);
        assertThat(res.items(), hasSize(500));
        assertThat(res.items().get(0), equalTo(new Object[]{1, 2}));
        assertThat(res.items(), everyItem(ODD_KEY_MATCHER));
    }

    private RewindableAsyncRoot<Object[], Object[]> createRootFragment(
            UUID queryId,
            int limit,
            ClusterNode localNode,
            List<String> sourceNodeNames,
            boolean ordered,
            ClusterServiceFactory serviceFactory
    ) {
        QueryTaskExecutor taskExecutor = getOrCreateTaskExecutor(localNode.name());

        ExecutionContext<Object[]> targetCtx = TestBuilders.executionContext()
                .queryId(queryId)
                .executor(taskExecutor)
                .fragment(new FragmentDescription(TARGET_FRAGMENT_ID, true, Long2ObjectMaps.emptyMap(), null, null))
                .localNode(localNode)
                .build();

        Comparator<Object[]> comparator = ordered ? COMPARATOR : null;

        MailboxRegistry mailboxRegistry = mailboxes.computeIfAbsent(localNode.name(), name -> new MailboxRegistryImpl());
        ExchangeService exchangeService = exchangeServices.computeIfAbsent(localNode.name(), name ->
                createExchangeService(taskExecutor, serviceFactory.forNode(localNode.name()), mailboxRegistry));

        Inbox<Object[]> inbox = new Inbox<>(
                targetCtx, exchangeService, mailboxRegistry, sourceNodeNames, comparator, rowFactory(),
                SOURCE_FRAGMENT_ID, SOURCE_FRAGMENT_ID
        );

        mailboxRegistry.register(inbox);

        AbstractNode<Object[]> node = inbox;

        if (limit > 0) {
            node = new LimitNode<>(targetCtx, () -> 0, () -> 1);
            node.register(List.of(inbox));
        }

        RewindableAsyncRoot<Object[], Object[]> root = new RewindableAsyncRoot<>(
                node, Function.identity()
        );

        node.onRegister(root);

        return root;
    }

    private Outbox<?> createSourceFragment(
            UUID queryId,
            ClusterNode localNode,
            ClusterServiceFactory serviceFactory,
            DataProvider<Object[]> dataProvider
    ) {
        QueryTaskExecutor taskExecutor = getOrCreateTaskExecutor(localNode.name());

        ExecutionContext<Object[]> sourceCtx = TestBuilders.executionContext()
                .queryId(queryId)
                .executor(taskExecutor)
                .fragment(new FragmentDescription(SOURCE_FRAGMENT_ID, true, Long2ObjectMaps.emptyMap(), null, null))
                .localNode(localNode)
                .build();

        MailboxRegistry mailboxRegistry = mailboxes.computeIfAbsent(localNode.name(), name -> new MailboxRegistryImpl());
        ExchangeService exchangeService = exchangeServices.computeIfAbsent(localNode.name(), name ->
                createExchangeService(taskExecutor, serviceFactory.forNode(localNode.name()), mailboxRegistry));

        Outbox<Object[]> outbox = new Outbox<>(
                sourceCtx, exchangeService, mailboxRegistry, SOURCE_FRAGMENT_ID,
                TARGET_FRAGMENT_ID, new AllNodes<>(List.of(ROOT_NODE_NAME))
        );
        mailboxRegistry.register(outbox);

        ScanNode<Object[]> source = new ScanNode<>(sourceCtx, dataProvider);

        outbox.register(source);

        return outbox;
    }

    private Outbox<?> createSourceFragmentMultiTarget(
            UUID queryId,
            ClusterNode localNode,
            ClusterServiceFactory serviceFactory,
            DataProvider<Object[]> dataProvider
    ) {
        QueryTaskExecutor taskExecutor = getOrCreateTaskExecutor(localNode.name());

        ExecutionContext<Object[]> sourceCtx = TestBuilders.executionContext()
                .queryId(queryId)
                .executor(taskExecutor)
                .fragment(new FragmentDescription(SOURCE_FRAGMENT_ID, true, Long2ObjectMaps.emptyMap(), null, null))
                .localNode(localNode)
                .build();

        MailboxRegistry mailboxRegistry = mailboxes.computeIfAbsent(localNode.name(), name -> new MailboxRegistryImpl());
        ExchangeService exchangeService = exchangeServices.computeIfAbsent(localNode.name(), name ->
                createExchangeService(taskExecutor, serviceFactory.forNode(localNode.name()), mailboxRegistry));

        Outbox<Object[]> outbox = new Outbox<>(
                sourceCtx, exchangeService, mailboxRegistry, SOURCE_FRAGMENT_ID,
                TARGET_FRAGMENT_ID,
                new Destination<>() {
                    @Override
                    public List<String> targets(Object[] row) {
                        if ((int) row[0] % 2 != 0) {
                            return List.of(ROOT_NODE_NAME);
                        } else {
                            return List.of(ANOTHER_NODE_NAME);
                        }
                    }

                    @Override
                    public List<String> targets() {
                        return List.of(ROOT_NODE_NAME, ANOTHER_NODE_NAME);
                    }
                }
        );
        mailboxRegistry.register(outbox);

        ScanNode<Object[]> source = new ScanNode<>(sourceCtx, dataProvider);

        outbox.register(source);

        return outbox;
    }

    private static ExchangeService createExchangeService(
            QueryTaskExecutor taskExecutor,
            ClusterService clusterService,
            MailboxRegistry mailboxRegistry
    ) {
        MessageService messageService = new MessageServiceImpl(
                clusterService.topologyService().localMember().name(),
                clusterService.messagingService(),
                taskExecutor,
                new IgniteSpinBusyLock()
        );

        ExchangeService exchangeService = new ExchangeServiceImpl(
                mailboxRegistry,
                messageService
        );

        messageService.start();
        exchangeService.start();

        return exchangeService;
    }

    private static QueryTaskExecutor getOrCreateTaskExecutor(String name) {
        return executors.computeIfAbsent(name, name0 -> {
            var executor = new QueryTaskExecutorImpl(name0);

            executor.start();

            return executor;
        });
    }

    static class RewindableAsyncRoot<InT, OutT> extends AsyncRootNode<InT, OutT> {
        /**
         * Constructor.
         *
         * @param source A source to requests rows from.
         * @param converter A converter to convert rows from an internal format to desired output format.
         */
        RewindableAsyncRoot(AbstractNode<InT> source, Function<InT, OutT> converter) {
            super(source, converter);
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            // do nothing
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<?> rewind() {
            AbstractNode<?> source = (AbstractNode<?>) IgniteTestUtils.getFieldValue(this, AsyncRootNode.class, "source");

            return source.context().submit(
                    () -> {
                        IgniteTestUtils.setFieldValue(this, AsyncRootNode.class, "waiting", 0);
                        IgniteTestUtils.setFieldValue(this, AsyncRootNode.class, "closed", false);

                        ((ArrayDeque<?>) IgniteTestUtils.getFieldValue(this, AsyncRootNode.class, "buff")).clear();

                        source.rewind();
                    },
                    this::onError
            );
        }
    }

    static class TestDataProvider implements DataProvider<Object[]> {
        // Particular implementation is used to have access to the owner
        private final NonReentrantLock lock = new NonReentrantLock();

        private final int times;

        private int rewindCnt;

        TestDataProvider(int times) {
            this.times = times;
        }

        @SuppressWarnings("LockAcquiredButNotSafelyReleased")
        void pause() {
            lock.lock();
        }

        void resume() {
            if (lock.getOwner() != null) {
                lock.unlock();
            }
        }

        /** Returns approximate number of threads awaiting this data provider to be {@link #resume() resumed}. */
        int awaitingResume() {
            return lock.getQueueLength();
        }

        /** {@inheritDoc} */
        @Override
        public Iterator<Object[]> iterator() {
            int value = this.rewindCnt;

            this.rewindCnt++;

            return new Iterator<>() {
                private int counter;

                /** {@inheritDoc} */
                @Override
                public boolean hasNext() {
                    return counter < times;
                }

                /** {@inheritDoc} */
                @Override
                public Object[] next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    lock.lock();

                    int key;
                    try {
                        key = (1 + counter);
                        counter++;
                    } finally {
                        lock.unlock();
                    }

                    return new Object[]{key, value};
                }
            };
        }
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
