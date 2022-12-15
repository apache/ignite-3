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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER;

import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.ignite.internal.sql.engine.AsyncCursor.BatchedResult;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExchangeServiceImpl;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistryImpl;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.framework.TestBuilders;
import org.apache.ignite.internal.sql.engine.message.MessageService;
import org.apache.ignite.internal.sql.engine.message.MessageServiceImpl;
import org.apache.ignite.internal.sql.engine.metadata.FragmentDescription;
import org.apache.ignite.internal.sql.engine.trait.AllNodes;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests to verify Outbox to Inbox interoperation.
 */
public class ExchangeExecutionTest extends AbstractExecutionTest {
    private static final String NODE_NAME = "N1";
    private static final ClusterNode LOCAL_NODE =
            new ClusterNode(NODE_NAME, NODE_NAME, NetworkAddress.from("127.0.0.1:10001"));
    private static final int SOURCE_FRAGMENT_ID = 0;
    private static final int TARGET_FRAGMENT_ID = 1;

    @ParameterizedTest(name = "rowCount={0}, prefetch={1}, ordered={1}")
    @MethodSource("testArgs")
    public void test(int rowCount, boolean prefetch, boolean ordered) {
        UUID queryId = UUID.randomUUID();

        MailboxRegistry mailboxRegistry = new MailboxRegistryImpl();
        ExchangeService exchangeService = createExchangeService(mailboxRegistry);

        Outbox<?> outbox = createSourceFragment(
                queryId,
                exchangeService,
                mailboxRegistry,
                rowCount
        );

        if (prefetch) {
            await(outbox.context().submit(outbox::prefetch, outbox::onError));
        }

        AsyncRootNode<Object[], Object[]> root = createRootFragment(
                queryId,
                ordered,
                exchangeService,
                mailboxRegistry
        );

        BatchedResult<Object[]> res = await(root.requestNextAsync(rowCount));

        assertEquals(rowCount, res.items().size());
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

                // more that count of so called "in-flight" batches. In flight batches
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

    private AsyncRootNode<Object[], Object[]> createRootFragment(
            UUID queryId,
            boolean ordered,
            ExchangeService exchangeService,
            MailboxRegistry mailboxRegistry
    ) {
        ExecutionContext<Object[]> targetCtx = TestBuilders.executionContext()
                .queryId(queryId)
                .executor(taskExecutor)
                .fragment(new FragmentDescription(TARGET_FRAGMENT_ID, null, null, Long2ObjectMaps.emptyMap()))
                .localNode(LOCAL_NODE)
                .build();

        Comparator<Object[]> comparator = ordered ? Comparator.comparingInt(o -> (Integer) o[0]) : null;

        var inbox = new Inbox<>(
                targetCtx, exchangeService, mailboxRegistry, List.of(NODE_NAME), comparator, SOURCE_FRAGMENT_ID, SOURCE_FRAGMENT_ID
        );

        mailboxRegistry.register(inbox);

        var root = new AsyncRootNode<>(
                inbox, Function.identity()
        );

        inbox.onRegister(root);

        return root;
    }

    private Outbox<?> createSourceFragment(
            UUID queryId,
            ExchangeService exchangeService,
            MailboxRegistry mailboxRegistry,
            int rowCount
    ) {
        ExecutionContext<Object[]> sourceCtx = TestBuilders.executionContext()
                .queryId(queryId)
                .executor(taskExecutor)
                .fragment(new FragmentDescription(SOURCE_FRAGMENT_ID, null, null, Long2ObjectMaps.emptyMap()))
                .localNode(LOCAL_NODE)
                .build();

        var outbox = new Outbox<>(
                sourceCtx, exchangeService, mailboxRegistry, SOURCE_FRAGMENT_ID, TARGET_FRAGMENT_ID, new AllNodes<>(List.of(NODE_NAME))
        );
        mailboxRegistry.register(outbox);

        var source = new ScanNode<>(sourceCtx, DataProvider.fromRow(new Object[]{1, 1}, rowCount));

        outbox.register(source);

        return outbox;
    }

    private ExchangeService createExchangeService(MailboxRegistry mailboxRegistry) {
        ClusterService clusterService = TestBuilders.clusterService(NODE_NAME);

        MessageService messageService = new MessageServiceImpl(
                clusterService.topologyService(),
                clusterService.messagingService(),
                taskExecutor,
                new IgniteSpinBusyLock()
        );

        ExchangeService exchangeService = new ExchangeServiceImpl(
                LOCAL_NODE,
                taskExecutor,
                mailboxRegistry,
                messageService
        );

        messageService.start();
        exchangeService.start();

        return exchangeService;
    }
}
