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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.ScannableDataSource;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.internal.util.AsyncCursor.BatchedResult;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify {@link AsyncRootNode}.
 */
@SuppressWarnings("NumericCastThatLosesPrecision")
class AsyncRootNodeTest extends AbstractExecutionTest<RowWrapper> {
    private static final StructNativeType SINGLE_INT_ROW_SCHEMA = NativeTypes.rowBuilder()
            .addField("C1", NativeTypes.INT32, true)
            .build();

    private static final BinaryTupleSchema SINGLE_INT_SCHEMA = BinaryTupleSchema.create(
            new Element[]{new Element(NativeTypes.INT32, false)}
    );

    @Test
    void ensurePrefetchFutureIsNotCompletedByRequestedOfBatch() throws InterruptedException {
        var publisherRequestedLatch = new CountDownLatch(1);
        ExecutionContext<RowWrapper> context = executionContext();

        var dataSourceScanNode = new DataSourceScanNode<>(
                context,
                context.rowFactoryFactory().create(SINGLE_INT_ROW_SCHEMA),
                SINGLE_INT_SCHEMA,
                () -> subscriber -> publisherRequestedLatch.countDown(),
                null,
                null,
                null
        );

        var rootNode = new AsyncRootNode<>(dataSourceScanNode, Function.identity());

        rootNode.requestNextAsync(1);

        publisherRequestedLatch.await(1, TimeUnit.SECONDS);

        var holder = new AtomicReference<CompletableFuture<?>>();

        await(context.submit(() -> holder.set(rootNode.startPrefetch()), err -> {}));

        CompletableFuture<?> prefetchFuture = holder.get();
        assertNotNull(prefetchFuture);
        assertFalse(prefetchFuture.isDone());

        await(rootNode.closeAsync());
    }

    /**
     * Test to make sure root node won't return false-positive result in {@link BatchedResult#hasMore()}.
     *
     * <p>Such problem may arise when incoming request drains the internal buffer of the node empty, while source node has no more data
     * left yet {@link Downstream#end()} has not been called.
     *
     * <p>Test below is a simplified reproducer: data source has exactly the same number of rows that the size of the buffer.
     *
     * <p>Another scenario may involve one exchange: one remote should fulfill the demand with all the rows it has, while another remote
     * doesn't have data at all, but batch message from that remote is delayed.
     */
    @Test
    void ensureNodeWontReturnFalsePositiveHasMoreFlag() {
        ExecutionContext<RowWrapper> context = executionContext();
        TestDataSource dataSource = new TestDataSource();

        DataSourceScanNode<RowWrapper> dataSourceScanNode = new DataSourceScanNode<>(
                context,
                context.rowFactoryFactory().create(SINGLE_INT_ROW_SCHEMA),
                SINGLE_INT_SCHEMA,
                dataSource,
                null,
                null,
                null
        );

        AsyncRootNode<RowWrapper, RowWrapper> rootNode = new AsyncRootNode<>(dataSourceScanNode, Function.identity());
        dataSourceScanNode.onRegister(rootNode);

        // trigger prefetch
        await(context.submit(rootNode::startPrefetch, err -> {}));

        // wait for datasource to emit rows
        await(dataSource.demandFulfilled);

        int requested = (int) dataSource.wasRequested.get();

        // request the same amount to empty the buffer of RootNode
        CompletableFuture<BatchedResult<RowWrapper>> result = rootNode.requestNextAsync(requested);

        try {
            result.get(1, TimeUnit.SECONDS);

            fail("Future should not be completed, because it must wait for TestDataSource#endDelay to be triggered");
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TimeoutException ignored) {
            // this is expected
        }

        dataSource.endDelay.complete(null);

        BatchedResult<?> batch = await(result);
        assertFalse(batch.hasMore());

        await(rootNode.closeAsync());
    }

    @Test
    void ensureRootNodeProperlyHandlesConcurrentRequest() {
        var scanNodeLatch = new CountDownLatch(1);
        ExecutionContext<RowWrapper> context = executionContext();

        RowFactory<RowWrapper> factory = context.rowFactoryFactory().create(SINGLE_INT_ROW_SCHEMA);
        ScanNode<RowWrapper> scanNode = new ScanNode<>(context, () -> {
            try {
                scanNodeLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return IntStream.range(0, 76).mapToObj(factory::create).iterator();
        });

        AsyncRootNode<RowWrapper, RowWrapper> rootNode = new AsyncRootNode<>(scanNode, Function.identity());
        scanNode.onRegister(rootNode);

        // trigger prefetch
        context.submit(rootNode::startPrefetch, err -> {});

        CompletableFuture<?> resultFuture1 = rootNode.requestNextAsync(50);
        CompletableFuture<?> resultFuture2 = rootNode.requestNextAsync(50);

        scanNodeLatch.countDown();

        await(resultFuture1);
        await(resultFuture2);
    }

    @Override
    protected RowHandler<RowWrapper> rowHandler() {
        return SqlRowHandler.INSTANCE;
    }

    @Override
    protected RowFactoryFactory<RowWrapper> rowFactoryFactory() {
        return SqlRowHandler.INSTANCE;
    }

    private static class TestDataSource implements ScannableDataSource {
        final AtomicLong wasRequested = new AtomicLong();
        final CompletableFuture<Void> endDelay = new CompletableFuture<>();
        final CompletableFuture<Void> demandFulfilled = new CompletableFuture<>();

        @Override
        public Publisher<InternalTuple> scan() {
            return subscriber -> {
                Subscription subscription = new TestSubscription(subscriber);

                subscriber.onSubscribe(subscription);
            };
        }

        private class TestSubscription implements Subscription {
            private final AtomicBoolean requested = new AtomicBoolean();

            private final Subscriber<? super InternalTuple> subscriber;

            private TestSubscription(Subscriber<? super InternalTuple> subscriber) {
                this.subscriber = subscriber;
            }

            @Override
            public void request(long n) {
                if (requested.compareAndSet(false, true)) {
                    wasRequested.set(n);

                    IntStream.range(0, (int) n)
                            .mapToObj(AsyncRootNodeTest::createTuple)
                            .forEach(subscriber::onNext);

                    demandFulfilled.complete(null);
                } else {
                    endDelay.thenRun(subscriber::onComplete);
                }
            }

            @Override
            public void cancel() {
            }
        }
    }

    private static InternalTuple createTuple(int value) {
        return new BinaryTuple(1, new BinaryTupleBuilder(1, 4)
                .appendInt(value)
                .build()
        );
    }
}
