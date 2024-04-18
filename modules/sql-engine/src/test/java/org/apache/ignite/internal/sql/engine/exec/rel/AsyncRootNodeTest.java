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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.Test;

/**
 * Tests to verify {@link AsyncRootNode}.
 */
class AsyncRootNodeTest extends AbstractExecutionTest<RowWrapper> {
    private static final RowSchema SINGLE_INT_ROW_SCHEMA = RowSchema.builder()
            .addField(NativeTypes.INT32)
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
                rowHandler().factory(SINGLE_INT_ROW_SCHEMA),
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
    }

    @Override
    protected RowHandler<RowWrapper> rowHandler() {
        return SqlRowHandler.INSTANCE;
    }
}
