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

import java.util.BitSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Based abstract scan node.
 */
public abstract class StorageScanNode<RowT> extends AbstractNode<RowT> {
    /** Special value to highlights that all row were received and we are not waiting any more. */
    private static final int NOT_WAITING = -1;

    private final Queue<RowT> inBuff = new LinkedBlockingQueue<>(inBufSize);

    private final @Nullable Predicate<RowT> filters;

    private final @Nullable Function<RowT, RowT> rowTransformer;

    private final Function<BinaryRow, RowT> tableRowConverter;

    private int requested;

    private int waiting;

    private boolean inLoop;

    private Subscription activeSubscription;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param rowFactory Row factory.
     * @param schemaTable The table this node should scan.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     */
    public StorageScanNode(
            ExecutionContext<RowT> ctx,
            RowHandler.RowFactory<RowT> rowFactory,
            InternalIgniteTable schemaTable,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer,
            @Nullable BitSet requiredColumns
    ) {
        super(ctx);

        assert context().transaction() != null || context().transactionTime() != null : "Transaction not initialized.";

        tableRowConverter = row -> schemaTable.toRow(context(), row, rowFactory, requiredColumns);

        this.filters = filters;
        this.rowTransformer = rowTransformer;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            context().execute(this::push, this::onError);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        super.closeInternal();

        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;

        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /**
     *  Publisher of scan node.
     *
     *  @return Publisher of scan node or {@code null} in case nothing to scan.
     */
    protected abstract Publisher<RowT> scan();

    /**
     * Proxy publisher with singe goal convert rows from {@code BinaryRow} to {@code RowT}.
     *
     * @param pub {@code BinaryRow} Publisher.
     *
     * @return Proxy publisher with conversion from {@code BinaryRow} to {@code RowT}.
     */
    @NotNull
    public Publisher<RowT> convertPublisher(Publisher<BinaryRow> pub) {
        Publisher<RowT> convPub = downstream -> {
            // BinaryRow -> RowT converter.
            Subscriber<BinaryRow> subs = new Subscriber<>() {
                @Override
                public void onSubscribe(Subscription subscription) {
                    downstream.onSubscribe(subscription);
                }

                @Override
                public void onNext(BinaryRow item) {
                    downstream.onNext(convert(item));
                }

                @Override
                public void onError(Throwable throwable) {
                    downstream.onError(throwable);
                }

                @Override
                public void onComplete() {
                    downstream.onComplete();
                }
            };

            pub.subscribe(subs);
        };
        return convPub;
    }

    private void push() throws Exception {
        if (isClosed()) {
            return;
        }

        checkState();

        if (requested > 0 && !inBuff.isEmpty()) {
            inLoop = true;
            try {
                while (requested > 0 && !inBuff.isEmpty()) {
                    checkState();

                    RowT row = inBuff.poll();

                    if (filters != null && !filters.test(row)) {
                        continue;
                    }

                    if (rowTransformer != null) {
                        row = rowTransformer.apply(row);
                    }

                    requested--;
                    downstream().push(row);
                }
            } finally {
                inLoop = false;
            }
        }

        if (requested > 0) {
            if (waiting == 0 || activeSubscription == null) {
                requestNextBatch();
            }
        }

        if (requested > 0 && waiting == NOT_WAITING) {
            if (inBuff.isEmpty()) {
                requested = 0;
                downstream().end();
            } else {
                context().execute(this::push, this::onError);
            }
        }
    }

    private void requestNextBatch() {
        if (waiting == NOT_WAITING) {
            return;
        }

        if (waiting == 0) {
            // we must not request rows more than inBufSize
            waiting = inBufSize - inBuff.size();
        }

        Subscription subscription = this.activeSubscription;
        if (subscription != null) {
            subscription.request(waiting);
        } else {
            Publisher<RowT> scan = scan();

            if (scan != null) {
                scan.subscribe(new SubscriberImpl());
            } else {
                waiting = NOT_WAITING;
            }
        }
    }

    /** Subscriber which handle scan's rows. */
    private class SubscriberImpl implements Flow.Subscriber<RowT> {

        /** {@inheritDoc} */
        @Override
        public void onSubscribe(Subscription subscription) {
            assert StorageScanNode.this.activeSubscription == null;

            StorageScanNode.this.activeSubscription = subscription;
            subscription.request(waiting);
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(RowT row) {
            inBuff.add(row);

            if (inBuff.size() == inBufSize) {
                context().execute(() -> {
                    waiting = 0;
                    push();
                }, StorageScanNode.this::onError);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable throwable) {
            context().execute(() -> {
                throw throwable;
            }, StorageScanNode.this::onError);
        }

        /** {@inheritDoc} */
        @Override
        public void onComplete() {
            context().execute(() -> {
                activeSubscription = null;
                waiting = 0;

                push();
            }, StorageScanNode.this::onError);
        }
    }

    /** Convert row from {@code BinaryRow} to internal SQL row format {@code RowT}. */
    private RowT convert(BinaryRow binaryRow) {
        return tableRowConverter.apply(binaryRow);
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<RowT>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }
}
