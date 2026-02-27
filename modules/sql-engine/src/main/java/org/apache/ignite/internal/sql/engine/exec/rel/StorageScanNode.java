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

import java.util.List;
import java.util.Queue;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.jetbrains.annotations.Nullable;

/**
 * Base abstract scan node required to encapsulate logic of buffered read of a datasource and push read data to downstream node. In most
 * cases to realize concrete implementation require to implement {@code scan()} method and override {@code rewindInternal()} one.
 */
public abstract class StorageScanNode<RowT> extends AbstractNode<RowT> {
    private Queue<RowT> inBuff = new LinkedBlockingQueue<>(inBufSize);

    private final Predicate<RowT> filters;

    private final @Nullable Function<RowT, RowT> rowTransformer;

    private int requested;

    private int waiting;

    private boolean inLoop;

    private @Nullable Subscription activeSubscription;

    /** Flag that indicate scan method was called already. */
    private boolean dataRequested;

    // Metrics
    private long filteredRows = 0L;
    private long scanStartTime = -1L;
    private long scanTime = 0L;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param filters Optional filter to filter out rows.
     * @param rowTransformer Optional projection function.
     */
    public StorageScanNode(
            ExecutionContext<RowT> ctx,
            @Nullable Predicate<RowT> filters,
            @Nullable Function<RowT, RowT> rowTransformer
    ) {
        super(ctx);

        assert ctx.txAttributes() != null : "Transaction not initialized.";

        this.filters = filters;
        this.rowTransformer = rowTransformer;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;

        onRequestReceived();

        requested = rowsCnt;

        if (!inLoop) {
            this.execute(this::push);
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
        dataRequested = false;

        inBuff = new LinkedBlockingQueue<>(inBufSize);

        if (activeSubscription != null) {
            activeSubscription.cancel();

            activeSubscription = null;
        }
    }

    /**
     *  Scan publisher of datasource.
     *  The method will be invoked just once and should return publisher provide data from a storage. However the method will be invoked
     *  again in case called {@code rewindInternal}.
     *
     *  @return Publisher of datasource.
     */
    protected abstract Publisher<RowT> scan();

    private void push() throws Exception {
        if (requested > 0 && !inBuff.isEmpty()) {
            int processed = 0;
            inLoop = true;
            try {
                while (requested > 0 && !inBuff.isEmpty()) {
                    if (processed++ >= inBufSize) {
                        // Allow others to do their job.
                        execute(this::push);

                        return;
                    }

                    RowT row = inBuff.poll();

                    if (filters != null && !filters.test(row)) {
                        onRowFiltered();
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
                this.execute(this::push);
            }
        }
    }

    private void requestNextBatch() {
        if (waiting == NOT_WAITING) {
            return;
        }
        if (isClosed()) {
            return;
        }

        if (waiting == 0) {
            // we must not request rows more than inBufSize
            waiting = inBufSize - inBuff.size();
        }

        onScanStarted();

        Subscription subscription = this.activeSubscription;
        if (subscription != null) {
            subscription.request(waiting);
        } else if (!dataRequested) {
            scan().subscribe(new SubscriberImpl());

            dataRequested = true;
        } else {
            waiting = NOT_WAITING;
        }
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

    @Override
    protected void dumpDebugInfo0(IgniteStringBuilder buf) {
        buf.app("class=").app(getClass().getSimpleName())
                .app(", requested=").app(requested)
                .app(", waiting=").app(waiting);
    }

    /** Subscriber which handle scan's rows. */
    private class SubscriberImpl implements Flow.Subscriber<RowT> {
        private Queue<RowT> inBuffInner;

        /** {@inheritDoc} */
        @Override
        public void onSubscribe(Subscription subscription) {
            assert StorageScanNode.this.activeSubscription == null;

            inBuffInner = inBuff;

            StorageScanNode.this.activeSubscription = subscription;
            subscription.request(waiting);
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(RowT row) {
            // This method is called from outside query execution thread.
            // It is safe not to be aware about already closed execution flow.
            inBuffInner.add(row);

            int size = inBuffInner.size();
            if (size == inBufSize) {
                StorageScanNode.this.execute(() -> {
                    onRowsReceived(size);
                    onScanFinished();

                    waiting = 0;
                    push();
                });
            }
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable throwable) {
            StorageScanNode.this.execute(() -> {
                throw throwable;
            });
        }

        /** {@inheritDoc} */
        @Override
        public void onComplete() {
            StorageScanNode.this.execute(() -> {
                onRowsReceived(inBuff.size());
                onScanFinished();

                activeSubscription = null;
                waiting = 0;

                push();
            });
        }
    }

    @Override
    protected void dumpMetrics0(IgniteStringBuilder writer) {
        writer.app(this.getClass().getSimpleName()).app(": ");

        writer.app("scannedRows=").app(receivedRowsCount);

        if (requestCount > 0) {
            writer.app(", requests=").app(requestCount);
        }

        if (rewindCount > 0) {
            writer.app(", rewinds=").app(rewindCount);
        }

        if (filteredRows > 0) {
            writer.app(", filteredRows=").app(filteredRows);
        }

        writer.app(", scanTime=").app(MetricsAwareNode.beautifyNanoTime(scanTime));
    }

    private void onRowFiltered() {
        filteredRows++;
    }

    private void onScanStarted() {
        scanStartTime = System.nanoTime();
    }

    private void onScanFinished() {
        scanTime += System.nanoTime() - scanStartTime;
        scanStartTime = -1L;
    }
}
