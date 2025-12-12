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

import static org.apache.calcite.util.Util.unexpected;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.lang.Debuggable;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.partition.replicator.network.replication.BinaryTupleMessage;
import org.apache.ignite.internal.sql.engine.NodeLeftException;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SharedState;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox.RemoteSource.State;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * A part of exchange which receives batches from remote sources.
 */
public class Inbox<RowT> extends AbstractNode<RowT> implements Mailbox<RowT>, SingleNode<RowT> {
    private final ExchangeService exchange;
    private final MailboxRegistry registry;
    private final long exchangeId;
    private final long srcFragmentId;
    private final Collection<String> srcNodeNames;
    private final @Nullable Comparator<RowT> comp;
    private final Map<String, RemoteSource<RowT>> perNodeBuffers;
    private final RowFactory<RowT> rowFactory;

    private @Nullable List<RemoteSource<RowT>> remoteSources;
    private int requested;
    private boolean inLoop;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param exchange Exchange service.
     * @param registry Mailbox registry.
     * @param rowFactory Incoming row factory.
     * @param exchangeId Exchange ID.
     * @param srcFragmentId Source fragment ID.
     */
    public Inbox(
            ExecutionContext<RowT> ctx,
            ExchangeService exchange,
            MailboxRegistry registry,
            Collection<String> srcNodeNames,
            @Nullable Comparator<RowT> comp,
            RowFactory<RowT> rowFactory,
            long exchangeId,
            long srcFragmentId
    ) {
        super(ctx);

        assert !nullOrEmpty(srcNodeNames);

        this.exchange = exchange;
        this.registry = registry;
        this.srcNodeNames = srcNodeNames;
        this.comp = comp;
        this.rowFactory = rowFactory;

        this.srcFragmentId = srcFragmentId;
        this.exchangeId = exchangeId;

        Map<String, RemoteSource<RowT>> sources = new HashMap<>();
        for (String nodeName : srcNodeNames) {
            sources.put(nodeName, new RemoteSource<>((cnt, state) -> requestBatches(nodeName, cnt, state)));
        }

        this.perNodeBuffers = Map.copyOf(sources);
    }

    /** {@inheritDoc} */
    @Override
    public long exchangeId() {
        return exchangeId;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        if (!inLoop) {
            this.execute(this::push);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        super.closeInternal();

        registry.unregister(this);
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<RowT>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        remoteSources = null;
        requested = 0;
        for (String nodeName : srcNodeNames) {
            RemoteSource<?> source = perNodeBuffers.get(nodeName);

            assert source != null;

            source.reset(context().sharedState());
        }
    }

    @Override
    @TestOnly
    public void dumpState(IgniteStringBuilder writer, String indent) {
        writer.app(indent)
                .app("class=").app(getClass().getSimpleName())
                .app(", requested=").app(requested)
                .nl();

        String childIndent = Debuggable.childIndentation(indent);

        for (String nodeName : srcNodeNames) {
            RemoteSource<?> source = perNodeBuffers.get(nodeName);
            writer.app(childIndent)
                    .app("class=" + source.getClass().getSimpleName())
                    .app(", nodeName=").app(nodeName)
                    .app(", state=").app(source.state)
                    .nl();
        }
    }

    /**
     * Pushes a batch into a buffer.
     *
     * @param srcNodeName Source node consistent id.
     * @param batchId Batch ID.
     * @param last Last batch flag.
     * @param rows Rows.
     */
    public void onBatchReceived(String srcNodeName, int batchId, boolean last, List<BinaryTupleMessage> rows) throws Exception {
        checkState();

        RemoteSource<RowT> source = perNodeBuffers.get(srcNodeName);

        boolean waitingBefore = source.check() == State.WAITING;

        List<RowT> rows0 = new ArrayList<>(rows.size());

        for (BinaryTupleMessage row : rows) {
            rows0.add(rowFactory.create(row.asBinaryTuple()));
        }

        source.onBatchReceived(batchId, last, rows0);

        if (requested > 0 && waitingBefore && source.check() != State.WAITING) {
            push();
        }
    }

    private void push() throws Exception {
        if (remoteSources == null) {
            remoteSources = new ArrayList<>(srcNodeNames.size());

            for (String nodeName : srcNodeNames) {
                remoteSources.add(perNodeBuffers.get(nodeName));
            }
        }

        if (comp != null) {
            pushOrdered();
        } else {
            pushUnordered();
        }
    }

    /** Checks that all corresponding buffers are in ready state. */
    private boolean checkAllBuffsReady(Iterator<RemoteSource<RowT>> it) {
        while (it.hasNext()) {
            RemoteSource<?> buf = it.next();

            State state = buf.check();

            switch (state) {
                case READY:
                    break;
                case END:
                    it.remove();
                    break;
                case WAITING:
                    return false;
                default:
                    throw unexpected(state);
            }
        }
        return true;
    }

    @SuppressWarnings("LabeledStatement")
    private void pushOrdered() throws Exception {
        if (!checkAllBuffsReady(remoteSources.iterator())) {
            for (RemoteSource<RowT> remote : remoteSources) {
                remote.requestNextBatchIfNeeded();
            }

            return;
        }

        assert comp != null;

        PriorityQueue<Pair<RowT, RemoteSource<RowT>>> heap =
                new PriorityQueue<>(Math.max(remoteSources.size(), 1), Map.Entry.comparingByKey(comp));

        for (RemoteSource<RowT> buf : remoteSources) {
            State state = buf.check();

            if (state == State.READY) {
                heap.offer(Pair.of(buf.peek(), buf));
            } else {
                throw new AssertionError("Unexpected buffer state: " + state);
            }
        }

        int processed = 0;
        inLoop = true;
        try {
            loop:
            while (requested > 0 && !heap.isEmpty()) {
                RemoteSource<RowT> source = heap.poll().right;

                requested--;
                downstream().push(source.remove());

                State state = source.check();

                switch (state) {
                    case END:
                        remoteSources.remove(source);
                        break;
                    case READY:
                        heap.offer(Pair.of(source.peek(), source));
                        break;
                    case WAITING:
                        // at this point we've drained all received batches from particular source,
                        // thus we need to wait next batch in order to be able to preserve the ordering
                        break loop;
                    default:
                        throw unexpected(state);
                }

                if (processed++ >= inBufSize) {
                    // Allow others to do their job.
                    execute(this::push);

                    return;
                }
            }
        } finally {
            inLoop = false;
        }

        for (RemoteSource<?> remote : remoteSources) {
            remote.requestNextBatchIfNeeded();
        }

        if (requested > 0 && remoteSources.isEmpty() && heap.isEmpty()) {
            requested = 0;
            downstream().end();
        }
    }

    private void pushUnordered() throws Exception {
        int idx = 0;
        int noProgress = 0;

        int processed = 0;
        inLoop = true;
        try {
            while (requested > 0 && !remoteSources.isEmpty()) {
                RemoteSource<RowT> source = remoteSources.get(idx);

                switch (source.check()) {
                    case END:
                        remoteSources.remove(idx);

                        break;
                    case READY:
                        noProgress = 0;
                        requested--;
                        downstream().push(source.remove());

                        break;
                    case WAITING:
                        noProgress++;
                        idx++;

                        break;
                    default:
                        break;
                }

                if (noProgress >= remoteSources.size()) {
                    break;
                }

                if (idx == remoteSources.size()) {
                    idx = 0;
                }

                if (processed++ >= inBufSize) {
                    // Allow others to do their job.
                    execute(this::push);

                    return;
                }
            }
        } finally {
            inLoop = false;
        }

        for (RemoteSource<?> source : remoteSources) {
            source.requestNextBatchIfNeeded();
        }

        if (requested > 0 && remoteSources.isEmpty()) {
            requested = 0;
            downstream().end();
        }
    }

    private void requestBatches(String nodeName, int cnt, @Nullable SharedState state) {
        exchange.request(nodeName, executionId(), srcFragmentId, exchangeId, cnt, state)
                .whenComplete((ignored, ex) -> {
                    if (ex != null) {
                        IgniteInternalException wrapperEx = ExceptionUtils.withCause(
                                IgniteInternalException::new,
                                Common.INTERNAL_ERR,
                                "Unable to request next batch: " + ex.getMessage(),
                                ex
                        );

                        this.execute(() -> onError(wrapperEx));
                    }
                });
    }

    /** Notifies the inbox that provided node has left the cluster. */
    public void onNodeLeft(InternalClusterNode node, long version) {
        Long topologyVersion = context().topologyVersion();
        if (topologyVersion != null && topologyVersion > version) {
            return; // Ignore outdated event.
        }

        if (srcNodeNames.contains(node.name())) {
            this.execute(() -> onNodeLeft0(node.name()));
        }
    }

    private void onNodeLeft0(String nodeName) {
        if (perNodeBuffers.get(nodeName).check() != State.END) {
            throw new NodeLeftException(nodeName);
        }
    }

    private static final class Batch<RowT> implements Comparable<Batch<RowT>> {
        private final int batchId;

        private final boolean last;

        private final List<RowT> rows;

        private int idx;

        private Batch(int batchId, boolean last, List<RowT> rows) {
            this.batchId = batchId;
            this.last = last;
            this.rows = rows;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Batch<RowT> batch = (Batch<RowT>) o;

            return batchId == batch.batchId;
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return batchId;
        }

        /** {@inheritDoc} */
        @Override
        public int compareTo(Inbox.Batch<RowT> o) {
            return Integer.compare(batchId, o.batchId);
        }
    }

    /**
     * An object to keep track of batches and their order from particular remote source.
     *
     * @param <RowT> A type if the rows received in batches.
     * @see State
     */
    static final class RemoteSource<RowT> {
        @FunctionalInterface
        private interface BatchRequester {
            void request(int amountOfBatches, @Nullable SharedState state) throws IgniteInternalCheckedException;
        }

        /**
         * A enumeration of all possible states of the {@link RemoteSource remote source}. Below is a state diagram showing possible
         * transitions from one state to another.
         *
         * <p>Node: "out of order", "next received", "and batch drained" are ephemeral states, thus not presented in enumeration.
         * <pre>
         *                    +---+
         *                    | * |
         *                    +---+
         *                      |
         *                      v
         *                 +---------+
         *                 | WAITING |<-----\
         *                 +---------+       \
         *                  /       ^         \
         *       batch received     |          \
         *                /         |           \
         *               v          |            \
         *        /-------------\   |            |
         *       | out of order? |  |            |
         *        \-------------/   |            |
         *           /       \      |            no
         *          no       yes   /             |
         *          |          \__/              |
         *          v                            |
         *      +-------+                 /--------------\
         *      | READY |<-----yes-------| next received? |
         *      +-------+                 \--------------/
         *           \___                        ^
         *               \                       |
         *           batch drained               /
         *                 \____                /
         *                      v              no
         *                /-----------\       /
         *               | last batch? |-----/
         *                \-----------/
         *                      |
         *                     yes
         *                      |
         *                      v
         *                   +-----+
         *                   | END |
         *                   +-----+
         *                      |
         *                      v
         *                    +---+
         *                    | * |
         *                    +---+
         * </pre>
         */
        enum State {
            /** Last batch was received and has already drained. No more data expected from this source. */
            END,

            /** Batch with expected id is received and ready to be drained. */
            READY,

            /** Batch with expected id is not received yet. */
            WAITING
        }

        private final PriorityQueue<Batch<RowT>> batches = new PriorityQueue<>(IO_BATCH_CNT);

        private final BatchRequester batchRequester;

        private State state = State.WAITING;
        private int lastEnqueued = -1;
        private int lastRequested = -1;
        private @Nullable Batch<RowT> curr = null;

        /**
         * The state should be propagated only once per every rewind iteration.
         *
         * <p>Thus, the new state is set on each rewind, propagated with the next request message,
         * and then immediately reset to null to prevent the same state from being sent once again.
         */
        private @Nullable SharedState sharedStateHolder = null;

        private RemoteSource(BatchRequester batchRequester) {
            this.batchRequester = batchRequester;
        }

        /**
         * Drops all received but not yet processed batches. Accepts the state that should be propagated to the source on the next
         * {@link #request} invocation.
         *
         * @param state State to propagate to the source.
         */
        void reset(SharedState state) {
            sharedStateHolder = state;
            batches.clear();

            this.lastEnqueued = lastRequested;
            this.state = State.WAITING;
            this.curr = null;
        }

        /** A handler for batches received from remote source. */
        void onBatchReceived(int id, boolean last, List<RowT> rows) {
            if (id <= lastEnqueued) {
                // most probably it's a batch that was prefetched in advance,
                // but the execution tree has been rewinded, so we just silently
                // drop it
                return;
            }

            batches.offer(new Batch<>(id, last, rows));

            if (state == State.WAITING && id == lastEnqueued + 1) {
                advanceBatch();
            }
        }

        /**
         * Requests another several batches from remote source if a count of in-flight batches is less or equal than half of
         * {@link #IO_BATCH_CNT}.
         */
        void requestNextBatchIfNeeded() throws IgniteInternalCheckedException {
            int maxInFlightCount = Math.max(IO_BATCH_CNT, 1);
            int currentInFlightCount = lastRequested - lastEnqueued;

            if (maxInFlightCount / 2 >= currentInFlightCount) {
                int countOfBatches = maxInFlightCount - currentInFlightCount;

                lastRequested += countOfBatches;

                batchRequester.request(countOfBatches, sharedStateHolder);
                // shared state should be send only once until next rewind
                sharedStateHolder = null;
            }
        }

        /** Returns the state of the source. */
        State check() {
            return state;
        }

        /**
         * Returns the first element of a buffer without removing.
         *
         * @return The first element of a buffer.
         */
        RowT peek() {
            assert state == State.READY;
            assert curr != null;

            return curr.rows.get(curr.idx);
        }

        /**
         * Removes the first element from a buffer.
         *
         * @return The removed element.
         */
        RowT remove() {
            assert state == State.READY;
            assert curr != null;

            RowT row = curr.rows.set(curr.idx++, null);

            if (curr.idx == curr.rows.size()) {
                if (curr.last) {
                    state = State.END;
                } else {
                    advanceBatch();
                }
            }

            return row;
        }

        private boolean hasNextBatch() {
            return !batches.isEmpty() && batches.peek().batchId == lastEnqueued + 1;
        }

        private void advanceBatch() {
            if (!hasNextBatch()) {
                state = State.WAITING;

                return;
            }

            curr = batches.poll();

            assert curr != null;

            state = curr.rows.isEmpty() ? State.END : State.READY;

            lastEnqueued = curr.batchId;
        }
    }
}
