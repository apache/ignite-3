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
import static org.apache.ignite.lang.ErrorGroups.Sql.NODE_LEFT_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.exec.rel.Inbox.RemoteSource.State;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * A part of exchange which receives batches from remote sources.
 */
public class Inbox<RowT> extends AbstractNode<RowT> implements Mailbox<RowT>, SingleNode<RowT> {
    private final ExchangeService exchange;

    private final MailboxRegistry registry;

    private final long exchangeId;

    private final long srcFragmentId;

    private final Map<String, RemoteSource<RowT>> perNodeBuffers;

    private final Collection<String> srcNodeNames;

    private final @Nullable Comparator<RowT> comp;

    private List<RemoteSource<RowT>> remoteSources;

    private int requested;

    private boolean inLoop;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param exchange Exchange service.
     * @param registry Mailbox registry.
     * @param exchangeId Exchange ID.
     * @param srcFragmentId Source fragment ID.
     */
    public Inbox(
            ExecutionContext<RowT> ctx,
            ExchangeService exchange,
            MailboxRegistry registry,
            Collection<String> srcNodeNames,
            @Nullable Comparator<RowT> comp,
            long exchangeId,
            long srcFragmentId
    ) {
        super(ctx);
        this.exchange = exchange;
        this.registry = registry;
        this.srcNodeNames = srcNodeNames;
        this.comp = comp;

        this.srcFragmentId = srcFragmentId;
        this.exchangeId = exchangeId;

        perNodeBuffers = new HashMap<>();
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

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            context().execute(this::doPush, this::onError);
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
        throw new UnsupportedOperationException();
    }

    /**
     * Pushes a batch into a buffer.
     *
     * @param srcNodeName Source node consistent id.
     * @param batchId Batch ID.
     * @param last Last batch flag.
     * @param rows Rows.
     */
    public void onBatchReceived(String srcNodeName, int batchId, boolean last, List<RowT> rows) throws Exception {
        RemoteSource<RowT> source = getOrCreateBuffer(srcNodeName);

        boolean waitingBefore = source.check() == State.WAITING;

        source.onBatchReceived(batchId, last, rows);

        if (requested > 0 && waitingBefore && source.check() != State.WAITING) {
            push();
        }
    }

    private void doPush() throws Exception {
        checkState();

        push();
    }

    private void push() throws Exception {
        if (remoteSources == null) {
            for (String node : srcNodeNames) {
                checkNode(node);
            }

            remoteSources = new ArrayList<>(srcNodeNames.size());

            for (String nodeName : srcNodeNames) {
                remoteSources.add(getOrCreateBuffer(nodeName));
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

        inLoop = true;
        try {
            loop:
            while (requested > 0 && !heap.isEmpty()) {
                checkState();

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

        inLoop = true;
        try {
            while (requested > 0 && !remoteSources.isEmpty()) {
                checkState();

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

    private void requestBatches(String nodeName, int cnt) throws IgniteInternalCheckedException {
        exchange.request(nodeName, queryId(), srcFragmentId, exchangeId, cnt);
    }

    private RemoteSource<RowT> getOrCreateBuffer(String nodeName) {
        return perNodeBuffers.computeIfAbsent(nodeName, name -> new RemoteSource<>(cnt -> requestBatches(name, cnt)));
    }

    /**
     * OnNodeLeft.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public void onNodeLeft(String nodeName) {
        if (context().originatingNodeName().equals(nodeName) && srcNodeNames == null) {
            context().execute(this::close, this::onError);
        } else if (srcNodeNames != null && srcNodeNames.contains(nodeName)) {
            context().execute(() -> onNodeLeft0(nodeName), this::onError);
        }
    }

    private void onNodeLeft0(String nodeName) throws Exception {
        checkState();

        if (getOrCreateBuffer(nodeName).check() != State.END) {
            throw new IgniteInternalCheckedException(NODE_LEFT_ERR, "Failed to execute query, node left [nodeName=" + nodeName + ']');
        }
    }

    private void checkNode(String nodeName) throws IgniteInternalCheckedException {
        if (!exchange.alive(nodeName)) {
            throw new IgniteInternalCheckedException(NODE_LEFT_ERR, "Failed to execute query, node left [nodeName=" + nodeName + ']');
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
            void request(int amountOfBatches) throws IgniteInternalCheckedException;
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

        private RemoteSource(BatchRequester batchRequester) {
            this.batchRequester = batchRequester;
        }

        /** A handler for batches received from remote source. */
        void onBatchReceived(int id, boolean last, List<RowT> rows) {
            batches.offer(new Batch<>(id, last, rows));

            if (state == State.WAITING && id == lastEnqueued + 1) {
                advanceBatch();
            }
        }

        /**
         * Requests another several batches from remote source if a count of in-flight batches
         * is less or equal than half of {@link #IO_BATCH_CNT}.
         */
        void requestNextBatchIfNeeded() throws IgniteInternalCheckedException {
            int inFlightCount = lastRequested - lastEnqueued;

            // IO_BATCH_CNT should never be less than 1, but we don't have validation
            if (IO_BATCH_CNT <= 1 && inFlightCount == 0) {
                batchRequester.request(1);
            } else if (IO_BATCH_CNT / 2 >= inFlightCount) {
                int countOfBatches = IO_BATCH_CNT - inFlightCount;

                lastRequested += countOfBatches;

                batchRequester.request(countOfBatches);
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
