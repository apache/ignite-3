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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.sql.engine.exec.ExchangeService;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.MailboxRegistry;
import org.apache.ignite.internal.sql.engine.trait.Destination;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * A part of exchange which sends batches to a remote downstream.
 */
public class Outbox<RowT> extends AbstractNode<RowT> implements Mailbox<RowT>, SingleNode<RowT>, Downstream<RowT> {
    private static final IgniteLogger LOG = Loggers.forClass(Outbox.class);

    private final long exchangeId;
    private final long targetFragmentId;
    private final ExchangeService exchange;
    private final MailboxRegistry registry;
    private final Destination<RowT> dest;

    private final Deque<RowT> inBuf = new ArrayDeque<>(inBufSize);
    private final Map<String, RemoteDownstream<RowT>> nodeBuffers = new HashMap<>();

    private int waiting;

    /**
     * Constructor.
     *
     * @param ctx An execution context.
     * @param exchange A service that provide a way for Inbox and Outbox to communicate with each other.
     * @param registry A registry of all created inboxes and outboxes.
     * @param exchangeId An identifier of the exchange this outbox is part of.
     * @param targetFragmentId An identifier of the fragment to send batches to.
     * @param dest A function which determines which row to send on which remote.
     */
    public Outbox(
            ExecutionContext<RowT> ctx,
            ExchangeService exchange,
            MailboxRegistry registry,
            long exchangeId,
            long targetFragmentId,
            Destination<RowT> dest
    ) {
        super(ctx);
        this.exchange = exchange;
        this.registry = registry;
        this.targetFragmentId = targetFragmentId;
        this.exchangeId = exchangeId;
        this.dest = dest;

        initBuffers();
    }

    private void initBuffers() {
        for (String nodeName : dest.targets()) {
            nodeBuffers.put(nodeName, new RemoteDownstream<>(nodeName, this::sendBatch));
        }
    }

    /** {@inheritDoc} */
    @Override
    public long exchangeId() {
        return exchangeId;
    }

    /**
     * A handler with saves the demand from remote downstream and starts the execution.
     *
     * @param nodeName An identifier of the demander.
     * @param amountOfBatches A count of demanded batches.
     */
    public void onRequest(String nodeName, int amountOfBatches) throws Exception {
        checkState();

        RemoteDownstream<?> downstream = getOrCreateBuffer(nodeName);

        downstream.onBatchRequested(amountOfBatches);

        if (waiting != -1 || !inBuf.isEmpty()) {
            flush();
        }
    }

    /**
     * Starts the execution of the fragment and keeps the result in the intermediate buffer.
     *
     * <p>Note: this method must be called by the same thread that will execute the whole fragment.
     */
    public void prefetch() {
        if (!context().description().prefetch()) {
            // this fragment can't be executed in advance
            return;
        }

        try {
            checkState();

            if (waiting == 0) {
                source().request(waiting = inBufSize);
            }
        } catch (Throwable t) {
            onError(t);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowCnt) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert waiting > 0;

        checkState();

        waiting--;

        inBuf.add(row);

        flush();
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert waiting > 0;

        checkState();

        waiting = -1;

        flush();
    }

    /** {@inheritDoc} */
    @Override
    public void onError(Throwable e) {
        try {
            sendError(e);
        } catch (IgniteInternalCheckedException ex) {
            LOG.info("Unable to send error message", e);
        } finally {
            Commons.closeQuiet(this);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        super.closeInternal();

        registry.unregister(this);

        // Send cancel message for the Inbox to close Inboxes created by batch message race.
        for (String node : dest.targets()) {
            getOrCreateBuffer(node).close();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onRegister(Downstream<RowT> downstream) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        inBuf.clear();
        nodeBuffers.clear();
        waiting = 0;

        initBuffers();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    private void sendBatch(String nodeName, int batchId, boolean last, List<RowT> rows) throws IgniteInternalCheckedException {
        exchange.sendBatch(nodeName, queryId(), targetFragmentId, exchangeId, batchId, last, rows);
    }

    private void sendError(Throwable err) throws IgniteInternalCheckedException {
        exchange.sendError(context().originatingNodeName(), queryId(), fragmentId(), err);
    }

    private void sendInboxClose(String nodeName) {
        try {
            exchange.closeInbox(nodeName, queryId(), targetFragmentId, exchangeId);
        } catch (IgniteInternalCheckedException e) {
            LOG.info("Unable to send cancel message", e);
        }
    }

    private RemoteDownstream<RowT> getOrCreateBuffer(String nodeName) {
        return nodeBuffers.computeIfAbsent(nodeName, name -> new RemoteDownstream<>(name, this::sendBatch));
    }

    private void flush() throws Exception {
        while (!inBuf.isEmpty()) {
            checkState();

            List<String> targets = dest.targets(inBuf.peek());
            List<RemoteDownstream<RowT>> buffers = new ArrayList<>(targets.size());

            for (String target : targets) {
                RemoteDownstream<RowT> buffer = getOrCreateBuffer(target);

                if (!buffer.ready()) {
                    return;
                }

                buffers.add(buffer);
            }

            assert !nullOrEmpty(buffers);

            RowT row = inBuf.remove();

            for (RemoteDownstream<RowT> dest : buffers) {
                dest.add(row);
            }
        }

        assert inBuf.isEmpty();

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        } else if (waiting == -1) {
            for (RemoteDownstream<RowT> buffer : nodeBuffers.values()) {
                buffer.end();
            }
        }
    }

    /**
     * OnNodeLeft.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public void onNodeLeft(String nodeName) {
        if (nodeName.equals(context().originatingNodeName())) {
            context().execute(this::close, this::onError);
        }
    }

    private static final class RemoteDownstream<RowT> {
        @FunctionalInterface
        private interface BatchSender<RowT> {
            void send(String targetNodeName, int batchId, boolean last, List<RowT> rows) throws IgniteInternalCheckedException;
        }

        /**
         * A enumeration of all possible states of the {@link RemoteDownstream remote downstream}. Below is a state diagram showing possible
         * transitions from one state to another.
         *
         * <p>Node: "batch is full" is ephemeral state, thus not presented in enumeration.
         * <pre>
         *                    +---+
         *                    | * |
         *                    +---+
         *                      |
         *                      v
         *                 +---------+
         *        /--------| FILLING |<---\
         *       /         +---------+     \
         *      |           /       ^       \
         *      |    row added       \       |
         *      |          \         no      |
         *      |           v        /       |
         *      |       /---------------\    |
         *      |      | batch is full?  |   |
         *      |       \---------------/    |
         *      |               |            |
         *      |              yes          /
         *      |               |       batch sent
         * downstream ended     v         /
         *      |           +-------+    /
         *      |           | FULL  |---/
         *      |           +-------+
         *      |               |
         *       \     downstream ended
         *        \             |
         *         \            v
         *          \     /-----------\
         *           \-->| LAST BATCH  |
         *                \-----------/
         *                      |
         *                  batch sent
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
            /** Batch is ready to accept at leas one row. */
            FILLING,

            /** Batch is full, thus is ready to be sent. */
            FULL,

            /** No more rows are expected to be added to downstream. The next batch will be sent, probably, partially filled. */
            LAST_BATCH,

            /** Downstream is closed. All resources were released. */
            END
        }

        private final String nodeName;
        private final BatchSender<RowT> sender;

        private State state = State.FILLING;
        private int lastSentBatchId = -1;

        private @Nullable List<RowT> curr;
        private int pendingCount;

        private RemoteDownstream(String nodeName, BatchSender<RowT> sender) {
            this.nodeName = nodeName;
            this.sender = sender;

            curr = new ArrayList<>(IO_BATCH_SIZE);
        }

        /** A handler of a requests from downstream. */
        void onBatchRequested(int amountOfBatches) throws Exception {
            assert amountOfBatches > 0 : amountOfBatches;

            this.pendingCount = amountOfBatches;

            // if there is a batch which is ready to be sent, then just sent it
            if (state == State.FULL || state == State.LAST_BATCH) {
                sendBatch();
            }
        }

        /** Returns {@code true} if this downstream is ready to accepts at least one more row. */
        boolean ready() {
            return state == State.FILLING;
        }

        /**
         * Adds a row to current batch.
         *
         * @param row Row to add.
         */
        void add(RowT row) throws Exception {
            assert ready() : state;
            assert curr != null;

            curr.add(row);

            if (curr.size() == IO_BATCH_SIZE) {
                state = State.FULL;

                if (pendingCount > 0) {
                    sendBatch();
                }
            }
        }

        /** Sends current batch to remote downstream. */
        void sendBatch() throws Exception {
            assert pendingCount > 0;
            assert state == State.FULL || state == State.LAST_BATCH : state;
            assert curr != null;

            boolean lastBatch = state == State.LAST_BATCH;

            sender.send(nodeName, ++lastSentBatchId, lastBatch, curr);

            pendingCount--;

            if (lastBatch) {
                state = State.END;
                curr = null;
            } else {
                state = State.FILLING;
                curr = new ArrayList<>(IO_BATCH_SIZE);
            }
        }

        /** Completes this downstream by sending all collected so far rows. */
        void end() throws Exception {
            assert state == State.FILLING || state == State.FULL : state;

            state = State.LAST_BATCH;

            if (pendingCount > 0) {
                sendBatch();
            }
        }

        /** Closes this downstream and clears all acquired resources. */
        void close() {
            curr = null;
            state = State.END;
        }
    }
}
