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

import static org.apache.ignite.internal.util.CollectionUtils.first;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlJoinProjection;

/**
 * CorrelatedNestedLoopJoinNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class CorrelatedNestedLoopJoinNode<RowT> extends AbstractNode<RowT> {
    private final BiPredicate<RowT, RowT> cond;

    private final List<CorrelationId> correlationIds;

    private final JoinRelType joinType;

    private final SqlJoinProjection<RowT> joinProjection;

    private final int leftInBufferSize;

    private final int rightInBufferSize;

    private final BitSet leftMatched = new BitSet();

    private final RowT rightEmptyRow;

    private int requested;

    private int waitingLeft;

    private int waitingRight;

    private List<RowT> leftInBuf;

    private List<RowT> rightInBuf;

    private int leftIdx;

    private int rightIdx;

    private State state = State.INITIAL;

    private enum State {
        INITIAL, FILLING_LEFT, FILLING_RIGHT, IDLE, IN_LOOP, END
    }

    /**
     * Creates CorrelatedNestedLoopJoin node.
     *
     * @param ctx  Execution context.
     * @param cond Join expression.
     * @param correlationIds Set of collections ids.
     * @param joinType Join rel type.
     * @param rightRowFactory Right row factory.
     * @param joinProjection Output row factory.
     */
    public CorrelatedNestedLoopJoinNode(
            ExecutionContext<RowT> ctx,
            BiPredicate<RowT, RowT> cond,
            Set<CorrelationId> correlationIds,
            JoinRelType joinType,
            RowFactory<RowT> rightRowFactory,
            SqlJoinProjection<RowT> joinProjection
    ) {
        super(ctx);

        assert !nullOrEmpty(correlationIds);
        assert joinType == JoinRelType.LEFT || joinType == JoinRelType.INNER : joinType;

        this.cond = cond;
        this.correlationIds = new ArrayList<>(correlationIds);
        this.joinType = joinType;
        this.joinProjection = joinProjection;

        leftInBufferSize = correlationIds.size();
        rightInBufferSize = inBufSize;

        rightEmptyRow = rightRowFactory.create();
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        onRequest();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        leftInBuf = null;
        rightInBuf = null;

        leftIdx = 0;
        rightIdx = 0;

        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;

        state = State.INITIAL;
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx == 0) {
            return new Downstream<RowT>() {
                /** {@inheritDoc} */
                @Override
                public void push(List<RowT> batch) throws Exception {
                    pushLeft(batch);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endLeft();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    CorrelatedNestedLoopJoinNode.this.onError(e);
                }
            };
        } else if (idx == 1) {
            return new Downstream<RowT>() {
                /** {@inheritDoc} */
                @Override
                public void push(List<RowT> batch) throws Exception {
                    pushRight(batch);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endRight();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    CorrelatedNestedLoopJoinNode.this.onError(e);
                }
            };
        }

        throw new IndexOutOfBoundsException();
    }

    private void pushLeft(List<RowT> batch) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft -= batch.size();

        if (leftInBuf == null) {
            leftInBuf = new ArrayList<>(leftInBufferSize);
        }

        leftInBuf.addAll(batch);

        onPushLeft();
    }

    private void pushRight(List<RowT> batch) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        waitingRight -= batch.size();

        if (rightInBuf == null) {
            rightInBuf = new ArrayList<>(rightInBufferSize);
        }

        rightInBuf.addAll(batch);

        onPushRight();
    }

    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft = NOT_WAITING;

        if (leftInBuf == null) {
            leftInBuf = Collections.emptyList();
        }

        onEndLeft();
    }

    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        waitingRight = NOT_WAITING;

        if (rightInBuf == null) {
            rightInBuf = Collections.emptyList();
        }

        onEndRight();
    }

    private void onRequest() throws Exception {
        switch (state) {
            case IN_LOOP:
            case FILLING_RIGHT:
            case FILLING_LEFT:
                break;
            case INITIAL:
                assert waitingLeft == 0;
                assert waitingRight == 0;
                assert nullOrEmpty(leftInBuf);
                assert nullOrEmpty(rightInBuf);

                this.execute(() -> {
                    state = State.FILLING_LEFT;
                    leftSource().request(waitingLeft = leftInBufferSize);
                });

                break;
            case IDLE:
                assert rightInBuf != null;
                assert leftInBuf != null;
                assert waitingRight == NOT_WAITING || waitingRight == 0 && rightInBuf.size() == rightInBufferSize;
                assert waitingLeft == NOT_WAITING || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

                this.execute(this::join);

                break;

            case END:
                downstream().end();
                break;

            default:
                throw new AssertionError("Unexpected state:" + state);
        }
    }

    private void onPushLeft() throws Exception {
        assert state == State.FILLING_LEFT : "Unexpected state:" + state;
        assert waitingRight == 0 || waitingRight == NOT_WAITING;
        assert nullOrEmpty(rightInBuf);

        if (leftInBuf.size() == leftInBufferSize) {
            assert waitingLeft == 0;

            prepareCorrelations();
            rightSource().rewind();

            state = State.FILLING_RIGHT;
            rightSource().request(waitingRight = rightInBufferSize);
        }
    }

    private void onPushRight() throws Exception {
        assert state == State.FILLING_RIGHT : "Unexpected state:" + state;
        assert !nullOrEmpty(leftInBuf);
        assert waitingLeft == NOT_WAITING || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

        if (rightInBuf.size() == rightInBufferSize) {
            assert waitingRight == 0;

            state = State.IDLE;

            join();
        }
    }

    private void onEndLeft() throws Exception {
        assert state == State.FILLING_LEFT : "Unexpected state:" + state;
        assert waitingLeft == NOT_WAITING;
        assert waitingRight == 0 || waitingRight == NOT_WAITING;
        assert nullOrEmpty(rightInBuf);

        if (nullOrEmpty(leftInBuf)) {
            waitingRight = NOT_WAITING;

            state = State.END;

            if (requested > 0) {
                downstream().end();
            }
        } else {
            prepareCorrelations();

            if (waitingRight == NOT_WAITING) {
                rightSource().rewind();
            }

            state = State.FILLING_RIGHT;

            rightSource().request(waitingRight = rightInBufferSize);
        }
    }

    private void onEndRight() throws Exception {
        assert state == State.FILLING_RIGHT : "Unexpected state:" + state;
        assert waitingRight == NOT_WAITING;
        assert !nullOrEmpty(leftInBuf);
        assert waitingLeft == NOT_WAITING || waitingLeft == 0 && leftInBuf.size() == leftInBufferSize;

        state = State.IDLE;

        join();
    }

    private void join() throws Exception {
        assert state == State.IDLE;

        state = State.IN_LOOP;
        try {
            List<RowT> batch = allocateBatch(requested);
            while (requested > 0 && rightIdx < rightInBuf.size()) {
                if (leftIdx == leftInBuf.size()) {
                    leftIdx = 0;
                }

                while (requested > 0 && leftIdx < leftInBuf.size()) {
                    RowT left = leftInBuf.get(leftIdx);
                    RowT right = rightInBuf.get(rightIdx);

                    if (cond.test(left, right)) {
                        leftMatched.set(leftIdx);

                        requested--;

                        RowT row = joinProjection.project(context(), left, right);

                        batch.add(row);
                    }

                    leftIdx++;
                }

                if (leftIdx == leftInBuf.size()) {
                    rightInBuf.set(rightIdx++, null);
                }
            }

            if (!batch.isEmpty()) {
                downstream().push(batch);

                if (requested > 0 && rightIdx < rightInBuf.size()) {
                    releaseBatch(batch);
                    execute(this::join);

                    return;
                }
            }
            releaseBatch(batch);
        } finally {
            state = State.IDLE;
        }

        if (rightIdx == rightInBuf.size()) {
            leftIdx = 0;
            rightIdx = 0;

            if (waitingRight == 0) {
                rightInBuf = null;

                state = State.FILLING_RIGHT;

                rightSource().request(waitingRight = rightInBufferSize);

                return;
            }

            if (joinType == JoinRelType.LEFT && !nullOrEmpty(leftInBuf)) {
                int notMatchedIdx = leftMatched.nextClearBit(0);

                state = State.IN_LOOP;

                try {
                    List<RowT> batch = allocateBatch(Math.min(requested, leftInBuf.size() - notMatchedIdx));
                    while (requested > 0 && notMatchedIdx < leftInBuf.size()) {
                        requested--;

                        batch.add(joinProjection.project(context(), leftInBuf.get(notMatchedIdx), rightEmptyRow));

                        leftMatched.set(notMatchedIdx);

                        notMatchedIdx = leftMatched.nextClearBit(notMatchedIdx + 1);
                    }

                    if (!batch.isEmpty()) {
                        downstream().push(batch);

                        if (requested > 0 && notMatchedIdx < leftInBuf.size()) {
                            execute(this::join);
                            releaseBatch(batch);

                            return;
                        }
                    }
                    releaseBatch(batch);
                } finally {
                    state = State.IDLE;
                }

                if (requested == 0 && notMatchedIdx < leftInBuf.size()) {
                    return; // Some rows required to be pushed, wait for request.
                }
            }

            if (waitingLeft == 0) {
                rightInBuf = null;
                leftInBuf = null;
                leftMatched.clear();

                state = State.FILLING_LEFT;

                leftSource().request(waitingLeft = leftInBufferSize);

                return;
            }

            assert waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING;

            if (requested > 0) {
                leftInBuf = null;
                rightInBuf = null;

                state = State.END;

                if (requested > 0) {
                    downstream().end();
                }

                return;
            }

            // let's free the rows for GC
            leftInBuf = Collections.emptyList();
            rightInBuf = Collections.emptyList();
        }
    }

    private Node<RowT> leftSource() {
        return sources().get(0);
    }

    private Node<RowT> rightSource() {
        return sources().get(1);
    }

    private void prepareCorrelations() {
        for (int i = 0; i < correlationIds.size(); i++) {
            RowT row = i < leftInBuf.size() ? leftInBuf.get(i) : first(leftInBuf);
            context().correlatedVariable(row, correlationIds.get(i).getId());
        }
    }
}
