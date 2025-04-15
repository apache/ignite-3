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
import java.util.Deque;
import java.util.List;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.jetbrains.annotations.Nullable;

/** Right part materialized join node, i.e. all data from right part of join is available locally. */
public abstract class AbstractRightMaterializedJoinNode<RowT> extends AbstractNode<RowT> {
    protected boolean inLoop;
    protected int requested;
    int waitingLeft;
    int waitingRight;
    final Deque<RowT> leftInBuf = new ArrayDeque<>(inBufSize);
    protected @Nullable RowT left;

    AbstractRightMaterializedJoinNode(ExecutionContext<RowT> ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 2;
        assert rowsCnt > 0 && requested == 0;

        requested = rowsCnt;

        if (!inLoop) {
            this.execute(this::join);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waitingLeft = 0;
        waitingRight = 0;
        left = null;

        leftInBuf.clear();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx == 0) {
            return new Downstream<>() {
                /** {@inheritDoc} */
                @Override
                public void push(RowT row) throws Exception {
                    pushLeft(row);
                }

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
                    AbstractRightMaterializedJoinNode.this.onError(e);
                }
            };
        } else if (idx == 1) {
            return new Downstream<>() {
                /** {@inheritDoc} */
                @Override
                public void push(RowT row) throws Exception {
                    pushRight(row);
                }

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
                    AbstractRightMaterializedJoinNode.this.onError(e);
                }
            };
        }

        throw new IndexOutOfBoundsException();
    }

    protected void pushLeft(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft--;

        leftInBuf.add(row);

        join();
    }

    protected void pushLeft(List<RowT> batch) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft -= batch.size();

        leftInBuf.addAll(batch);

        join();
    }

    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft = NOT_WAITING;

        join();
    }

    protected void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        waitingRight = NOT_WAITING;

        join();
    }

    Node<RowT> leftSource() {
        return sources().get(0);
    }

    Node<RowT> rightSource() {
        return sources().get(1);
    }

    protected abstract void join() throws Exception;

    protected void pushRight(RowT row) throws Exception {
        pushRight(List.of(row));
    }

    protected abstract void pushRight(List<RowT> batch) throws Exception;
}
