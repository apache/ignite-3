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

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.convertStructuredType;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.SqlJoinProjection;
import org.apache.ignite.internal.type.StructNativeType;
import org.jetbrains.annotations.Nullable;

/**
 * MergeJoinNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class MergeJoinNode<RowT> extends AbstractNode<RowT> {
    protected final Comparator<RowT> comp;

    protected int requested;

    protected int waitingLeft;

    protected int waitingRight;

    protected final Deque<RowT> rightInBuf = new ArrayDeque<>(inBufSize);

    protected final Deque<RowT> leftInBuf = new ArrayDeque<>(inBufSize);

    protected boolean inLoop;

    /**
     * Creates MergeJoinNode.
     *
     * @param ctx  Execution context.
     * @param comp Join expression.
     */
    private MergeJoinNode(ExecutionContext<RowT> ctx, Comparator<RowT> comp) {
        super(ctx);

        this.comp = comp;
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

        rightInBuf.clear();
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

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endLeft();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    MergeJoinNode.this.onError(e);
                }
            };
        } else if (idx == 1) {
            return new Downstream<>() {
                /** {@inheritDoc} */
                @Override
                public void push(RowT row) throws Exception {
                    pushRight(row);
                }

                /** {@inheritDoc} */
                @Override
                public void end() throws Exception {
                    endRight();
                }

                /** {@inheritDoc} */
                @Override
                public void onError(Throwable e) {
                    MergeJoinNode.this.onError(e);
                }
            };
        }

        throw new IndexOutOfBoundsException();
    }

    @Override
    protected void dumpDebugInfo0(IgniteStringBuilder buf) {
        buf.app("class=").app(getClass().getSimpleName())
                .app(", requested=").app(requested)
                .app(", waitingLeft=").app(waitingLeft)
                .app(", waitingRight=").app(waitingRight);
    }

    private void pushLeft(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft--;

        leftInBuf.add(row);

        if (waitingLeft == 0 && waitingRight <= 0) {
            join();
        }
    }

    private void pushRight(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        waitingRight--;

        rightInBuf.add(row);

        if (waitingRight == 0 && waitingLeft <= 0) {
            join();
        }
    }

    private void endLeft() throws Exception {
        assert downstream() != null;
        assert waitingLeft > 0;

        waitingLeft = NOT_WAITING;

        if (waitingRight <= 0) {
            join();
        }
    }

    private void endRight() throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        waitingRight = NOT_WAITING;

        if (waitingLeft <= 0) {
            join();
        }
    }

    protected Node<RowT> leftSource() {
        return sources().get(0);
    }

    protected Node<RowT> rightSource() {
        return sources().get(1);
    }

    protected abstract void join() throws Exception;

    /**
     * Create MergeJoinNode for requested join operator type.
     *
     * @param ctx Execution context.
     * @param leftRowType Row type of the left source.
     * @param rightRowType Row type of the right source.
     * @param joinType Join operator type.
     * @param comp Join expression comparator.
     * @param outputProjection Output projection.
     */
    public static <RowT> MergeJoinNode<RowT> create(ExecutionContext<RowT> ctx, RelDataType leftRowType,
            RelDataType rightRowType, JoinRelType joinType, Comparator<RowT> comp, @Nullable SqlJoinProjection<RowT> outputProjection) {
        switch (joinType) {
            case INNER: {
                assert outputProjection != null;

                return new InnerJoin<>(ctx, comp, outputProjection);
            }

            case LEFT: {
                assert outputProjection != null;

                StructNativeType rightRowSchema = convertStructuredType(rightRowType);
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftJoin<>(ctx, comp, outputProjection, rightRowFactory);
            }

            case RIGHT: {
                assert outputProjection != null;

                StructNativeType leftRowSchema = convertStructuredType(leftRowType);
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightJoin<>(ctx, comp, outputProjection, leftRowFactory);
            }

            case FULL: {
                assert outputProjection != null;

                StructNativeType leftRowSchema = convertStructuredType(leftRowType);
                StructNativeType rightRowSchema = convertStructuredType(rightRowType);

                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterJoin<>(ctx, comp, outputProjection, leftRowFactory, rightRowFactory);
            }

            case SEMI: {
                assert outputProjection == null;

                return new SemiJoin<>(ctx, comp);
            }

            case ANTI: {
                assert outputProjection == null;

                return new AntiJoin<>(ctx, comp);
            }

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    private static class InnerJoin<RowT> extends MergeJoinNode<RowT> {
        private final SqlJoinProjection<RowT> outputProjection;

        private RowT left;

        private RowT right;

        /** Used to store similar rows of rights stream in many-to-many join mode. */
        private List<RowT> rightMaterialization;

        private int rightIdx;

        private boolean drainMaterialization;

        /**
         * Creates MergeJoinNode for INNER JOIN operator.
         *
         * @param ctx Execution context.
         * @param comp Join expression comparator.
         * @param outputProjection Output projection.
         */
        private InnerJoin(ExecutionContext<RowT> ctx, Comparator<RowT> comp, SqlJoinProjection<RowT> outputProjection) {
            super(ctx, comp);

            this.outputProjection = outputProjection;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            right = null;
            rightIdx = 0;
            rightMaterialization = null;
            drainMaterialization = false;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            int processed = 0;
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty()) && (right != null || !rightInBuf.isEmpty()
                        || rightMaterialization != null)) {
                    if (processed++ > inBufSize) {
                        // Allow others to do their job.
                        execute(this::join);

                        return;
                    }

                    if (left == null) {
                        left = leftInBuf.remove();
                    }

                    if (right == null) {
                        if (rightInBuf.isEmpty() && waitingRight != NOT_WAITING) {
                            break;
                        }

                        if (!rightInBuf.isEmpty()) {
                            right = rightInBuf.remove();
                        }
                    }

                    if (right == null && rightMaterialization != null && !drainMaterialization) {
                        drainMaterialization = true;
                        left = null;

                        continue;
                    }

                    RowT row;
                    if (!drainMaterialization) {
                        int cmp = comp.compare(left, right);

                        if (cmp < 0) {
                            left = null;
                            rightIdx = 0;

                            if (rightMaterialization != null) {
                                drainMaterialization = true;
                            }

                            continue;
                        } else if (cmp > 0) {
                            right = null;
                            rightIdx = 0;
                            rightMaterialization = null;

                            continue;
                        }

                        if (rightMaterialization == null && (!rightInBuf.isEmpty() || waitingRight != NOT_WAITING)) {
                            if (rightInBuf.isEmpty()) {
                                break;
                            }

                            if (comp.compare(left, rightInBuf.peek()) == 0) {
                                rightMaterialization = new ArrayList<>();
                            }
                        }

                        row = outputProjection.project(context(), left, right);

                        if (rightMaterialization != null) {
                            rightMaterialization.add(right);

                            right = null;
                        } else {
                            left = null;
                        }
                    } else {
                        if (rightIdx >= rightMaterialization.size()) {
                            rightIdx = 0;
                            left = null;

                            continue;
                        }

                        RowT right = rightMaterialization.get(rightIdx++);

                        int cmp = comp.compare(left, right);

                        if (cmp > 0) {
                            rightIdx = 0;
                            rightMaterialization = null;
                            drainMaterialization = false;

                            continue;
                        }

                        row = outputProjection.project(context(), left, right);
                    }

                    requested--;
                    downstream().push(row);
                }
            } finally {
                inLoop = false;
            }

            if (requested > 0 && ((waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty())
                    || (waitingRight == NOT_WAITING && right == null && rightInBuf.isEmpty() && rightMaterialization == null))
            ) {
                requested = 0;
                downstream().end();

                return;
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0) {
                leftSource().request(waitingLeft = inBufSize);
            }
        }
    }

    private static class LeftJoin<RowT> extends MergeJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        private final SqlJoinProjection<RowT> outputProjection;

        private RowT left;

        private RowT right;

        /** Used to store similar rows of rights stream in many-to-many join mode. */
        private List<RowT> rightMaterialization;

        private int rightIdx;

        private boolean drainMaterialization;

        /** Whether current left row was matched (hence pushed to downstream) or not. */
        private boolean matched;

        /**
         * Creates MergeJoinNode for LEFT OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param comp Join expression comparator.
         * @param outputProjection Output projection.
         * @param rightRowFactory Row factory for the right source.
         */
        private LeftJoin(
                ExecutionContext<RowT> ctx,
                Comparator<RowT> comp,
                SqlJoinProjection<RowT> outputProjection,
                RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, comp);

            this.outputProjection = outputProjection;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            right = null;
            rightIdx = 0;
            rightMaterialization = null;
            drainMaterialization = false;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            int processed = 0;
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty()) && (right != null || !rightInBuf.isEmpty()
                        || rightMaterialization != null || waitingRight == NOT_WAITING)) {
                    if (processed++ > inBufSize) {
                        // Allow others to do their job.
                        execute(this::join);

                        return;
                    }

                    if (left == null) {
                        left = leftInBuf.remove();

                        matched = false;
                    }

                    if (right == null) {
                        if (rightInBuf.isEmpty() && waitingRight != NOT_WAITING) {
                            break;
                        }

                        if (!rightInBuf.isEmpty()) {
                            right = rightInBuf.remove();
                        }
                    }

                    if (right == null && rightMaterialization != null && !drainMaterialization) {
                        drainMaterialization = true;
                        left = null;

                        continue;
                    }

                    RowT row;
                    if (!drainMaterialization) {
                        if (right == null) {
                            row = outputProjection.project(context(), left, rightRowFactory.create());

                            requested--;
                            downstream().push(row);

                            left = null;

                            continue;
                        }

                        int cmp = comp.compare(left, right);

                        if (cmp < 0) {
                            if (!matched) {
                                row = outputProjection.project(context(), left, rightRowFactory.create());

                                requested--;
                                downstream().push(row);
                            }

                            left = null;
                            rightIdx = 0;

                            if (rightMaterialization != null) {
                                drainMaterialization = true;
                            }

                            continue;
                        } else if (cmp > 0) {
                            right = null;
                            rightIdx = 0;
                            rightMaterialization = null;

                            continue;
                        }

                        matched = true;

                        if (rightMaterialization == null && (!rightInBuf.isEmpty() || waitingRight != NOT_WAITING)) {
                            if (rightInBuf.isEmpty()) {
                                break;
                            }

                            if (comp.compare(left, rightInBuf.peek()) == 0) {
                                rightMaterialization = new ArrayList<>();
                            }
                        }

                        row = outputProjection.project(context(), left, right);

                        if (rightMaterialization != null) {
                            rightMaterialization.add(right);

                            right = null;
                        } else {
                            left = null;
                        }
                    } else {
                        if (rightIdx >= rightMaterialization.size()) {
                            rightIdx = 0;
                            left = null;

                            continue;
                        }

                        RowT right = rightMaterialization.get(rightIdx++);

                        int cmp = comp.compare(left, right);

                        if (cmp > 0) {
                            rightIdx = 0;
                            rightMaterialization = null;
                            drainMaterialization = false;

                            continue;
                        }

                        row = outputProjection.project(context(), left, right);
                    }

                    requested--;
                    downstream().push(row);
                }
            } finally {
                inLoop = false;
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();

                return;
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0) {
                leftSource().request(waitingLeft = inBufSize);
            }
        }
    }

    private static class RightJoin<RowT> extends MergeJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        private final SqlJoinProjection<RowT> outputProjection;

        private RowT left;

        private RowT right;

        /** Used to store similar rows of rights stream in many-to-many join mode. */
        private List<RowT> rightMaterialization;

        private int rightIdx;

        private boolean drainMaterialization;

        /** Whether current right row was matched (hence pushed to downstream) or not. */
        private boolean matched;

        /**
         * Creates MergeJoinNode for RIGHT OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param comp Join expression comparator.
         * @param outputProjection Output projection.
         * @param leftRowFactory Row factory for the left source.
         */
        private RightJoin(
                ExecutionContext<RowT> ctx,
                Comparator<RowT> comp,
                SqlJoinProjection<RowT> outputProjection,
                RowHandler.RowFactory<RowT> leftRowFactory
        ) {
            super(ctx, comp);

            this.outputProjection = outputProjection;
            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            right = null;
            rightIdx = 0;
            rightMaterialization = null;
            drainMaterialization = false;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            int processed = 0;
            inLoop = true;
            try {
                while (requested > 0 && !(left == null && leftInBuf.isEmpty() && waitingLeft != NOT_WAITING)
                        && (right != null || !rightInBuf.isEmpty() || rightMaterialization != null)) {
                    if (processed++ > inBufSize) {
                        // Allow others to do their job.
                        execute(this::join);

                        return;
                    }

                    if (left == null && !leftInBuf.isEmpty()) {
                        left = leftInBuf.remove();
                    }

                    if (right == null) {
                        if (rightInBuf.isEmpty() && waitingRight != NOT_WAITING) {
                            break;
                        }

                        if (!rightInBuf.isEmpty()) {
                            right = rightInBuf.remove();

                            matched = false;
                        }
                    }

                    if (right == null && rightMaterialization != null && !drainMaterialization) {
                        drainMaterialization = true;
                        left = null;

                        continue;
                    }

                    RowT row;
                    if (!drainMaterialization) {
                        if (left == null) {
                            if (!matched) {
                                row = outputProjection.project(context(), leftRowFactory.create(), right);

                                requested--;
                                downstream().push(row);
                            }

                            right = null;

                            continue;
                        }

                        int cmp = comp.compare(left, right);

                        if (cmp < 0) {
                            left = null;
                            rightIdx = 0;

                            if (rightMaterialization != null) {
                                drainMaterialization = true;
                            }

                            continue;
                        } else if (cmp > 0) {
                            if (!matched) {
                                row = outputProjection.project(context(), leftRowFactory.create(), right);

                                requested--;
                                downstream().push(row);
                            }

                            right = null;
                            rightIdx = 0;
                            rightMaterialization = null;

                            continue;
                        }

                        if (rightMaterialization == null && (!rightInBuf.isEmpty() || waitingRight != NOT_WAITING)) {
                            if (rightInBuf.isEmpty()) {
                                break;
                            }

                            if (comp.compare(left, rightInBuf.peek()) == 0) {
                                rightMaterialization = new ArrayList<>();
                            }
                        }

                        matched = true;

                        row = outputProjection.project(context(), left, right);

                        if (rightMaterialization != null) {
                            rightMaterialization.add(right);

                            right = null;
                        } else {
                            left = null;
                        }
                    } else {
                        if (left == null) {
                            if (waitingLeft == NOT_WAITING) {
                                rightIdx = 0;
                                rightMaterialization = null;
                                drainMaterialization = false;
                            }

                            continue;
                        }

                        if (rightIdx >= rightMaterialization.size()) {
                            rightIdx = 0;
                            left = null;

                            continue;
                        }

                        RowT right = rightMaterialization.get(rightIdx++);

                        int cmp = comp.compare(left, right);

                        if (cmp > 0) {
                            rightIdx = 0;
                            rightMaterialization = null;
                            drainMaterialization = false;

                            continue;
                        }

                        row = outputProjection.project(context(), left, right);
                    }

                    requested--;
                    downstream().push(row);
                }
            } finally {
                inLoop = false;
            }

            if (requested > 0 && waitingRight == NOT_WAITING && right == null && rightInBuf.isEmpty() && rightMaterialization == null) {
                requested = 0;
                downstream().end();

                return;
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0) {
                leftSource().request(waitingLeft = inBufSize);
            }
        }
    }

    private static class FullOuterJoin<RowT> extends MergeJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        private final SqlJoinProjection<RowT> outputProjection;

        private RowT left;

        private RowT right;

        /** Used to store similar rows of rights stream in many-to-many join mode. */
        private List<RowT> rightMaterialization;

        private int rightIdx;

        private boolean drainMaterialization;

        /** Whether current left row was matched (hence pushed to downstream) or not. */
        private boolean leftMatched;

        /** Whether current right row was matched (hence pushed to downstream) or not. */
        private boolean rightMatched;

        /**
         * Creates MergeJoinNode for FULL OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param comp Join expression comparator.
         * @param outputProjection Output projection.
         * @param leftRowFactory Row factory for the left source.
         * @param rightRowFactory Row factory for the right source.
         */
        private FullOuterJoin(
                ExecutionContext<RowT> ctx,
                Comparator<RowT> comp,
                SqlJoinProjection<RowT> outputProjection,
                RowHandler.RowFactory<RowT> leftRowFactory,
                RowHandler.RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, comp);

            this.outputProjection = outputProjection;
            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            right = null;
            rightIdx = 0;
            rightMaterialization = null;
            drainMaterialization = false;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            int processed = 0;
            inLoop = true;
            try {
                while (requested > 0 && !(left == null && leftInBuf.isEmpty() && waitingLeft != NOT_WAITING)
                        && !(right == null && rightInBuf.isEmpty() && rightMaterialization == null && waitingRight != NOT_WAITING)) {
                    if (processed++ > inBufSize) {
                        // Allow others to do their job.
                        execute(this::join);

                        return;
                    }

                    if (left == null && !leftInBuf.isEmpty()) {
                        left = leftInBuf.remove();

                        leftMatched = false;
                    }

                    if (right == null) {
                        if (rightInBuf.isEmpty() && waitingRight != NOT_WAITING) {
                            break;
                        }

                        if (!rightInBuf.isEmpty()) {
                            right = rightInBuf.remove();

                            rightMatched = false;
                        }
                    }

                    if (right == null && rightMaterialization != null && !drainMaterialization) {
                        drainMaterialization = true;
                        left = null;

                        continue;
                    }

                    RowT row;
                    if (!drainMaterialization) {
                        if (left == null || right == null) {
                            if (left == null && right != null) {
                                if (!rightMatched) {
                                    row = outputProjection.project(context(), leftRowFactory.create(), right);

                                    requested--;
                                    downstream().push(row);
                                }

                                right = null;

                                continue;
                            }

                            if (left != null && right == null) {
                                if (!leftMatched) {
                                    row = outputProjection.project(context(), left, rightRowFactory.create());

                                    requested--;
                                    downstream().push(row);
                                }

                                left = null;

                                continue;
                            }

                            break;
                        }

                        int cmp = comp.compare(left, right);

                        if (cmp < 0) {
                            if (!leftMatched) {
                                row = outputProjection.project(context(), left, rightRowFactory.create());

                                requested--;
                                downstream().push(row);
                            }

                            left = null;
                            rightIdx = 0;

                            if (rightMaterialization != null) {
                                drainMaterialization = true;
                            }

                            continue;
                        } else if (cmp > 0) {
                            if (!rightMatched) {
                                row = outputProjection.project(context(), leftRowFactory.create(), right);

                                requested--;
                                downstream().push(row);
                            }

                            right = null;
                            rightIdx = 0;
                            rightMaterialization = null;

                            continue;
                        }

                        if (rightMaterialization == null && (!rightInBuf.isEmpty() || waitingRight != NOT_WAITING)) {
                            if (rightInBuf.isEmpty()) {
                                break;
                            }

                            if (comp.compare(left, rightInBuf.peek()) == 0) {
                                rightMaterialization = new ArrayList<>();
                            }
                        }

                        leftMatched = true;
                        rightMatched = true;

                        row = outputProjection.project(context(), left, right);

                        if (rightMaterialization != null) {
                            rightMaterialization.add(right);

                            right = null;
                        } else {
                            left = null;
                        }
                    } else {
                        if (left == null) {
                            if (waitingLeft == NOT_WAITING) {
                                rightIdx = 0;
                                rightMaterialization = null;
                                drainMaterialization = false;
                            }

                            continue;
                        }

                        if (rightIdx >= rightMaterialization.size()) {
                            rightIdx = 0;
                            left = null;

                            continue;
                        }

                        RowT right = rightMaterialization.get(rightIdx++);

                        int cmp = comp.compare(left, right);

                        if (cmp > 0) {
                            rightIdx = 0;
                            rightMaterialization = null;
                            drainMaterialization = false;

                            continue;
                        }

                        leftMatched = true;

                        row = outputProjection.project(context(), left, right);
                    }

                    requested--;
                    downstream().push(row);
                }
            } finally {
                inLoop = false;
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty()
                    && waitingRight == NOT_WAITING && right == null && rightInBuf.isEmpty() && rightMaterialization == null
            ) {
                requested = 0;
                downstream().end();

                return;
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0) {
                leftSource().request(waitingLeft = inBufSize);
            }
        }
    }

    private static class SemiJoin<RowT> extends MergeJoinNode<RowT> {
        private RowT left;

        private RowT right;

        /**
         * Creates MergeJoinNode for SEMI JOIN operator.
         *
         * @param ctx Execution context.
         * @param comp Join expression comparator.
         */
        private SemiJoin(ExecutionContext<RowT> ctx, Comparator<RowT> comp) {
            super(ctx, comp);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            right = null;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            int processed = 0;
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty()) && (right != null || !rightInBuf.isEmpty())) {
                    if (processed++ > inBufSize) {
                        // Allow others to do their job.
                        execute(this::join);

                        return;
                    }

                    if (left == null) {
                        left = leftInBuf.remove();
                    }

                    if (right == null) {
                        right = rightInBuf.remove();
                    }

                    int cmp = comp.compare(left, right);

                    if (cmp < 0) {
                        left = null;

                        continue;
                    } else if (cmp > 0) {
                        right = null;

                        continue;
                    }

                    requested--;
                    downstream().push(left);

                    left = null;
                }
            } finally {
                inLoop = false;
            }

            if (requested > 0 && ((waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty()
                    || (waitingRight == NOT_WAITING && right == null && rightInBuf.isEmpty())))
            ) {
                requested = 0;
                downstream().end();

                return;
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0) {
                leftSource().request(waitingLeft = inBufSize);
            }
        }
    }

    private static class AntiJoin<RowT> extends MergeJoinNode<RowT> {
        private RowT left;

        private RowT right;

        /**
         * Creates MergeJoinNode for ANTI JOIN operator.
         *
         * @param ctx Execution context.
         * @param comp Join expression comparator.
         */
        private AntiJoin(ExecutionContext<RowT> ctx, Comparator<RowT> comp) {
            super(ctx, comp);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            left = null;
            right = null;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            int processed = 0;
            inLoop = true;
            try {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())
                        && !(right == null && rightInBuf.isEmpty() && waitingRight != NOT_WAITING)) {
                    if (processed++ > inBufSize) {
                        // Allow others to do their job.
                        execute(this::join);

                        return;
                    }

                    if (left == null) {
                        left = leftInBuf.remove();
                    }

                    if (right == null && !rightInBuf.isEmpty()) {
                        right = rightInBuf.remove();
                    }

                    if (right != null) {
                        int cmp = comp.compare(left, right);

                        if (cmp == 0) {
                            left = null;

                            continue;
                        } else if (cmp > 0) {
                            right = null;

                            continue;
                        }
                    }

                    requested--;
                    downstream().push(left);

                    left = null;
                }
            } finally {
                inLoop = false;
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();

                return;
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0) {
                leftSource().request(waitingLeft = inBufSize);
            }
        }
    }
}
