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

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.function.BiPredicate;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.jetbrains.annotations.Nullable;

/**
 * NestedLoopJoinNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class NestedLoopJoinNode<RowT> extends AbstractRightMaterializedJoinNode<RowT> {
    protected final BiPredicate<RowT, RowT> cond;

    final RowFactory<RowT> outputRowFactory;

    final List<RowT> rightMaterialized = new ArrayList<>(inBufSize);

    /**
     * Creates NestedLoopJoinNode.
     *
     * @param ctx Execution context.
     * @param cond Join expression.
     * @param outputRowFactory Output row factory.
     */
    NestedLoopJoinNode(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond, RowFactory<RowT> outputRowFactory) {
        super(ctx);

        this.cond = cond;
        this.outputRowFactory = outputRowFactory;
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        rightMaterialized.clear();

        super.rewindInternal();
    }

    @Override
    protected void pushRight(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        rightMaterialized.add(row);

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }

    /**
     * Create NestedLoopJoinNode for requested join operator type.
     *
     * @param ctx Execution context.
     * @param outputRowType Output row type.
     * @param leftRowType Row type of the left source.
     * @param rightRowType Row type of the right source.
     * @param joinType Join operator type.
     * @param cond Join condition predicate.
     */
    public static <RowT> NestedLoopJoinNode<RowT> create(
            ExecutionContext<RowT> ctx,
            RelDataType outputRowType,
            RelDataType leftRowType,
            RelDataType rightRowType,
            JoinRelType joinType,
            BiPredicate<RowT, RowT> cond
    ) {
        RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
        RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
        RowSchema outputSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(outputRowType));

        RowFactory<RowT> outputRowFactory = ctx.rowHandler().factory(outputSchema);

        switch (joinType) {
            case INNER:
                return new InnerJoin<>(ctx, cond, outputRowFactory);

            case LEFT: {
                RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftJoin<>(ctx, cond, outputRowFactory, rightRowFactory);
            }

            case RIGHT: {
                RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightJoin<>(ctx, cond, outputRowFactory, leftRowFactory);
            }

            case FULL: {
                RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterJoin<>(ctx, cond, outputRowFactory, leftRowFactory, rightRowFactory);
            }

            case SEMI:
                return new SemiJoin<>(ctx, cond, outputRowFactory);

            case ANTI:
                return new AntiJoin<>(ctx, cond, outputRowFactory);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    private static class InnerJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for INNER JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputRowFactory Output row factory.
         */
        private InnerJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond, RowFactory<RowT> outputRowFactory) {
            super(ctx, cond, outputRowFactory);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            rightIdx = 0;

            super.rewindInternal();
        }

        @Override
        protected void pushLeft(RowT row) throws Exception {
            // Prefent fetching left if right is empty.
            if (waitingRight == NOT_WAITING && rightMaterialized.isEmpty()) {
                waitingLeft--;

                if (waitingLeft == 0) {
                    waitingLeft = NOT_WAITING;
                    leftInBuf.clear();

                    join();
                }

                return;
            }

            super.pushLeft(row);
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                int processed = 0;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (processed++ > inBufSize) {
                                // Allow others to do their job.
                                execute(this::doJoin);

                                return;
                            }

                            if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                                continue;
                            }

                            requested--;
                            RowT row = outputRowFactory.concat(left, rightMaterialized.get(rightIdx - 1));
                            downstream().push(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    private static class LeftJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Right row factory. */
        private final RowFactory<RowT> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean matched;

        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for LEFT OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputRowFactory Output row factory.
         * @param rightRowFactory Right row factory.
         */
        private LeftJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                RowFactory<RowT> outputRowFactory,
                RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, cond, outputRowFactory);

            this.rightRowFactory = rightRowFactory;
        }

        @Override
        protected void rewindInternal() {
            matched = false;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                int processed = 0;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            matched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (processed++ > inBufSize) {
                                // Allow others to do their job.
                                execute(this::doJoin);

                                return;
                            }

                            if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                                continue;
                            }

                            requested--;
                            matched = true;

                            RowT row = outputRowFactory.concat(left, rightMaterialized.get(rightIdx - 1));
                            downstream().push(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!matched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                downstream().push(outputRowFactory.concat(left, rightRowFactory.create()));

                                processed++;
                            }

                            if (matched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }

                        if (processed > inBufSize) {
                            // Allow others to do their job.
                            execute(this::doJoin);

                            return;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    private static class RightJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Left row factory. */
        private final RowFactory<RowT> leftRowFactory;

        private @Nullable BitSet rightNotMatchedIndexes;

        private @Nullable PrimitiveIterator.OfInt rightNotMatchedIt;

        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for RIGHT OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputRowFactory Output row factory.
         * @param leftRowFactory Left row factory.
         */
        private RightJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                RowFactory<RowT> outputRowFactory,
                RowFactory<RowT> leftRowFactory
        ) {
            super(ctx, cond, outputRowFactory);

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            rightNotMatchedIndexes = null;
            rightNotMatchedIt = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        @Override
        protected void pushLeft(RowT row) throws Exception {
            // Prefent fetching left if right is empty.
            if (waitingRight == NOT_WAITING && rightMaterialized.isEmpty()) {
                waitingLeft--;

                if (waitingLeft == 0) {
                    waitingLeft = NOT_WAITING;
                    leftInBuf.clear();

                    join();
                }

                return;
            }

            super.pushLeft(row);
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatchedIndexes == null) {
                    rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                    rightNotMatchedIndexes.set(0, rightMaterialized.size());
                }

                inLoop = true;
                int processed = 0;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (processed++ > inBufSize) {
                                // Allow others to do their job.
                                execute(this::doJoin);

                                return;
                            }

                            RowT right = rightMaterialized.get(rightIdx++);

                            if (!cond.test(left, right)) {
                                continue;
                            }

                            requested--;
                            rightNotMatchedIndexes.clear(rightIdx - 1);

                            RowT joined = outputRowFactory.concat(left, right);
                            downstream().push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }
                } finally {
                    inLoop = false;
                }

                if (waitingLeft == NOT_WAITING && requested > 0) {
                    if (rightNotMatchedIt == null) {
                        rightNotMatchedIt = rightNotMatchedIndexes.stream().iterator();
                    }

                    inLoop = true;
                    try {
                        while (requested > 0 && rightNotMatchedIt.hasNext()) {
                            checkState();

                            if (processed++ > inBufSize) {
                                // Allow others to do their job.
                                execute(this::doJoin);

                                return;
                            }

                            int rowIdx = rightNotMatchedIt.nextInt();
                            RowT row = outputRowFactory.concat(leftRowFactory.create(), rightMaterialized.get(rowIdx));

                            requested--;
                            downstream().push(row);
                        }
                    } finally {
                        inLoop = false;
                    }
                }
            }

            getMoreOrEnd();
        }

        @Override
        protected boolean endOfRight() {
            return waitingRight == NOT_WAITING && (rightNotMatchedIt != null && !rightNotMatchedIt.hasNext());
        }
    }

    private static class FullOuterJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Left row factory. */
        private final RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowFactory<RowT> rightRowFactory;

        /** Whether current left row was matched or not. */
        private boolean leftMatched;

        private @Nullable BitSet rightNotMatchedIndexes;
        private @Nullable PrimitiveIterator.OfInt rightNotMatchedIt;

        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for FULL OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputRowFactory Output row factory.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         */
        private FullOuterJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                RowFactory<RowT> outputRowFactory,
                RowFactory<RowT> leftRowFactory,
                RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, cond, outputRowFactory);

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            leftMatched = false;
            rightNotMatchedIndexes = null;
            rightNotMatchedIt = null;
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                if (rightNotMatchedIndexes == null) {
                    rightNotMatchedIndexes = new BitSet(rightMaterialized.size());

                    rightNotMatchedIndexes.set(0, rightMaterialized.size());
                }

                inLoop = true;
                int processed = 0;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            leftMatched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (processed++ > inBufSize) {
                                // Allow others to do their job.
                                execute(this::doJoin);

                                return;
                            }

                            RowT right = rightMaterialized.get(rightIdx++);

                            if (!cond.test(left, right)) {
                                continue;
                            }

                            requested--;
                            leftMatched = true;
                            rightNotMatchedIndexes.clear(rightIdx - 1);

                            RowT joined = outputRowFactory.concat(left, right);
                            downstream().push(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!leftMatched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                downstream().push(outputRowFactory.concat(left, rightRowFactory.create()));
                                processed++;
                            }

                            if (leftMatched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }

                        if (processed >= inBufSize) {
                            // Allow others to do their job.
                            execute(this::doJoin);

                            return;
                        }
                    }
                } finally {
                    inLoop = false;
                }

                if (waitingLeft == NOT_WAITING && requested > 0) {
                    if (rightNotMatchedIt == null) {
                        rightNotMatchedIt = rightNotMatchedIndexes.stream().iterator();
                    }

                    inLoop = true;
                    try {
                        while (requested > 0 && rightNotMatchedIt.hasNext()) {
                            checkState();

                            int rowIdx = rightNotMatchedIt.nextInt();
                            RowT row = outputRowFactory.concat(leftRowFactory.create(), rightMaterialized.get(rowIdx));

                            requested--;
                            downstream().push(row);

                            if (processed++ >= inBufSize) {
                                // Allow others to do their job.
                                execute(this::doJoin);

                                return;
                            }
                        }
                    } finally {
                        inLoop = false;
                    }
                }
            }

            getMoreOrEnd();
        }

        @Override
        protected boolean endOfRight() {
            return waitingRight == NOT_WAITING && (rightNotMatchedIt != null && !rightNotMatchedIt.hasNext());
        }
    }

    private static class SemiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for SEMI JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputRowFactory Output row factory.
         */
        private SemiJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond, RowFactory<RowT> outputRowFactory) {
            super(ctx, cond, outputRowFactory);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            rightIdx = 0;

            super.rewindInternal();
        }

        @Override
        protected void pushLeft(RowT row) throws Exception {
            // Prefent fetching left if right is empty.
            if (waitingRight == NOT_WAITING && rightMaterialized.isEmpty()) {
                waitingLeft--;

                if (waitingLeft == 0) {
                    waitingLeft = NOT_WAITING;
                    leftInBuf.clear();

                    join();
                }

                return;
            }

            super.pushLeft(row);
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null) {
                        left = leftInBuf.remove();
                    }

                    boolean matched = false;

                    int processed = 0;
                    while (!matched && requested > 0 && rightIdx < rightMaterialized.size()) {
                        checkState();

                        if (processed++ > inBufSize) {
                            // Allow others to do their job.
                            execute(this::doJoin);

                            return;
                        }

                        if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                            continue;
                        }

                        requested--;
                        downstream().push(left);

                        matched = true;
                    }

                    if (matched || rightIdx == rightMaterialized.size()) {
                        left = null;
                        rightIdx = 0;
                    }
                }
            }

            getMoreOrEnd();
        }
    }

    private static class AntiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for ANTI JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputRowFactory Output row factory.
         */
        private AntiJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond, RowFactory<RowT> outputRowFactory) {
            super(ctx, cond, outputRowFactory);
        }

        @Override
        protected void rewindInternal() {
            rightIdx = 0;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                int processed = 0;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        boolean matched = false;

                        while (rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (processed++ > inBufSize) {
                                // Allow others to do their job.
                                execute(this::doJoin);

                                return;
                            }

                            if (cond.test(left, rightMaterialized.get(rightIdx++))) {
                                matched = true;
                                break;
                            }
                        }

                        if (!matched) {
                            requested--;
                            downstream().push(left);
                        }

                        left = null;
                        rightIdx = 0;

                        if (rightMaterialized.isEmpty() && processed++ >= inBufSize) {
                            // Allow others to do their job.
                            execute(this::doJoin);

                            return;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    void getMoreOrEnd() throws Exception {
        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }

        if (waitingLeft == 0 && leftInBuf.isEmpty()) {
            leftSource().request(waitingLeft = inBufSize);
        }

        if (requested > 0 && waitingLeft == NOT_WAITING && left == null && leftInBuf.isEmpty() && endOfRight()) {
            requested = 0;
            rightMaterialized.clear();
            downstream().end();
        }
    }

    protected boolean endOfRight() {
        return waitingRight == NOT_WAITING;
    }
}
