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
     * @param ctx  Execution context.
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
     * Create.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static <RowT> NestedLoopJoinNode<RowT> create(ExecutionContext<RowT> ctx, RelDataType outputRowType,
            RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, BiPredicate<RowT, RowT> cond) {
        RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
        RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));

        switch (joinType) {
            case INNER: {
                RowSchema outputSchema = RowSchema.concat(leftRowSchema, rightRowSchema);
                RowFactory<RowT> outputRowFactory = ctx.rowHandler().factory(outputSchema);

                return new InnerJoin<>(ctx, cond, outputRowFactory);
            }

            case LEFT: {
                RowSchema outputSchema = RowSchema.concat(leftRowSchema, rightRowSchema);
                RowFactory<RowT> outputRowFactory = ctx.rowHandler().factory(outputSchema);
                RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftJoin<>(ctx, cond, outputRowFactory, rightRowFactory);
            }

            case RIGHT: {
                RowSchema outputSchema = RowSchema.concat(leftRowSchema, rightRowSchema);
                RowFactory<RowT> outputRowFactory = ctx.rowHandler().factory(outputSchema);
                RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightJoin<>(ctx, cond, outputRowFactory, leftRowFactory);
            }

            case FULL: {
                RowSchema outputSchema = RowSchema.concat(leftRowSchema, rightRowSchema);
                RowFactory<RowT> outputRowFactory = ctx.rowHandler().factory(outputSchema);
                RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterJoin<>(ctx, cond, outputRowFactory, leftRowFactory, rightRowFactory);
            }

            case SEMI: {
                RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new SemiJoin<>(ctx, cond, leftRowFactory);
            }

            case ANTI: {
                RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new AntiJoin<>(ctx, cond, leftRowFactory);
            }

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
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

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

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
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
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            matched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

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
                            }

                            if (matched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    private static class RightJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Left row factory. */
        private final RowFactory<RowT> leftRowFactory;

        private @Nullable BitSet rightNotMatchedIndexes;

        private int lastPushedInd;

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
            lastPushedInd = 0;
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
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

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
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && (rightNotMatchedIndexes != null && !rightNotMatchedIndexes.isEmpty())) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd); ;
                            lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        checkState();

                        if (lastPushedInd < 0) {
                            break;
                        }

                        RowT row = outputRowFactory.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream().push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0) {
                            break;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream().end();
            }
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

        private int lastPushedInd;

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
            lastPushedInd = 0;
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
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            leftMatched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            checkState();

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
                            }

                            if (leftMatched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingLeft == NOT_WAITING && requested > 0 && (rightNotMatchedIndexes != null && !rightNotMatchedIndexes.isEmpty())) {
                assert lastPushedInd >= 0;

                inLoop = true;
                try {
                    for (lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd); ;
                            lastPushedInd = rightNotMatchedIndexes.nextSetBit(lastPushedInd + 1)
                    ) {
                        checkState();

                        if (lastPushedInd < 0) {
                            break;
                        }

                        RowT row = outputRowFactory.concat(leftRowFactory.create(), rightMaterialized.get(lastPushedInd));

                        rightNotMatchedIndexes.clear(lastPushedInd);

                        requested--;
                        downstream().push(row);

                        if (lastPushedInd == Integer.MAX_VALUE || requested <= 0) {
                            break;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty() && rightNotMatchedIndexes.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }

    private static class SemiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for SEMI JOIN operator.
         *
         * @param ctx  Execution context.
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

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                    if (left == null) {
                        left = leftInBuf.remove();
                    }

                    boolean matched = false;

                    while (!matched && requested > 0 && rightIdx < rightMaterialized.size()) {
                        checkState();

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

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null
                    && leftInBuf.isEmpty()) {
                downstream().end();
                requested = 0;
            }
        }
    }

    private static class AntiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for ANTI JOIN operator.
         *
         * @param ctx  Execution context.
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
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        boolean matched = false;

                        while (!matched && rightIdx < rightMaterialized.size()) {
                            checkState();

                            if (cond.test(left, rightMaterialized.get(rightIdx++))) {
                                matched = true;
                            }
                        }

                        if (!matched) {
                            requested--;
                            downstream().push(left);
                        }

                        left = null;
                        rightIdx = 0;
                    }
                } finally {
                    inLoop = false;
                }
            }

            if (waitingRight == 0) {
                rightSource().request(waitingRight = inBufSize);
            }

            if (waitingLeft == 0 && leftInBuf.isEmpty()) {
                leftSource().request(waitingLeft = inBufSize);
            }

            if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && left == null && leftInBuf.isEmpty()) {
                requested = 0;
                downstream().end();
            }
        }
    }
}
