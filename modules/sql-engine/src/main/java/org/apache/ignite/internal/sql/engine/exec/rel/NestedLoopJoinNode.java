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
import org.apache.ignite.internal.sql.engine.exec.exp.SqlJoinProjection;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.jetbrains.annotations.Nullable;

/**
 * NestedLoopJoinNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public abstract class NestedLoopJoinNode<RowT> extends AbstractRightMaterializedJoinNode<RowT> {
    protected final BiPredicate<RowT, RowT> cond;

    final List<RowT> rightMaterialized = new ArrayList<>(inBufSize);

    /**
     * Creates NestedLoopJoinNode.
     *
     * @param ctx Execution context.
     * @param cond Join expression.
     */
    NestedLoopJoinNode(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond) {
        super(ctx);

        this.cond = cond;
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

        waitingRight--;

        rightMaterialized.add(row);

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }

    @Override
    protected void pushRight(List<RowT> batch) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        waitingRight -= batch.size();
        rightMaterialized.addAll(batch);

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }

    /**
     * Create NestedLoopJoinNode for requested join operator type.
     *
     * @param ctx Execution context.
     * @param joinProjection Join projection.
     * @param leftRowType Row type of the left source.
     * @param rightRowType Row type of the right source.
     * @param joinType Join operator type.
     * @param cond Join condition predicate.
     */
    public static <RowT> NestedLoopJoinNode<RowT> create(
            ExecutionContext<RowT> ctx,
            @Nullable SqlJoinProjection<RowT> joinProjection,
            RelDataType leftRowType,
            RelDataType rightRowType,
            JoinRelType joinType,
            BiPredicate<RowT, RowT> cond
    ) {
        switch (joinType) {
            case INNER:
                assert joinProjection != null;

                return new InnerJoin<>(ctx, cond, joinProjection);

            case LEFT: {
                assert joinProjection != null;

                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftJoin<>(ctx, cond, joinProjection, rightRowFactory);
            }

            case RIGHT: {
                assert joinProjection != null;

                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightJoin<>(ctx, cond, joinProjection, leftRowFactory);
            }

            case FULL: {
                assert joinProjection != null;

                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));

                RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterJoin<>(ctx, cond, joinProjection, leftRowFactory, rightRowFactory);
            }

            case SEMI:
                assert joinProjection == null;

                return new SemiJoin<>(ctx, cond);

            case ANTI:
                assert joinProjection == null;

                return new AntiJoin<>(ctx, cond);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    private static class InnerJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private final SqlJoinProjection<RowT> outputProjection;

        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for INNER JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputProjection Output projection.
         */
        private InnerJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond, SqlJoinProjection<RowT> outputProjection) {
            super(ctx, cond);

            this.outputProjection = outputProjection;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            rightIdx = 0;

            super.rewindInternal();
        }

        @Override
        protected void pushLeft(List<RowT> rows) throws Exception {
            // Prefent fetching left if right is empty.
            if (waitingRight == NOT_WAITING && rightMaterialized.isEmpty()) {
                waitingLeft -= rows.size();

                if (waitingLeft == 0) {
                    waitingLeft = NOT_WAITING;
                    leftInBuf.clear();

                    join();
                }

                return;
            }

            super.pushLeft(rows);
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                int processed = 0;
                try {
                    List<RowT> batch = newBatch();
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            if (processed++ > inBufSize) {
                                if (!batch.isEmpty()) {
                                    downstream().push(batch);
                                }
                                // Allow others to do their job.
                                execute(this::join);

                                return;
                            }

                            if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                                continue;
                            }

                            requested--;
                            RowT row = outputProjection.project(context(), left, rightMaterialized.get(rightIdx - 1));
                            batch.add(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }

                    if (!batch.isEmpty()) {
                        downstream().push(batch);

                        if (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                            execute(this::join);

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

    private static class LeftJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Right row factory. */
        private final RowFactory<RowT> rightRowFactory;

        private final SqlJoinProjection<RowT> outputProjection;

        /** Whether current left row was matched or not. */
        private boolean matched;

        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for LEFT OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputProjection Output projection.
         * @param rightRowFactory Right row factory.
         */
        private LeftJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                SqlJoinProjection<RowT> outputProjection,
                RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, cond);

            this.outputProjection = outputProjection;
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
                    List<RowT> batch = newBatch();
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            matched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            if (processed++ > inBufSize) {
                                if (!batch.isEmpty()) {
                                    downstream().push(batch);
                                }
                                // Allow others to do their job.
                                execute(this::join);

                                return;
                            }

                            if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                                continue;
                            }

                            requested--;
                            matched = true;

                            RowT row = outputProjection.project(context(), left, rightMaterialized.get(rightIdx - 1));
                            batch.add(row);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!matched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                batch.add(outputProjection.project(context(), left, rightRowFactory.create()));
                            }

                            if (matched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }

                    if (!batch.isEmpty()) {
                        downstream().push(batch);

                        if (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                            execute(this::join);

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
        private final SqlJoinProjection<RowT> outputProjection;

        private @Nullable BitSet rightNotMatchedIndexes;

        private @Nullable PrimitiveIterator.OfInt rightNotMatchedIt;

        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for RIGHT OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         * @param outputProjection Output projection.
         * @param leftRowFactory Left row factory.
         */
        private RightJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                SqlJoinProjection<RowT> outputProjection,
                RowFactory<RowT> leftRowFactory
        ) {
            super(ctx, cond);

            this.outputProjection = outputProjection;
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
        protected void pushLeft(List<RowT> rows) throws Exception {
            // Prefent fetching left if right is empty.
            if (waitingRight == NOT_WAITING && rightMaterialized.isEmpty()) {
                waitingLeft -= rows.size();

                if (waitingLeft == 0) {
                    waitingLeft = NOT_WAITING;
                    leftInBuf.clear();

                    join();
                }

                return;
            }

            super.pushLeft(rows);
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
                    List<RowT> batch = newBatch();
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            if (processed++ > inBufSize) {
                                if (!batch.isEmpty()) {
                                    downstream().push(batch);
                                }
                                // Allow others to do their job.
                                execute(this::join);

                                return;
                            }

                            RowT right = rightMaterialized.get(rightIdx++);

                            if (!cond.test(left, right)) {
                                continue;
                            }

                            requested--;
                            rightNotMatchedIndexes.clear(rightIdx - 1);

                            RowT joined = outputProjection.project(context(), left, right);
                            batch.add(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }

                    if (!batch.isEmpty()) {
                        downstream().push(batch);

                        if (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                            execute(this::join);

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
                        List<RowT> batch = newBatch();
                        while (requested > 0 && rightNotMatchedIt.hasNext()) {
                            int rowIdx = rightNotMatchedIt.nextInt();
                            RowT row = outputProjection.project(context(), leftRowFactory.create(), rightMaterialized.get(rowIdx));

                            requested--;
                            batch.add(row);
                        }

                        if (!batch.isEmpty()) {
                            downstream().push(batch);

                            if (requested > 0 && rightNotMatchedIt.hasNext()) {
                                execute(this::join);

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

    private static class FullOuterJoin<RowT> extends NestedLoopJoinNode<RowT> {
        /** Left row factory. */
        private final RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowFactory<RowT> rightRowFactory;

        private final SqlJoinProjection<RowT> outputProjection;

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
         * @param outputProjection Output projection.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         */
        private FullOuterJoin(
                ExecutionContext<RowT> ctx,
                BiPredicate<RowT, RowT> cond,
                SqlJoinProjection<RowT> outputProjection,
                RowFactory<RowT> leftRowFactory,
                RowFactory<RowT> rightRowFactory
        ) {
            super(ctx, cond);

            this.outputProjection = outputProjection;
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
                    List<RowT> batch = newBatch();
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();

                            leftMatched = false;
                        }

                        while (requested > 0 && rightIdx < rightMaterialized.size()) {
                            if (processed++ > inBufSize) {
                                if (!batch.isEmpty()) {
                                    downstream().push(batch);
                                }
                                // Allow others to do their job.
                                execute(this::join);

                                return;
                            }

                            RowT right = rightMaterialized.get(rightIdx++);

                            if (!cond.test(left, right)) {
                                continue;
                            }

                            requested--;
                            leftMatched = true;
                            rightNotMatchedIndexes.clear(rightIdx - 1);

                            RowT joined = outputProjection.project(context(), left, right);
                            batch.add(joined);
                        }

                        if (rightIdx == rightMaterialized.size()) {
                            boolean wasPushed = false;

                            if (!leftMatched && requested > 0) {
                                requested--;
                                wasPushed = true;

                                batch.add(outputProjection.project(context(), left, rightRowFactory.create()));
                                processed++;
                            }

                            if (leftMatched || wasPushed) {
                                left = null;
                                rightIdx = 0;
                            }
                        }
                    }

                    if (!batch.isEmpty()) {
                        downstream().push(batch);

                        if (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                            execute(this::join);

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
                        List<RowT> batch = newBatch();
                        while (requested > 0 && rightNotMatchedIt.hasNext()) {
                            int rowIdx = rightNotMatchedIt.nextInt();
                            RowT row = outputProjection.project(context(), leftRowFactory.create(), rightMaterialized.get(rowIdx));

                            requested--;
                            batch.add(row);
                        }

                        if (!batch.isEmpty()) {
                            downstream().push(batch);

                            if (requested > 0 && rightNotMatchedIt.hasNext()) {
                                execute(this::join);

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
         */
        private SemiJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond) {
            super(ctx, cond);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            rightIdx = 0;

            super.rewindInternal();
        }

        @Override
        protected void pushLeft(List<RowT> rows) throws Exception {
            // Prefent fetching left if right is empty.
            if (waitingRight == NOT_WAITING && rightMaterialized.isEmpty()) {
                waitingLeft -= rows.size();

                if (waitingLeft == 0) {
                    waitingLeft = NOT_WAITING;
                    leftInBuf.clear();

                    join();
                }

                return;
            }

            super.pushLeft(rows);
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    int processed = 0;
                    List<RowT> batch = newBatch();
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        boolean matched = false;

                        while (!matched && requested > 0 && rightIdx < rightMaterialized.size()) {
                            if (processed++ > inBufSize) {
                                if (!batch.isEmpty()) {
                                    downstream().push(batch);
                                }
                                // Allow others to do their job.
                                execute(this::join);

                                return;
                            }

                            if (!cond.test(left, rightMaterialized.get(rightIdx++))) {
                                continue;
                            }

                            requested--;
                            batch.add(left);

                            matched = true;
                        }

                        if (matched || rightIdx == rightMaterialized.size()) {
                            left = null;
                            rightIdx = 0;
                        }
                    }

                    if (!batch.isEmpty()) {
                        downstream().push(batch);

                        if (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                            execute(this::join);

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

    private static class AntiJoin<RowT> extends NestedLoopJoinNode<RowT> {
        private int rightIdx;

        /**
         * Creates NestedLoopJoinNode for ANTI JOIN operator.
         *
         * @param ctx Execution context.
         * @param cond Join expression.
         */
        private AntiJoin(ExecutionContext<RowT> ctx, BiPredicate<RowT, RowT> cond) {
            super(ctx, cond);
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
                    List<RowT> batch = newBatch();
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (left == null) {
                            left = leftInBuf.remove();
                        }

                        boolean matched = false;

                        while (rightIdx < rightMaterialized.size()) {
                            if (processed++ > inBufSize) {
                                if (!batch.isEmpty()) {
                                    downstream().push(batch);
                                }
                                // Allow others to do their job.
                                execute(this::join);

                                return;
                            }

                            if (cond.test(left, rightMaterialized.get(rightIdx++))) {
                                matched = true;
                                break;
                            }
                        }

                        if (!matched) {
                            requested--;
                            batch.add(left);
                        }

                        left = null;
                        rightIdx = 0;
                    }

                    if (!batch.isEmpty()) {
                        downstream().push(batch);

                        if (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                            execute(this::join);

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
