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

import static org.apache.ignite.internal.sql.engine.util.Commons.cast;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiPredicate;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.jetbrains.annotations.Nullable;

/** HashJoin implementor. */
public abstract class HashJoinNode<RowT> extends AbstractRightMaterializedJoinNode<RowT> {
    private static final int INITIAL_CAPACITY = 128;
    private static final BiPredicate<?, ?> ALWAYS_TRUE = (l, r) -> true;

    /** All keys with null-fields are mapped to this object. */
    private static final Key NULL_KEY = new Key();

    final Map<Key, TouchedCollection<RowT>> hashStore = new Object2ObjectOpenHashMap<>(INITIAL_CAPACITY);

    private final int[] leftJoinPositions;
    private final int[] rightJoinPositions;

    Iterator<RowT> rightIt = Collections.emptyIterator();

    /** Output row factory. */
    final RowFactory<RowT> outputRowFactory;

    final BiPredicate<RowT, RowT> nonEquiCondition;

    /**
     * Creates HashJoinNode.
     *
     * @param ctx Execution context.
     * @param joinInfo Join info.
     * @param outputRowFactory Output row factory.
     * @param nonEquiCondition Optional post-filtration predicate. If provided, only rows matching the predicate will be emitted as
     *         matched rows.
     */
    private HashJoinNode(
            ExecutionContext<RowT> ctx,
            JoinInfo joinInfo,
            RowFactory<RowT> outputRowFactory,
            @Nullable BiPredicate<RowT, RowT> nonEquiCondition
    ) {
        super(ctx);

        leftJoinPositions = joinInfo.leftKeys.toIntArray();
        rightJoinPositions = joinInfo.rightKeys.toIntArray();
        assert leftJoinPositions.length == rightJoinPositions.length;

        this.outputRowFactory = outputRowFactory;
        this.nonEquiCondition = nonEquiCondition != null
                ? nonEquiCondition
                : cast(ALWAYS_TRUE);
    }

    @Override
    protected void rewindInternal() {
        rightIt = Collections.emptyIterator();

        hashStore.clear();

        super.rewindInternal();
    }

    /** Supplied algorithm implementation. */
    public static <RowT> HashJoinNode<RowT> create(ExecutionContext<RowT> ctx, RelDataType outputRowType,
            RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, JoinInfo joinInfo,
            @Nullable BiPredicate<RowT, RowT> nonEquiCondition) {
        RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
        RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
        RowSchema outputRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(outputRowType));

        RowFactory<RowT> outputRowFactory = ctx.rowHandler().factory(outputRowSchema);

        switch (joinType) {
            case INNER:
                return new InnerHashJoin<>(ctx, joinInfo, outputRowFactory, nonEquiCondition);

            case LEFT: {
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftHashJoin<>(ctx, joinInfo, outputRowFactory, rightRowFactory, nonEquiCondition);
            }
            case RIGHT: {
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightHashJoin<>(ctx, joinInfo, outputRowFactory, leftRowFactory, nonEquiCondition);
            }
            case FULL: {
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterHashJoin<>(
                        ctx, joinInfo, outputRowFactory, leftRowFactory, rightRowFactory, nonEquiCondition
                );
            }
            case SEMI:
                return new SemiHashJoin<>(ctx, joinInfo, outputRowFactory, nonEquiCondition);

            case ANTI:
                return new AntiHashJoin<>(ctx, joinInfo, outputRowFactory, nonEquiCondition);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    private static class InnerHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates HashJoinNode for INNER JOIN operator.
         *
         * @param ctx Execution context.
         * @param joinInfo Join info.
         * @param outputRowFactory Output row factory.
         */
        private InnerHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RowFactory<RowT> outputRowFactory,
                @Nullable BiPredicate<RowT, RowT> nonEquiCondition
        ) {
            super(ctx, joinInfo, outputRowFactory, nonEquiCondition);
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        // Proceed with next left row, if previous was fully processed.
                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left);

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            // Emits matched rows.
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                if (!nonEquiCondition.test(left, right)) {
                                    continue;
                                }

                                --requested;

                                RowT row = outputRowFactory.concat(left, right);
                                downstream().push(row);

                                if (requested == 0) {
                                    break;
                                }
                            }
                        }

                        if (!rightIt.hasNext()) {
                            left = null;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    private static class LeftHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        /**
         * Creates HashJoinNode for LEFT OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param joinInfo Join info.
         * @param outputRowFactory Output row factory.
         * @param rightRowFactory Right row factory.
         */
        private LeftHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RowFactory<RowT> outputRowFactory,
                RowFactory<RowT> rightRowFactory,
                @Nullable BiPredicate<RowT, RowT> nonEquiCondition
        ) {
            super(ctx, joinInfo, outputRowFactory, nonEquiCondition);

            assert nonEquiCondition == null : "Non equi condition is not supported in LEFT join";

            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        // Proceed with next left row, if previous was fully processed.
                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left);

                            // Emit unmatched left row.
                            if (rightRows.isEmpty()) {
                                requested--;
                                downstream().push(outputRowFactory.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            // Emits matched rows.
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = outputRowFactory.concat(left, right);
                                downstream().push(row);

                                if (requested == 0) {
                                    break;
                                }
                            }
                        }

                        if (!rightIt.hasNext()) {
                            left = null;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    private static class RightHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        private boolean drainMaterialization;

        /**
         * Creates HashJoinNode for RIGHT OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param joinInfo Join info.
         * @param outputRowFactory Output row factory.
         * @param leftRowFactory Left row factory.
         */
        private RightHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RowFactory<RowT> outputRowFactory,
                RowFactory<RowT> leftRowFactory,
                @Nullable BiPredicate<RowT, RowT> nonEquiCondition
        ) {
            super(ctx, joinInfo, outputRowFactory, nonEquiCondition);

            assert nonEquiCondition == null : "Non equi condition is not supported in RIGHT join";

            this.leftRowFactory = leftRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            drainMaterialization = false;

            super.rewindInternal();
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        // Proceed with next left row, if previous was fully processed.
                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left);

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            // Emits matched rows.
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = outputRowFactory.concat(left, right);
                                downstream().push(row);

                                if (requested == 0) {
                                    break;
                                }
                            }
                        }

                        if (!rightIt.hasNext()) {
                            left = null;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            // Emit unmatched right rows.
            if (left == null && leftInBuf.isEmpty() && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && requested > 0) {
                inLoop = true;
                try {
                    if (!rightIt.hasNext() && !drainMaterialization) {
                        // Prevent scanning store more than once.
                        drainMaterialization = true;
                        rightIt = getUntouched(hashStore);
                    }

                    RowT emptyLeft = leftRowFactory.create();

                    while (rightIt.hasNext()) {
                        checkState();
                        RowT right = rightIt.next();
                        RowT row = outputRowFactory.concat(emptyLeft, right);
                        --requested;

                        downstream().push(row);

                        if (requested == 0) {
                            break;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }

        @Override
        protected boolean keepRowsWithNull() {
            return true;
        }
    }

    private static class FullOuterHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        private boolean drainMaterialization;

        /**
         * Creates HashJoinNode for FULL OUTER JOIN operator.
         *
         * @param ctx Execution context.
         * @param joinInfo Join info.
         * @param outputRowFactory Output row factory.
         * @param leftRowFactory Left row factory.
         * @param rightRowFactory Right row factory.
         */
        private FullOuterHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RowFactory<RowT> outputRowFactory,
                RowFactory<RowT> leftRowFactory,
                RowFactory<RowT> rightRowFactory,
                @Nullable BiPredicate<RowT, RowT> nonEquiCondition
        ) {
            super(ctx, joinInfo, outputRowFactory, nonEquiCondition);

            assert nonEquiCondition == null : "Non equi condition is not supported in FULL OUTER join";

            this.leftRowFactory = leftRowFactory;
            this.rightRowFactory = rightRowFactory;
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            drainMaterialization = false;

            super.rewindInternal();
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        // Proceed with next left row, if previous was fully processed.
                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left);

                            // Emit unmatched left row.
                            if (rightRows.isEmpty()) {
                                requested--;
                                downstream().push(outputRowFactory.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            // Emits matched rows.
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = outputRowFactory.concat(left, right);
                                downstream().push(row);

                                if (requested == 0) {
                                    break;
                                }
                            }
                        }

                        if (!rightIt.hasNext()) {
                            left = null;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            // Emit unmatched right rows.
            if (left == null && leftInBuf.isEmpty() && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && requested > 0) {
                inLoop = true;
                try {
                    if (!rightIt.hasNext() && !drainMaterialization) {
                        // Prevent scanning store more than once.
                        drainMaterialization = true;
                        rightIt = getUntouched(hashStore);
                    }

                    RowT emptyLeft = leftRowFactory.create();

                    while (rightIt.hasNext()) {
                        checkState();
                        RowT right = rightIt.next();
                        RowT row = outputRowFactory.concat(emptyLeft, right);
                        --requested;

                        downstream().push(row);

                        if (requested == 0) {
                            break;
                        }
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }

        @Override
        protected boolean keepRowsWithNull() {
            return true;
        }
    }

    private static class SemiHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates HashJoinNode for SEMI JOIN operator.
         *
         * @param ctx Execution context.
         * @param joinInfo Join info.
         * @param outputRowFactory Output row factory.
         */
        private SemiHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RowFactory<RowT> outputRowFactory,
                @Nullable BiPredicate<RowT, RowT> nonEquiCondition
        ) {
            super(ctx, joinInfo, outputRowFactory, nonEquiCondition);
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        left = leftInBuf.remove();

                        Collection<RowT> rightRows = lookup(left);

                        boolean anyMatched = !rightRows.isEmpty();

                        if (anyMatched && nonEquiCondition != ALWAYS_TRUE) {
                            anyMatched = false;
                            for (RowT right : rightRows) {
                                if (nonEquiCondition.test(left, right)) {
                                    anyMatched = true;

                                    break;
                                }
                            }
                        }

                        // Emit matched row.
                        if (anyMatched) {
                            requested--;

                            downstream().push(left);

                            if (requested == 0) {
                                break;
                            }
                        }

                        left = null;
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    private static class AntiHashJoin<RowT> extends HashJoinNode<RowT> {
        /**
         * Creates HashJoinNode for ANTI JOIN operator.
         *
         * @param ctx Execution context.
         * @param joinInfo Join info.
         * @param outputRowFactory Output row factory.
         */
        private AntiHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RowFactory<RowT> outputRowFactory,
                @Nullable BiPredicate<RowT, RowT> nonEquiCondition
        ) {
            super(ctx, joinInfo, outputRowFactory, nonEquiCondition);

            assert nonEquiCondition == null : "Non equi condition is not supported in ANTI join";
        }

        /** {@inheritDoc} */
        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        left = leftInBuf.remove();

                        Collection<RowT> rightRows = lookup(left);

                        if (rightRows.isEmpty()) {
                            requested--;

                            downstream().push(left);
                        }

                        left = null;
                    }
                } finally {
                    inLoop = false;
                }
            }

            getMoreOrEnd();
        }
    }

    Collection<RowT> lookup(RowT row) {
        Key row0 = extractKey(row, leftJoinPositions);

        if (row0 == NULL_KEY) {
            // Key with null field can't be compared with other keys.
            return Collections.emptyList();
        }

        TouchedCollection<RowT> found = hashStore.get(row0);

        if (found != null) {
            found.touched = true;

            return found.items();
        }

        return Collections.emptyList();
    }

    private static <RowT> Iterator<RowT> getUntouched(Map<Key, TouchedCollection<RowT>> entries) {
        return new Iterator<RowT>() {
            private final Iterator<TouchedCollection<RowT>> it = entries.values().iterator();
            private Iterator<RowT> innerIt = Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (innerIt.hasNext()) {
                    return true;
                }

                advance();

                return innerIt.hasNext();
            }

            @Override
            public RowT next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return innerIt.next();
            }

            void advance() {
                while (it.hasNext()) {
                    TouchedCollection<RowT> coll = it.next();
                    if (!coll.touched && !coll.items().isEmpty()) {
                        innerIt = coll.items().iterator();
                        break;
                    }
                }
            }
        };
    }

    @Override
    protected void pushRight(RowT row) throws Exception {
        assert downstream() != null;
        assert waitingRight > 0;

        checkState();

        waitingRight--;

        Key key = extractKey(row, rightJoinPositions);

        // No need to store the row in hashStore, if it contains NULL,
        // and we are not going to emit right part alone (like in RIGHT and FULL OUTER joins)
        if (keepRowsWithNull() || key != NULL_KEY) {
            TouchedCollection<RowT> raw = hashStore.computeIfAbsent(key, k -> new TouchedCollection<>());
            raw.add(row);
        }

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }

    private Key extractKey(RowT row, int[] mapping) {
        RowHandler<RowT> handler = context().rowHandler();

        for (int i : mapping) {
            if (handler.isNull(i, row)) {
                return NULL_KEY;
            }
        }

        return new RowWrapper<>(row, handler, mapping);
    }

    /** Non-comparable key object. */
    private static class Key {
    }

    /** Comparable key object. */
    private static class RowWrapper<RowT> extends Key {
        RowT row;
        RowHandler<RowT> handler;
        int[] items;

        RowWrapper(RowT row, RowHandler<RowT> handler, int[] items) {
            this.row = row;
            this.handler = handler;
            this.items = items;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            for (int i : items) {
                Object entHold = handler.get(i, row);
                hashCode += Objects.hashCode(entHold);
            }
            return hashCode;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            RowWrapper<RowT> row0 = (RowWrapper<RowT>) obj;
            for (int i = 0; i < items.length; ++i) {
                Object input = row0.handler.get(row0.items[i], row0.row);
                Object current = handler.get(items[i], row);
                boolean comp = Objects.equals(input, current);
                if (!comp) {
                    return comp;
                }
            }
            return true;
        }
    }

    void getMoreOrEnd() throws Exception {
        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }

        if (waitingLeft == 0 && leftInBuf.isEmpty()) {
            leftSource().request(waitingLeft = inBufSize);
        }

        if (requested > 0 && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && leftInBuf.isEmpty() && left == null
                && !rightIt.hasNext()) {
            requested = 0;
            hashStore.clear();
            downstream().end();
        }
    }

    /**
     * Returns {@code true} if we need to store the row from right shoulder even if it contains NULL in any of join key position.
     *
     * <p>This is required for joins which emit unmatched part of the right shoulder, such as RIGHT JOIN and FULL OUTER JOIN.
     *
     * @return {@code true} when row must be stored in {@link #hashStore} unconditionally.
     */
    protected boolean keepRowsWithNull() {
        return false;
    }

    private static class TouchedCollection<RowT> {
        Collection<RowT> coll;
        boolean touched;

        TouchedCollection() {
            this.coll = new ArrayList<>();
        }

        void add(RowT row) {
            coll.add(row);
        }

        Collection<RowT> items() {
            return coll;
        }
    }
}
