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

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;

/** HashJoin implementor. */
public abstract class HashJoinNode<RowT> extends AbstractRightMaterializedJoinNode<RowT> {
    final Map<RowWrapper<RowT>, TouchedCollection<RowT>> hashStore = new Object2ObjectOpenHashMap<>();
    protected final RowHandler<RowT> handler;

    private final List<Integer> leftJoinPositions;
    private final List<Integer> rightJoinPositions;

    final boolean touchResults;

    Iterator<RowT> rightIt = Collections.emptyIterator();
    private final RowSchema rightJoinRelatedRowSchema;
    private final RowSchema leftJoinRelatedRowSchema;

    private final RowBuilder<RowT> leftRowBuilder;
    private final RowBuilder<RowT> rightRowBuilder;

    private HashJoinNode(ExecutionContext<RowT> ctx, JoinInfo joinInfo, boolean touch,
            RelDataType leftRowType, RelDataType rightRowType) {
        super(ctx);

        handler = ctx.rowHandler();
        touchResults = touch;

        leftJoinPositions = joinInfo.leftKeys.toIntegerList();
        rightJoinPositions = joinInfo.rightKeys.toIntegerList();

        assert leftJoinPositions.size() == rightJoinPositions.size();

        ImmutableIntList rightKeys = joinInfo.rightKeys;
        List<RelDataType> rightTypes = new ArrayList<>(rightKeys.size());
        List<RelDataTypeField> rightFields = rightRowType.getFieldList();
        for (int rightPos : rightKeys) {
            rightTypes.add(rightFields.get(rightPos).getType());
        }
        rightJoinRelatedRowSchema = rowSchemaFromRelTypes(rightTypes);

        ImmutableIntList leftKeys = joinInfo.leftKeys;
        List<RelDataType> leftTypes = new ArrayList<>(leftKeys.size());
        List<RelDataTypeField> leftFields = leftRowType.getFieldList();
        for (int leftPos : leftKeys) {
            leftTypes.add(leftFields.get(leftPos).getType());
        }
        leftJoinRelatedRowSchema = rowSchemaFromRelTypes(leftTypes);

        RowFactory<RowT> leftRowFactory = handler.factory(leftJoinRelatedRowSchema);
        leftRowBuilder = leftRowFactory.rowBuilder();

        RowFactory<RowT> rightRowFactory = handler.factory(rightJoinRelatedRowSchema);
        rightRowBuilder = rightRowFactory.rowBuilder();
    }

    @Override
    protected void rewindInternal() {
        rightIt = Collections.emptyIterator();

        hashStore.clear();

        super.rewindInternal();
    }

    /** Supplied algorithm implementation. */
    public static <RowT> HashJoinNode<RowT> create(ExecutionContext<RowT> ctx, RelDataType outputRowType,
            RelDataType leftRowType, RelDataType rightRowType, JoinRelType joinType, JoinInfo joinInfo) {

        switch (joinType) {
            case INNER:
                return new InnerHashJoin<>(ctx, joinInfo, leftRowType, rightRowType);

            case LEFT: {
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new LeftHashJoin<>(ctx, rightRowFactory, joinInfo, leftRowType, rightRowType);
            }

            case RIGHT: {
                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);

                return new RightHashJoin<>(ctx, leftRowFactory, joinInfo, leftRowType, rightRowType);
            }

            case FULL: {
                RowSchema leftRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(leftRowType));
                RowSchema rightRowSchema = rowSchemaFromRelTypes(RelOptUtil.getFieldTypeList(rightRowType));
                RowHandler.RowFactory<RowT> leftRowFactory = ctx.rowHandler().factory(leftRowSchema);
                RowHandler.RowFactory<RowT> rightRowFactory = ctx.rowHandler().factory(rightRowSchema);

                return new FullOuterHashJoin<>(ctx, leftRowFactory, rightRowFactory, joinInfo, leftRowType, rightRowType);
            }

            case SEMI:
                return new SemiHashJoin<>(ctx, joinInfo, leftRowType, rightRowType);

            case ANTI:
                return new AntiHashJoin<>(ctx, joinInfo, leftRowType, rightRowType);

            default:
                throw new IllegalStateException("Join type \"" + joinType + "\" is not supported yet");
        }
    }

    private static class InnerHashJoin<RowT> extends HashJoinNode<RowT> {
        private InnerHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RelDataType leftRowType,
                RelDataType rightRowType
        ) {
            super(ctx, joinInfo, false, leftRowType, rightRowType);
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left, touchResults);

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = handler.concat(left, right);
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

        private LeftHashJoin(
                ExecutionContext<RowT> ctx,
                RowHandler.RowFactory<RowT> rightRowFactory,
                JoinInfo joinInfo,
                RelDataType leftRowType,
                RelDataType rightRowType
        ) {
            super(ctx, joinInfo, false, leftRowType, rightRowType);

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

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left, touchResults);

                            if (rightRows.isEmpty()) {
                                requested--;
                                downstream().push(handler.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = handler.concat(left, right);
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

        private RightHashJoin(
                ExecutionContext<RowT> ctx,
                RowHandler.RowFactory<RowT> leftRowFactory,
                JoinInfo joinInfo,
                RelDataType leftRowType,
                RelDataType rightRowType
        ) {
            super(ctx, joinInfo, true, leftRowType, rightRowType);

            this.leftRowFactory = leftRowFactory;
        }

        @Override
        protected void join() throws Exception {
            if (waitingRight == NOT_WAITING) {
                inLoop = true;
                try {
                    while (requested > 0 && (left != null || !leftInBuf.isEmpty())) {
                        checkState();

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left, touchResults);

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = handler.concat(left, right);
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

            if (left == null && leftInBuf.isEmpty() && waitingLeft == NOT_WAITING && waitingRight == NOT_WAITING && requested > 0) {
                inLoop = true;
                try {
                    if (!rightIt.hasNext()) {
                        rightIt = getUntouched(hashStore);
                    }

                    RowT emptyLeft = leftRowFactory.create();

                    while (rightIt.hasNext()) {
                        checkState();
                        RowT right = rightIt.next();
                        RowT row = handler.concat(emptyLeft, right);
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
    }

    private static class FullOuterHashJoin<RowT> extends HashJoinNode<RowT> {
        /** Left row factory. */
        private final RowHandler.RowFactory<RowT> leftRowFactory;

        /** Right row factory. */
        private final RowHandler.RowFactory<RowT> rightRowFactory;

        private FullOuterHashJoin(
                ExecutionContext<RowT> ctx,
                RowHandler.RowFactory<RowT> leftRowFactory,
                RowHandler.RowFactory<RowT> rightRowFactory,
                JoinInfo joinInfo,
                RelDataType leftRowType,
                RelDataType rightRowType
        ) {
            super(ctx, joinInfo, true, leftRowType, rightRowType);

            this.leftRowFactory = leftRowFactory;
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

                        if (!rightIt.hasNext()) {
                            left = leftInBuf.remove();

                            Collection<RowT> rightRows = lookup(left, touchResults);

                            if (rightRows.isEmpty()) {
                                requested--;
                                downstream().push(handler.concat(left, rightRowFactory.create()));
                            }

                            rightIt = rightRows.iterator();
                        }

                        if (rightIt.hasNext()) {
                            while (rightIt.hasNext()) {
                                checkState();

                                RowT right = rightIt.next();

                                --requested;

                                RowT row = handler.concat(left, right);
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

            if (left == null && !rightIt.hasNext() && leftInBuf.isEmpty() && waitingLeft == NOT_WAITING
                    && waitingRight == NOT_WAITING && requested > 0) {
                inLoop = true;
                try {
                    if (!rightIt.hasNext()) {
                        rightIt = getUntouched(hashStore);
                    }

                    RowT emptyLeft = leftRowFactory.create();

                    while (rightIt.hasNext()) {
                        checkState();
                        RowT right = rightIt.next();
                        RowT row = handler.concat(emptyLeft, right);
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
    }

    private static class SemiHashJoin<RowT> extends HashJoinNode<RowT> {
        private SemiHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RelDataType leftRowType,
                RelDataType rightRowType
        ) {
            super(ctx, joinInfo, false, leftRowType, rightRowType);
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

                        Collection<RowT> rightRows = lookup(left, touchResults);

                        if (!rightRows.isEmpty()) {
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
        private AntiHashJoin(
                ExecutionContext<RowT> ctx,
                JoinInfo joinInfo,
                RelDataType leftRowType,
                RelDataType rightRowType
        ) {
            super(ctx, joinInfo, false, leftRowType, rightRowType);
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

                        Collection<RowT> rightRows = lookup(left, touchResults);

                        if (rightRows.isEmpty()) {
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

    Collection<RowT> lookup(RowT row, boolean processTouched) {
        Collection<RowT> coll = Collections.emptyList();

        for (Integer entry : leftJoinPositions) {
            Object ent = handler.get(entry, row);

            if (ent == null) {
                leftRowBuilder.reset();
                return Collections.emptyList();
            }

            leftRowBuilder.addField(ent);
        }

        RowWrapper<RowT> row0 = new RowWrapper<>(leftRowBuilder.buildAndReset(), handler, leftJoinPositions.size());

        TouchedCollection<RowT> found = hashStore.get(row0);

        if (found != null) {
            coll = found.items();

            if (processTouched) {
                found.touched = true;
            }
        }

        return coll;
    }

    private static <RowT> Iterator<RowT> getUntouched(Map<RowWrapper<RowT>, TouchedCollection<RowT>> entries) {
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

        for (Integer entry : rightJoinPositions) {
            Object ent = handler.get(entry, row);
            rightRowBuilder.addField(ent);
        }

        RowWrapper<RowT> row0 = new RowWrapper<>(rightRowBuilder.buildAndReset(), handler, rightJoinPositions.size());
        TouchedCollection<RowT> raw = hashStore.computeIfAbsent(row0, k -> new TouchedCollection<>());
        raw.add(row);

        if (waitingRight == 0) {
            rightSource().request(waitingRight = inBufSize);
        }
    }

    private static class RowWrapper<RowT> {
        RowT row;
        RowHandler<RowT> handler;
        int itemsCount;

        RowWrapper(RowT row, RowHandler<RowT> handler, int itemsCount) {
            this.row = row;
            this.handler = handler;
            this.itemsCount = itemsCount;
        }

        @Override
        public int hashCode() {
            int hashCode = 0;
            for (int i = 0; i < itemsCount; ++i) {
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
            for (int i = 0; i < itemsCount; ++i) {
                Object input = handler.get(i, row0.row);
                Object current = handler.get(i, row);
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
            downstream().end();
        }
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
