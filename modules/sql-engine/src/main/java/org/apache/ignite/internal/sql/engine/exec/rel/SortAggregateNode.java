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

import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorsState;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateRow;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;

/**
 * SortAggregateNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class SortAggregateNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    private final AggregateType type;

    private final RowFactory<RowT> rowFactory;

    private final ImmutableBitSet grpSet;

    private final Comparator<RowT> comp;

    private final Deque<RowT> outBuf = new ArrayDeque<>(inBufSize);

    private final List<AccumulatorWrapper<RowT>> accs;

    private RowT prevRow;

    private Group grp;

    private int requested;

    private int waiting;

    private int cmpRes;

    private boolean inLoop;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     * @param type Aggregation operation (phase) type.
     * @param grpSet Bit set of grouping fields.
     * @param accumulators Accumulators.
     * @param rowFactory Row factory.
     * @param comp Comparator.
     */
    public SortAggregateNode(
            ExecutionContext<RowT> ctx,
            AggregateType type,
            ImmutableBitSet grpSet,
            List<AccumulatorWrapper<RowT>> accumulators,
            RowFactory<RowT> rowFactory,
            Comparator<RowT> comp
    ) {
        super(ctx);
        assert Objects.nonNull(comp);

        this.type = type;
        this.rowFactory = rowFactory;
        this.grpSet = grpSet;
        this.comp = comp;
        this.accs = accumulators;

        init();
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        onRequestReceived();

        requested = rowsCnt;

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        } else if (!inLoop) {
            execute(this::doFlush);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        onRowReceived();

        waiting--;

        if (grp != null) {
            int cmp = comp.compare(row, prevRow);

            if (cmp == 0) {
                grp.add(row);
            } else {
                if (cmpRes == 0) {
                    cmpRes = cmp;
                } else {
                    assert Integer.signum(cmp) == Integer.signum(cmpRes) : "Input not sorted";
                }

                outBuf.add(grp.row());

                grp = newGroup(row);

                flush();
            }
        } else {
            grp = newGroup(row);
        }

        prevRow = row;

        if (waiting == 0 && requested > 0) {
            waiting = inBufSize;

            source().request(inBufSize);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        waiting = NOT_WAITING;

        if (grp != null) {
            outBuf.add(grp.row());
        }

        flush();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        grp = null;
        prevRow = null;

        init();
    }

    private void init() {
        // Initializes aggregates for case when no any rows will be added into the aggregate to have 0 as result.
        // Doesn't do it for MAP type due to we don't want send from MAP node zero results because it looks redundant.
        if (AggregateRow.addEmptyGroup(grpSet, type)) {
            grp = new Group(OBJECT_EMPTY_ARRAY);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    @Override
    protected void dumpDebugInfo0(IgniteStringBuilder buf) {
        buf.app("class=").app(getClass().getSimpleName())
                .app(", requested=").app(requested)
                .app(", waiting=").app(waiting);
    }

    private Group newGroup(RowT r) {
        RowHandler<RowT> rowHandler = context().rowAccessor();
        ObjectArrayList<Object> grpKeys = new ObjectArrayList<>(grpSet.cardinality());

        grpSet.forEachInt(fldIdx -> grpKeys.add(rowHandler.get(fldIdx, r)));

        Group grp = new Group(grpKeys.elements());

        grp.add(r);

        return grp;
    }

    private void doFlush() throws Exception {
        flush();
    }

    private void flush() throws Exception {
        inLoop = true;
        try {
            while (requested > 0 && !outBuf.isEmpty()) {
                requested--;

                downstream().push(outBuf.poll());
            }
        } finally {
            inLoop = false;
        }

        if (requested > 0 && waiting == NOT_WAITING && outBuf.isEmpty()) {
            requested = 0;
            downstream().end();

            grp = null;
            prevRow = null;
        }
    }

    private class Group {

        private final Object[] grpKeys;

        private final AggregateRow<RowT> aggRow;

        private Group(Object[] grpKeys) {
            this.grpKeys = grpKeys;

            AccumulatorsState state = new AccumulatorsState(accs.size());

            Int2ObjectMap<Set<Object>> distinctSets = new Int2ObjectArrayMap<>();
            for (int i = 0; i < accs.size(); i++) {
                AccumulatorWrapper<RowT> acc = accs.get(i);
                if (acc.isDistinct()) {
                    distinctSets.put(i, new HashSet<>());
                }
            }

            aggRow = new AggregateRow<>(state, distinctSets);
        }

        private void add(RowT row) {
            aggRow.update(accs, grpSet, context().rowAccessor(), row);
        }

        private RowT row() {
            Object[] fields = aggRow.createOutput(type, accs, grpSet, AggregateRow.NO_GROUP_ID);

            int i = 0;

            for (Object grpKey : grpKeys) {
                fields[i++] = grpKey;
            }

            aggRow.writeTo(type, accs, fields, grpSet.cardinality(), grpSet, AggregateRow.NO_GROUP_ID);

            return rowFactory.create(fields);
        }
    }
}
