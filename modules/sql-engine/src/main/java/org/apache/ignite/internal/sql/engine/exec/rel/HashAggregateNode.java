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

import static java.util.stream.Collectors.toCollection;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorWrapper;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AccumulatorsState;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateRow;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.GroupKey;

/**
 * HashAggregateNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class HashAggregateNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    private final AggregateType type;

    private final RowFactory<RowT> rowFactory;

    /** A bit set that contains fields included in all grouping sets. */
    private final ImmutableBitSet allFields;

    private final List<Grouping> groupings;

    private final List<AccumulatorWrapper<RowT>> accs;

    private int requested;

    private int waiting;

    private boolean inLoop;

    /**
     * Constructor.
     *
     * @param ctx Execution context.
     */
    public HashAggregateNode(
            ExecutionContext<RowT> ctx, AggregateType type, List<ImmutableBitSet> grpSets,
            List<AccumulatorWrapper<RowT>> accumulators, RowFactory<RowT> rowFactory) {
        super(ctx);

        this.type = type;
        this.rowFactory = rowFactory;

        assert grpSets.size() <= Byte.MAX_VALUE : "Too many grouping sets";

        ImmutableBitSet.Builder b = ImmutableBitSet.builder();

        groupings = new ArrayList<>(grpSets.size());
        accs = accumulators;

        for (byte i = 0; i < grpSets.size(); i++) {
            ImmutableBitSet grpFields = grpSets.get(i);
            b.addAll(grpFields);

            Grouping grouping = new Grouping(i, grpFields);
            grouping.init();
            groupings.add(grouping);
        }

        allFields = b.build();
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        requested = rowsCnt;

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        } else if (!inLoop) {
            this.execute(this::flush);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(List<RowT> batch) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        waiting -= batch.size();

        for (Grouping grouping : groupings) {
            batch.forEach(grouping::add);
        }

        if (waiting == 0) {
            source().request(waiting = inBufSize);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        waiting = NOT_WAITING;

        flush();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        groupings.forEach(Grouping::reset);
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    private void flush() throws Exception {
        assert waiting == NOT_WAITING;

        ArrayDeque<Grouping> groupingsQueue = groupingsQueue();

        inLoop = true;
        try {
            if (requested > 0 && !groupingsQueue.isEmpty()) {
                List<RowT> batch = newBatch(requested);
                while (requested > 0 && !groupingsQueue.isEmpty()) {
                    Grouping grouping = groupingsQueue.peek();

                    int collected = grouping.collectRows(batch, requested);
                    requested -= collected;

                    if (grouping.isEmpty()) {
                        groupingsQueue.poll();
                    }
                }

                downstream().push(batch);

                if (requested > 0 && !groupingsQueue.isEmpty()) {
                    // allow others to do their job
                    this.execute(this::flush);

                    return;
                }
            }
        } finally {
            inLoop = false;
        }

        if (requested > 0 && groupingsQueue.isEmpty()) {
            requested = 0;
            downstream().end();
        }
    }

    private ArrayDeque<Grouping> groupingsQueue() {
        return groupings.stream()
                .filter(g -> !g.isEmpty())
                .collect(toCollection(ArrayDeque::new));
    }

    private class Grouping {
        private final byte grpId;

        private final ImmutableBitSet grpFields;

        private final Map<GroupKey, AggregateRow<RowT>> groups = new HashMap<>();

        private Grouping(byte grpId, ImmutableBitSet grpFields) {
            this.grpId = grpId;
            this.grpFields = grpFields;
        }

        private void init() {
            // Initializes aggregates for case when no any rows will be added into the aggregate to have 0 as result.
            // Doesn't do it for MAP type due to we don't want send from MAP node zero results because it looks redundant.
            if (AggregateRow.addEmptyGroup(grpFields, type)) {
                groups.put(GroupKey.EMPTY_GRP_KEY, create());
            }
        }

        private void reset() {
            groups.clear();

            init();
        }

        private void add(RowT row) {
            RowHandler<RowT> handler = context().rowHandler();

            if (!AggregateRow.groupMatches(handler, row, type, grpId)) {
                return;
            }

            GroupKey.Builder b = GroupKey.builder(grpFields.cardinality());

            for (int field : grpFields) {
                b.add(handler.get(field, row));
            }

            GroupKey grpKey = b.build();

            AggregateRow<RowT> aggRow = groups.computeIfAbsent(grpKey, k -> create());
            aggRow.update(accs, allFields, handler, row);
        }

        /**
         * Returns up to {@code cnt} rows collected by the given node group by group.
         *
         * @param batch Batch to collect rows to.
         * @param cnt Number of rows to collect.
         * @return Actually collected rows number.
         */
        private int collectRows(List<RowT> batch, int cnt) {
            Iterator<Map.Entry<GroupKey, AggregateRow<RowT>>> it = groups.entrySet().iterator();

            int rowNum = Math.min(cnt, groups.size());
            int collected = 0;

            for (int i = 0; i < rowNum; i++) {
                Map.Entry<GroupKey, AggregateRow<RowT>> entry = it.next();

                GroupKey grpKey = entry.getKey();
                AggregateRow<RowT> aggRow = entry.getValue();

                Object[] fields = aggRow.createOutput(type, accs, allFields, grpId);

                int j = 0;
                int k = 0;

                for (int field : allFields) {
                    fields[j++] = grpFields.get(field) ? grpKey.field(k++) : null;
                }

                aggRow.writeTo(type, accs, fields, allFields, grpId);

                RowT row = rowFactory.create(fields);

                collected++;
                batch.add(row);
                it.remove();
            }

            return collected;
        }

        private AggregateRow<RowT> create() {
            Int2ObjectMap<Set<Object>> distinctSets = new Int2ObjectArrayMap<>();

            for (int i = 0; i < accs.size(); i++) {
                AccumulatorWrapper<RowT> acc = accs.get(i);
                if (acc.isDistinct()) {
                    distinctSets.put(i, new HashSet<>());
                }
            }

            AccumulatorsState state = new AccumulatorsState(accs.size());

            return new AggregateRow<>(state, distinctSets);
        }

        private boolean isEmpty() {
            return groups.isEmpty();
        }
    }
}
