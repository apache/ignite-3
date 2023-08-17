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
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.AggregateType;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.GroupKey;

/**
 * Abstract execution node for set operators (EXCEPT, INTERSECT).
 */
public abstract class AbstractSetOpNode<RowT> extends AbstractNode<RowT> {
    private final AggregateType type;

    private final Grouping<RowT> grouping;

    private int requested;

    private int waiting;

    /** Current source index. */
    private int curSrcIdx;

    private boolean inLoop;

    protected AbstractSetOpNode(ExecutionContext<RowT> ctx, AggregateType type, boolean all,
            RowFactory<RowT> rowFactory, Grouping<RowT> grouping) {
        super(ctx);

        this.type = type;
        this.grouping = grouping;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources());
        assert rowsCnt > 0 && requested == 0;
        assert waiting <= 0;

        checkState();

        requested = rowsCnt;

        if (waiting == 0) {
            sources().get(curSrcIdx).request(waiting = inBufSize);
        } else if (!inLoop) {
            context().execute(this::flush, this::onError);
        }
    }

    /**
     * Push.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public void push(RowT row, int idx) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        grouping.add(row, idx);

        if (waiting == 0) {
            sources().get(curSrcIdx).request(waiting = inBufSize);
        }
    }

    /**
     * End.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public void end(int idx) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert curSrcIdx == idx;

        checkState();

        grouping.endOfSet(idx);

        if (type == AggregateType.SINGLE && grouping.isEmpty()) {
            curSrcIdx = sources().size(); // Skip subsequent sources.
        } else {
            curSrcIdx++;
        }

        if (curSrcIdx >= sources().size()) {
            waiting = -1;

            flush();
        } else {
            sources().get(curSrcIdx).request(waiting);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        requested = 0;
        waiting = 0;
        curSrcIdx = 0;
        grouping.groups.clear();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        return new Downstream<RowT>() {
            @Override
            public void push(RowT row) throws Exception {
                AbstractSetOpNode.this.push(row, idx);
            }

            @Override
            public void end() throws Exception {
                AbstractSetOpNode.this.end(idx);
            }

            @Override
            public void onError(Throwable e) {
                AbstractSetOpNode.this.onError(e);
            }
        };
    }

    private void flush() throws Exception {
        if (isClosed()) {
            return;
        }

        checkState();

        assert waiting == -1;

        int processed = 0;

        inLoop = true;

        try {
            if (requested > 0 && !grouping.isEmpty()) {
                int toSnd = Math.min(requested, inBufSize - processed);

                for (RowT row : grouping.getRows(toSnd)) {
                    requested--;

                    downstream().push(row);

                    processed++;
                }

                if (processed >= inBufSize && requested > 0) {
                    // Allow others to do their job.
                    context().execute(this::flush, this::onError);

                    return;
                }
            }
        } finally {
            inLoop = false;
        }

        if (requested > 0) {
            requested = 0;

            downstream().end();
        }
    }

    /**
     * Grouping provides base driver code to implement a set operator.
     * The basic idea is to store the number of distinct rows per input set and use these numbers to calculate
     * the number of rows an operator should produce.
     */
    protected abstract static class Grouping<RowT> {
        protected final Map<GroupKey, int[]> groups = new HashMap<>();

        protected final AggregateType type;

        protected final boolean all;

        protected final RowFactory<RowT> rowFactory;

        private final RowHandler<RowT> hnd;

        private final int columnNum;

        protected Grouping(ExecutionContext<RowT> ctx, RowFactory<RowT> rowFactory,  int columnNum, AggregateType type, boolean all) {
            hnd = ctx.rowHandler();
            this.columnNum = columnNum;
            this.type = type;
            this.all = all;
            this.rowFactory = rowFactory;
        }

        protected void add(RowT row, int setIdx) {
            if (type == AggregateType.REDUCE) {
                assert setIdx == 0 : "Unexpected set index: " + setIdx;

                addOnReducer(row);
            } else if (type == AggregateType.MAP) {
                addOnMapper(row, setIdx);
            } else {
                addOnSingle(row, setIdx);
            }
        }

        /**
         * Returns a list of rows produced by the given operator.
         *
         * @param cnt Number of rows to return.
         * @return A list of rows to send.
         */
        private List<RowT> getRows(int cnt) {
            if (nullOrEmpty(groups)) {
                return Collections.emptyList();
            } else if (type == AggregateType.MAP) {
                return getOnMapper(cnt);
            } else {
                return getResultRows(cnt);
            }
        }

        protected GroupKey createKey(RowT row) {
            int size = hnd.columnCount(row);

            Object[] fields = new Object[size];

            for (int i = 0; i < size; i++) {
                fields[i] = hnd.get(i, row);
            }

            return new GroupKey(fields);
        }

        /** Callback called when data is over. */
        protected abstract void endOfSet(int setIdx);

        /** Implementation of colocated version of this operator.*/
        protected abstract void addOnSingle(RowT row, int setIdx);

        /** Adds a the given row produced by {@code setIdx}. */
        protected abstract void addOnMapper(RowT row, int setIdx);

        /** Returns the number of inputs of this operator. */
        protected abstract int getCounterFieldsCount();

        private void addOnReducer(RowT row) {
            GroupKey.Builder grpKeyBuilder = GroupKey.builder(columnNum);

            for (int i = 0; i < columnNum; i++) {
                Object field = hnd.get(i, row);
                grpKeyBuilder.add(field);
            }

            GroupKey grpKey = grpKeyBuilder.build();

            int inputsCnt = getCounterFieldsCount();
            int[] cntrs = groups.computeIfAbsent(grpKey, k -> new int[inputsCnt]);

            for (int i = 0; i < inputsCnt; i++) {
                cntrs[i] += (int) hnd.get(i + columnNum, row);
            }
        }

        private List<RowT> getOnMapper(int cnt) {
            Iterator<Map.Entry<GroupKey, int[]>> it = groups.entrySet().iterator();

            int amount = Math.min(cnt, groups.size());
            List<RowT> res = new ArrayList<>(amount);

            while (amount > 0 && it.hasNext()) {
                Map.Entry<GroupKey, int[]> entry = it.next();

                // Skip row if it doesn't affect the final result.
                if (affectResult(entry.getValue())) {
                    RowT row = createOutputRow(entry);
                    res.add(row);

                    amount--;
                }

                it.remove();
            }

            return res;
        }

        private List<RowT> getResultRows(int cnt) {
            Iterator<Map.Entry<GroupKey, int[]>> it = groups.entrySet().iterator();
            List<RowT> res = new ArrayList<>(cnt);

            while (it.hasNext() && cnt > 0) {
                Map.Entry<GroupKey, int[]> entry = it.next();
                RowT row = createOutputRow(entry);
                int availableRows = availableRows(entry.getValue());

                updateAvailableRows(entry.getValue(), availableRows);

                if (availableRows <= cnt) {
                    it.remove();

                    cnt -= availableRows;
                } else {
                    availableRows = cnt;

                    assert availableRows > 0;
                    assert all : "This branch should only be accessible for non distinct variant of a set operator";

                    int[] cntrs = entry.getValue();
                    cntrs[0] -= availableRows;

                    cnt = 0;
                }

                for (int i = 0; i < availableRows; i++) {
                    res.add(row);
                }
            }

            return res;
        }

        /**
         * Return {@code true} if counters affects the final result, or {@code false} if row can be skipped.
         */
        protected abstract boolean affectResult(int[] cntrs);

        /** Calculates the number of rows to return. */
        protected abstract int availableRows(int[] cntrs);

        /** Updates the counters after calculating the number of available rows. */
        protected abstract void updateAvailableRows(int[] cntrs, int availableRows);

        private boolean isEmpty() {
            return groups.isEmpty();
        }

        private RowT createOutputRow(Map.Entry<GroupKey, int[]> entry) {
            GroupKey groupKey = entry.getKey();
            int[] cnts = entry.getValue();

            assert groupKey.fieldsCount() == columnNum : format("Invalid key {} columnNum: {}", groupKey, columnNum);

            // Append counts on MAP phase.
            boolean appendCounts = type == AggregateType.MAP;
            int countsNum;
            Object[] fields;

            if (appendCounts) {
                countsNum = cnts.length;
                fields = new Object[columnNum + countsNum];
            } else {
                countsNum = 0;
                fields = new Object[columnNum];
            }

            for (int i = 0; i < groupKey.fieldsCount(); i++) {
                fields[i] = groupKey.field(i);
            }

            for (int j = 0; j < countsNum; j++) {
                fields[columnNum + j] = cnts[j];
            }

            return rowFactory.create(fields);
        }
    }
}
