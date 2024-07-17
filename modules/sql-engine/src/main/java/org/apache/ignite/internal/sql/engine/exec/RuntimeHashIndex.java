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

package org.apache.ignite.internal.sql.engine.exec;

import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.sql.engine.exec.exp.agg.GroupKey;
import org.apache.ignite.internal.util.FilteringIterator;
import org.jetbrains.annotations.Nullable;

/**
 * Runtime hash index based on on-heap hash map.
 */
public class RuntimeHashIndex<RowT> implements RuntimeIndex<RowT> {
    /**
     * Placeholder for keys containing NULL values. Used to skip rows with such keys, since condition NULL=NULL should not satisfy the
     * filter.
     */
    private static final GroupKey NULL_KEY = new GroupKey(OBJECT_EMPTY_ARRAY);

    protected final ExecutionContext<RowT> ectx;

    private final ImmutableBitSet keys;

    /** Rows. */
    private HashMap<GroupKey, List<RowT>> rows;

    /** Allow NULL values. */
    private final boolean allowNulls;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public RuntimeHashIndex(
            ExecutionContext<RowT> ectx,
            ImmutableBitSet keys,
            boolean allowNulls
    ) {
        this.ectx = ectx;

        assert !nullOrEmpty(keys);

        this.keys = keys;
        this.allowNulls = allowNulls;
        rows = new HashMap<>();
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT r) {
        GroupKey key = key(r);

        if (key == NULL_KEY) {
            return;
        }

        List<RowT> eqRows = rows.computeIfAbsent(key, k -> new ArrayList<>());

        eqRows.add(r);
    }

    @Override
    public void close() {
        rows.clear();
    }

    public Iterable<RowT> scan(Supplier<RowT> searchRow, @Nullable Predicate<RowT> filter) {
        return new IndexScan(searchRow, filter);
    }

    private GroupKey key(RowT r) {
        GroupKey.Builder b = GroupKey.builder(keys.cardinality());

        for (Integer field : keys) {
            Object fieldVal = ectx.rowHandler().get(field, r);

            if (fieldVal == null && !allowNulls) {
                return NULL_KEY;
            }

            b.add(fieldVal);
        }

        return b.build();
    }

    private class IndexScan implements Iterable<RowT>, AutoCloseable {
        /** Search row. */
        private final Supplier<RowT> searchRow;

        private final Predicate<RowT> filter;

        /**
         * Constructor.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         *
         * @param searchRow Search row.
         */
        IndexScan(Supplier<RowT> searchRow, @Nullable Predicate<RowT> filter) {
            this.searchRow = searchRow;
            this.filter = filter;
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override
        public Iterator<RowT> iterator() {
            GroupKey key = key(searchRow.get());

            if (key == NULL_KEY) {
                return Collections.emptyIterator();
            }

            List<RowT> eqRows = rows.get(key);

            if (eqRows == null) {
                return Collections.emptyIterator();
            }

            return filter == null ? eqRows.iterator() : new FilteringIterator<>(eqRows.iterator(), filter);
        }
    }
}
