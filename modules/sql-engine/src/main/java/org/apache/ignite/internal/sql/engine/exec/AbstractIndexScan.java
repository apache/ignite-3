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

import com.google.common.collect.Streams;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.FilteringIterator;
import org.apache.ignite.internal.util.TransformingIterator;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Abstract index scan.
 */
public abstract class AbstractIndexScan<RowT, IdxRowT> implements Iterable<RowT>, AutoCloseable {
    private final TreeIndex<IdxRowT> idx;

    /** Additional filters. */
    private final Predicate<RowT> filters;

    /** Index scan bounds. */
    private final RangeIterable<RowT> ranges;

    private final Function<RowT, RowT> rowTransformer;

    protected final ExecutionContext<RowT> ectx;

    protected final RelDataType rowType;

    /**
     * Constructor.
     *
     * @param ectx Execution context.
     * @param rowType Rel data type.
     * @param idx Physical index.
     * @param filters Additional filters.
     * @param ranges Index scan bounds.
     * @param rowTransformer Row transformer.
     */
    protected AbstractIndexScan(
            ExecutionContext<RowT> ectx,
            RelDataType rowType,
            TreeIndex<IdxRowT> idx,
            Predicate<RowT> filters,
            RangeIterable<RowT> ranges,
            Function<RowT, RowT> rowTransformer
    ) {
        this.ectx = ectx;
        this.rowType = rowType;
        this.idx = idx;
        this.filters = filters;
        this.ranges = ranges;
        this.rowTransformer = rowTransformer;
    }

    /** {@inheritDoc} */
    @Override
    public synchronized Iterator<RowT> iterator() {
        if (ranges == null) { // Full index scan.
            Cursor<IdxRowT> cursor = idx.find(null, null, true, true);

            Iterator<RowT> it = new TransformingIterator<>(cursor, AbstractIndexScan.this::indexRow2Row);

            it = (filters != null) ? new FilteringIterator<>(it, filters) : it;

            return (rowTransformer != null) ? new TransformingIterator<>(it, rowTransformer) : it;
        }

        Iterable<RowT>[] iterables = Streams.stream(ranges)
                .map(range -> new Iterable<RowT>() {
                            @Override
                            public Iterator<RowT> iterator() {
                                IdxRowT lower = row2indexRow(range.lower());
                                IdxRowT upper = row2indexRow(range.upper());

                                Cursor<IdxRowT> cursor = idx.find(lower, upper, range.lowerInclude(), range.upperInclude());

                                Iterator<RowT> it = new TransformingIterator<>(cursor, AbstractIndexScan.this::indexRow2Row);

                                it = (filters != null) ? new FilteringIterator<>(it, filters) : it;

                                return (rowTransformer != null) ? new TransformingIterator<>(it, rowTransformer) : it;
                            }
                        }
                )
                .toArray(Iterable[]::new);

        return CollectionUtils.concat(iterables).iterator();
    }

    protected abstract IdxRowT row2indexRow(RowT bound);

    protected abstract RowT indexRow2Row(IdxRowT idxRow) throws IgniteInternalException;

    /** {@inheritDoc} */
    @Override
    public void close() {
        // No-op.
    }
}
