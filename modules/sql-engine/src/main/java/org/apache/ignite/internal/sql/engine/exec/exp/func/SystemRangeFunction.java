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

package org.apache.ignite.internal.sql.engine.exec.exp.func;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.sql.fun.IgniteSqlOperatorTable;
import org.apache.ignite.internal.type.NativeTypes;
import org.jetbrains.annotations.Nullable;

/** Implementation of {@link IgniteSqlOperatorTable#SYSTEM_RANGE system range function}. */
public final class SystemRangeFunction<RowT> implements TableFunction<RowT> {

    private final RowSchema rowSchema = RowSchema.builder()
            .addField(NativeTypes.INT64)
            .build();

    private final Supplier<Long> startExpr;

    private final Supplier<Long> endExpr;

    private final Supplier<Long> incrementExpr;

    /** Constructor. */
    public SystemRangeFunction(Supplier<Long> startExpr,  Supplier<Long> endExpr, @Nullable Supplier<Long> incrementExpr) {
        this.startExpr = Objects.requireNonNull(startExpr, "startExpr");
        this.endExpr =  Objects.requireNonNull(endExpr, "endExpr");
        this.incrementExpr = incrementExpr != null ? incrementExpr : () -> 1L;
    }

    /** {@inheritDoc} */
    @Override
    public TableFunctionInstance<RowT> createInstance(ExecutionContext<RowT> ctx) {
        RowFactory<RowT> factory = ctx.rowHandler().factory(rowSchema);

        Long start = startExpr.get();
        Long end = endExpr.get();
        Long increment = incrementExpr.get();

        if (increment == null) {
            increment = 1L;
        } else if (increment == 0) {
            throw new IllegalArgumentException("Increment can't be 0");
        }

        if (start == null || end == null) {
            return TableFunctionInstance.empty();
        } else {
            return new SystemRangeInstance<>(factory, start, end, increment);
        }
    }

    private static class SystemRangeInstance<RowT> implements TableFunctionInstance<RowT> {

        private final long end;

        private final long increment;

        private long current;

        private final RowFactory<RowT> factory;

        SystemRangeInstance(RowFactory<RowT> factory, long start, long end, long increment) {
            this.factory = factory;
            this.end = end;
            this.increment = increment;
            this.current = start;
        }

        @Override
        public boolean hasNext() {
            return increment > 0 ? current <= end : current >= end;
        }

        @Override
        public RowT next() {
            if (increment > 0 && current > end) {
                throw new NoSuchElementException();
            } else if (increment < 0 && current < end) {
                throw new NoSuchElementException();
            }

            RowT row = factory.create(current);

            current += increment;

            return row;
        }

        @Override
        public void close() {

        }
    }
}
