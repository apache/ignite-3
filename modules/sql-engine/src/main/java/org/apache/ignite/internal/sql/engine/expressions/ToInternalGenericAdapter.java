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

package org.apache.ignite.internal.sql.engine.expressions;

import static org.apache.ignite.internal.sql.engine.util.TypeUtils.toInternal;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import org.apache.ignite.internal.sql.engine.api.expressions.RowAccessor;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/** Generic adapter which converts values to internal format based on the actual class of returned values. */
class ToInternalGenericAdapter<RowT> implements RowAccessor<RowT> {
    private final RowAccessor<RowT> delegate;

    ToInternalGenericAdapter(RowAccessor<RowT> delegate) {
        this.delegate = delegate;
    }

    @Override
    public @Nullable Object get(int field, RowT row) {
        Object value = delegate.get(field, row);

        if (value instanceof byte[]) {
            return toInternal(value, ColumnType.BYTE_ARRAY);
        }

        if (value instanceof LocalDate) {
            return toInternal(value, ColumnType.DATE);
        }

        if (value instanceof LocalTime) {
            return toInternal(value, ColumnType.TIME);
        }

        if (value instanceof LocalDateTime) {
            return toInternal(value, ColumnType.DATETIME);
        }

        if (value instanceof Instant) {
            return toInternal(value, ColumnType.TIMESTAMP);
        }

        if (value instanceof Duration) {
            return toInternal(value, ColumnType.DURATION);
        }

        if (value instanceof Period) {
            return toInternal(value, ColumnType.PERIOD);
        }

        return value;
    }

    @Override
    public boolean isNull(int field, RowT row) {
        return delegate.isNull(field, row);
    }

    @Override
    public int columnsCount(RowT row) {
        return delegate.columnsCount(row);
    }
}
