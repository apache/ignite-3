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

package org.apache.ignite.internal.sql.engine.util;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.sql.engine.InternalSqlRow;
import org.jetbrains.annotations.Nullable;

/** InternalSqlRow implementation for test purposes only. */
public class ListToInternalSqlRowAdapter implements InternalSqlRow {
    final List<Object> source;

    public ListToInternalSqlRowAdapter(List<Object> source) {
        this.source = source;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object get(int idx) {
        return source.get(idx);
    }

    /** {@inheritDoc} */
    @Override
    public int fieldCount() {
        return source.size();
    }

    /** {@inheritDoc} */
    @Override
    public BinaryTuple asBinaryTuple() {
        BinaryTupleBuilder binaryTupleBuilder = new BinaryTupleBuilder(source.size());

        source.forEach(o -> appendObject(binaryTupleBuilder, o));

        return new BinaryTuple(source.size(), binaryTupleBuilder.build());
    }

    private static void appendObject(BinaryTupleBuilder builder, @Nullable Object obj) {
        if (obj == null) {
            builder.appendNull();
        } else if (obj instanceof Boolean) {
            builder.appendBoolean((Boolean) obj);
        } else if (obj instanceof Byte) {
            builder.appendByte((Byte) obj);
        } else if (obj instanceof Short) {
            builder.appendShort((Short) obj);
        } else if (obj instanceof Integer) {
            builder.appendInt((Integer) obj);
        } else if (obj instanceof Long) {
            builder.appendLong((Long) obj);
        } else if (obj instanceof Float) {
            builder.appendFloat((Float) obj);
        } else if (obj instanceof Double) {
            builder.appendDouble((Double) obj);
        } else if (obj instanceof BigDecimal) {
            BigDecimal bigDecimal = (BigDecimal) obj;
            builder.appendDecimal(bigDecimal, bigDecimal.scale());
        } else if (obj instanceof UUID) {
            builder.appendUuid((UUID) obj);
        } else if (obj instanceof String) {
            builder.appendString((String) obj);
        } else if (obj instanceof byte[]) {
            builder.appendBytes((byte[]) obj);
        } else if (obj instanceof LocalDate) {
            builder.appendDate((LocalDate) obj);
        } else if (obj instanceof LocalTime) {
            builder.appendTime((LocalTime) obj);
        } else if (obj instanceof LocalDateTime) {
            builder.appendDateTime((LocalDateTime) obj);
        } else if (obj instanceof Instant) {
            builder.appendTimestamp((Instant) obj);
        } else if (obj instanceof Duration) {
            builder.appendDuration((Duration) obj);
        } else if (obj instanceof Period) {
            builder.appendPeriod((Period) obj);
        } else {
            throw new IllegalArgumentException("Unsupported type " + obj.getClass());
        }
    }
}
