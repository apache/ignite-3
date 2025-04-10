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

package org.apache.ignite.migrationtools.persistence.mappers;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.ignite3.table.Tuple;
import org.jetbrains.annotations.Nullable;

/** Slightly more efficient {@link Tuple} implementation that uses precomputed column name to column id mappings. */
public class CustomTupleImpl implements Tuple {
    private final Map<String, Integer> nameToIdMap;

    private final Object[] values;

    public CustomTupleImpl(Map<String, Integer> nameToIdMap) {
        this.nameToIdMap = nameToIdMap;
        this.values = new Object[this.nameToIdMap.size()];
    }

    @Override
    public int columnCount() {
        return this.values.length;
    }

    @Override
    public String columnName(int i) {
        if (i >= columnCount()) {
            throw new IllegalArgumentException("Out of bounds, tuple only has " + columnCount() + " columns.");
        }

        for (var e : this.nameToIdMap.entrySet()) {
            if (e.getValue() == i) {
                return e.getKey();
            }
        }

        throw new IllegalStateException("This should never have happened");
    }

    @Override
    public int columnIndex(String s) {
        return this.nameToIdMap.getOrDefault(s, -1);
    }

    @Override
    public <T> @Nullable T valueOrDefault(String s, @Nullable T t) {
        int idx = this.columnIndex(s);
        return idx == -1 ? t : (T) this.values[idx];
    }

    @Override
    public Tuple set(String s, @Nullable Object o) {
        int idx = this.columnIndex(s);
        if (idx == -1) {
            throw new IllegalArgumentException("Column does not exist: " + s);
        }

        return set(idx, o);
    }

    public Tuple set(int idx, @Nullable Object o) {
        this.values[idx] = o;
        return this;
    }

    @Override
    public <T> @Nullable T value(String s) throws IllegalArgumentException {
        return this.value(columnIndex(s));
    }

    @Override
    public <T> @Nullable T value(int i) {
        return (T) this.values[i];
    }

    @Override
    public boolean booleanValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public boolean booleanValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public byte byteValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public byte byteValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public short shortValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public short shortValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public int intValue(String s) {
        return this.value(s);
    }

    @Override
    public int intValue(int i) {
        return this.value(i);
    }

    @Override
    public long longValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public long longValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public float floatValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public float floatValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public double doubleValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public double doubleValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public BigDecimal decimalValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public BigDecimal decimalValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public String stringValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public String stringValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public byte[] bytesValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public byte[] bytesValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public UUID uuidValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public UUID uuidValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public LocalDate dateValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public LocalDate dateValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public LocalTime timeValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public LocalTime timeValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public LocalDateTime datetimeValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public LocalDateTime datetimeValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public Instant timestampValue(String s) {
        throw new NotImplementedException();
    }

    @Override
    public Instant timestampValue(int i) {
        throw new NotImplementedException();
    }

    @Override
    public int hashCode() {
        return Tuple.hashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else {
            return obj instanceof Tuple ? Tuple.equals(this, (Tuple) obj) : false;
        }
    }
}
