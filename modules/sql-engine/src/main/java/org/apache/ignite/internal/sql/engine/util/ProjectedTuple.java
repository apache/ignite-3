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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser.Sink;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.jetbrains.annotations.Nullable;

/**
 * A facade that creates projection of the given tuple.
 *
 * <p>Not thread safe!
 *
 * <p>This projection is used to change indexes of column in original tuple, or to trim
 * few columns from original tuple. Here are a few examples:<pre>
 *     Having tuple ['foo', 'bar', 'baz'], we can
 *
 *     - reorder fields with mapping [2, 1, 0] to get equivalent tuple ['baz', 'bar', 'foo']
 *     - or trim certain fields with mapping [0, 2] to get equivalent tuple ['foo', 'baz']
 *     - or even repeat some fields with mapping [0, 0, 0] to get equivalent tuple ['foo', 'foo', 'foo']
 * </pre>
 */
public class ProjectedTuple implements InternalTuple {
    private final @Nullable BinaryTupleSchema schema;

    private InternalTuple delegate;
    private int[] projection;

    private boolean normalized = false;

    /**
     * Creates projected tuple with not optimal but reliable conversion.
     *
     * <p>When call to {@link #byteBuffer()}, the original tuple will be read field by field with regard to provided projection.
     * Such an approach had an additional overhead on (de-)serialization fields value, but had no requirement for the tuple
     * to be compatible with Binary Tuple format.
     *
     * @param schema A schema of the original tuple (represented by delegate). Used to read content of the delegate to build a
     *         proper byte buffer which content satisfying the schema with regard to given projection.
     * @param delegate An original tuple to create projection from.
     * @param projection A projection. That is, desired order of fields in original tuple. In that projection, index of the array is
     *         an index of field in resulting projection, and an element of the array at that index is an index of column in original
     *         tuple.
     */
    public ProjectedTuple(
            BinaryTupleSchema schema,
            InternalTuple delegate,
            int[] projection
    ) {
        this.schema = Objects.requireNonNull(schema);
        this.delegate = delegate;
        this.projection = projection;
    }

    /**
     * Creates projected tuple with optimized conversion.
     *
     * <p>When call to {@link #byteBuffer()}, the original tuple will be rebuild with regard to provided projection
     * by copying raw bytes from original tuple. Although this works more optimal, it requires an original tuple
     * to be crafted with regard to Binary Tuple format.
     *
     * <p>It's up to the caller to get sure that provided tuple respect the format.
     *
     * @param delegate An original tuple to create projection from.
     * @param projection A projection. That is, desired order of fields in original tuple. In that projection, index of the array is
     *         an index of field in resulting projection, and an element of the array at that index is an index of column in original
     *         tuple.
     */
    public ProjectedTuple(
            InternalTuple delegate,
            int[] projection
    ) {
        this.delegate = delegate;
        this.projection = projection;

        this.schema = null;
    }

    @Override
    public int elementCount() {
        return projection.length;
    }

    @Override
    public boolean hasNullValue(int col) {
        return delegate.hasNullValue(projection[col]);
    }

    @Override
    public boolean booleanValue(int col) {
        return delegate.booleanValue(projection[col]);
    }

    @Override
    public Boolean booleanValueBoxed(int col) {
        return delegate.booleanValueBoxed(projection[col]);
    }

    @Override
    public byte byteValue(int col) {
        return delegate.byteValue(projection[col]);
    }

    @Override
    public Byte byteValueBoxed(int col) {
        return delegate.byteValueBoxed(projection[col]);
    }

    @Override
    public short shortValue(int col) {
        return delegate.shortValue(projection[col]);
    }

    @Override
    public Short shortValueBoxed(int col) {
        return delegate.shortValueBoxed(projection[col]);
    }

    @Override
    public int intValue(int col) {
        return delegate.intValue(projection[col]);
    }

    @Override
    public Integer intValueBoxed(int col) {
        return delegate.intValueBoxed(projection[col]);
    }

    @Override
    public long longValue(int col) {
        return delegate.longValue(projection[col]);
    }

    @Override
    public Long longValueBoxed(int col) {
        return delegate.longValueBoxed(projection[col]);
    }

    @Override
    public float floatValue(int col) {
        return delegate.floatValue(projection[col]);
    }

    @Override
    public Float floatValueBoxed(int col) {
        return delegate.floatValueBoxed(projection[col]);
    }

    @Override
    public double doubleValue(int col) {
        return delegate.doubleValue(projection[col]);
    }

    @Override
    public Double doubleValueBoxed(int col) {
        return delegate.doubleValueBoxed(projection[col]);
    }

    @Override
    public BigDecimal decimalValue(int col, int decimalScale) {
        return delegate.decimalValue(projection[col], decimalScale);
    }

    @Override
    public BigInteger numberValue(int col) {
        return delegate.numberValue(projection[col]);
    }

    @Override
    public String stringValue(int col) {
        return delegate.stringValue(projection[col]);
    }

    @Override
    public byte[] bytesValue(int col) {
        return delegate.bytesValue(projection[col]);
    }

    @Override
    public UUID uuidValue(int col) {
        return delegate.uuidValue(projection[col]);
    }

    @Override
    public BitSet bitmaskValue(int col) {
        return delegate.bitmaskValue(projection[col]);
    }

    @Override
    public LocalDate dateValue(int col) {
        return delegate.dateValue(projection[col]);
    }

    @Override
    public LocalTime timeValue(int col) {
        return delegate.timeValue(projection[col]);
    }

    @Override
    public LocalDateTime dateTimeValue(int col) {
        return delegate.dateTimeValue(projection[col]);
    }

    @Override
    public Instant timestampValue(int col) {
        return delegate.timestampValue(projection[col]);
    }

    @Override
    public ByteBuffer byteBuffer() {
        normalizeIfNeeded();

        return delegate.byteBuffer();
    }

    private void normalizeIfNeeded() {
        if (normalized) {
            return;
        }

        if (schema != null) {
            normalizeSlow();
        } else {
            normalizeFast();
        }
    }

    private void normalizeSlow() {
        assert schema != null;

        var builder = new BinaryTupleBuilder(projection.length);
        var newProjection = new int[projection.length];

        for (int i = 0; i < projection.length; i++) {
            int col = projection[i];

            newProjection[i] = i;

            Element element = schema.element(col);

            BinaryRowConverter.appendValue(builder, element, schema.value(delegate, col));
        }

        delegate = new BinaryTuple(projection.length, builder.build());
        projection = newProjection;
        normalized = true;
    }

    private void normalizeFast() {
        var newProjection = new int[projection.length];
        ByteBuffer tupleBuffer = delegate.byteBuffer();
        int[] requiredColumns = projection;

        var parser = new BinaryTupleParser(delegate.elementCount(), tupleBuffer);

        // Estimate total data size.
        var stats = new Sink() {
            int estimatedValueSize = 0;

            @Override
            public void nextElement(int index, int begin, int end) {
                estimatedValueSize += end - begin;
            }
        };

        for (int columnIndex : requiredColumns) {
            parser.fetch(columnIndex, stats);
        }

        // Now compose the tuple.
        BinaryTupleBuilder builder = new BinaryTupleBuilder(requiredColumns.length, stats.estimatedValueSize);

        int pos = 0;

        for (int columnIndex : requiredColumns) {
            parser.fetch(columnIndex, (index, begin, end) -> {
                if (begin == end) {
                    builder.appendNull();
                } else {
                    builder.appendElementBytes(tupleBuffer, begin, end - begin);
                }
            });

            newProjection[pos++] = columnIndex;
        }

        delegate = new BinaryTuple(projection.length, builder.build());
        projection = newProjection;
        normalized = true;
    }
}
