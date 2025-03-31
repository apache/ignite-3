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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.sql.engine.exec.VirtualColumn;

/**
 * A projected tuple that enriches {@link FieldDeserializingProjectedTuple} with extra columns.
 *
 * <p>Not thread safe!
 *
 * @see FieldDeserializingProjectedTuple
 */
public class ExtendedFieldDeserializingProjectedTuple extends FieldDeserializingProjectedTuple {

    private Int2ObjectMap<VirtualColumn> extraColumns;

    /**
     * Constructor.
     *
     * @param schema A schema of the original tuple (represented by delegate). Used to read content of the delegate to build a
     *         proper byte buffer which content satisfying the schema with regard to given projection.
     * @param delegate An original tuple to create projection from.
     * @param projection A projection. That is, desired order of fields in original tuple. In that projection, index of the array is
     *         an index of field in resulting projection, and an element of the array at that index is an index of column in original
     *         tuple.
     * @param extraColumns Extra columns.
     */
    public ExtendedFieldDeserializingProjectedTuple(BinaryTupleSchema schema, InternalTuple delegate, int[] projection,
            Int2ObjectMap<VirtualColumn> extraColumns) {
        super(schema, delegate, projection);

        this.extraColumns = extraColumns;
    }

    @Override
    protected void normalize() {
        var builder = new BinaryTupleBuilder(projection.length, 32, false);
        var newProjection = new int[projection.length];

        for (int i = 0; i < projection.length; i++) {
            int col = projection[i];

            newProjection[i] = i;

            VirtualColumn column = extraColumns.get(col);
            if (column != null) {
                BinaryRowConverter.appendValue(builder, column.schemaType(), column.value());

                continue;
            }

            Element element = schema.element(col);

            BinaryRowConverter.copyColumnValue(delegate, builder, element, col);
        }

        delegate = new BinaryTuple(projection.length, builder.build());
        projection = newProjection;
        extraColumns = Int2ObjectMaps.emptyMap();
    }

    private boolean isExtraColumn(int col) {
        return extraColumns.containsKey(projection[col]);
    }

    private VirtualColumn extraColumn(int col) {
        return extraColumns.get(projection[col]);
    }

    @Override
    public boolean hasNullValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value() == null;
        }

        return super.hasNullValue(col);
    }

    @Override
    public boolean booleanValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.booleanValue(col);
    }

    @Override
    public Boolean booleanValueBoxed(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.booleanValueBoxed(col);
    }

    @Override
    public byte byteValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.byteValue(col);
    }

    @Override
    public Byte byteValueBoxed(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.byteValueBoxed(col);
    }

    @Override
    public short shortValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.shortValue(col);
    }

    @Override
    public Short shortValueBoxed(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.shortValueBoxed(col);
    }

    @Override
    public int intValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.intValue(col);
    }

    @Override
    public Integer intValueBoxed(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.intValueBoxed(col);
    }

    @Override
    public long longValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.longValue(col);
    }

    @Override
    public Long longValueBoxed(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.longValueBoxed(col);
    }

    @Override
    public float floatValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.floatValue(col);
    }

    @Override
    public Float floatValueBoxed(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.floatValueBoxed(col);
    }

    @Override
    public double doubleValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.doubleValue(col);
    }

    @Override
    public Double doubleValueBoxed(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.doubleValueBoxed(col);
    }

    @Override
    public BigDecimal decimalValue(int col, int decimalScale) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.decimalValue(col, decimalScale);
    }

    @Override
    public String stringValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.stringValue(col);
    }

    @Override
    public byte[] bytesValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.bytesValue(col);
    }

    @Override
    public UUID uuidValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.uuidValue(col);
    }

    @Override
    public LocalDate dateValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.dateValue(col);
    }

    @Override
    public LocalTime timeValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.timeValue(col);
    }

    @Override
    public LocalDateTime dateTimeValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }

        return super.dateTimeValue(col);
    }

    @Override
    public Instant timestampValue(int col) {
        if (isExtraColumn(col)) {
            return extraColumn(col).value();
        }
        return super.timestampValue(col);
    }
}
