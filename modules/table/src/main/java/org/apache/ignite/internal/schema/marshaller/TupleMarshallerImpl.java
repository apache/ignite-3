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

package org.apache.ignite.internal.schema.marshaller;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;
import static org.apache.ignite.internal.util.IgniteUtils.newHashMap;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.ignite.internal.binarytuple.BinaryTupleCommon;
import org.apache.ignite.internal.binarytuple.BinaryTupleContainer;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.SchemaVersionMismatchException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleHelper;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Tuple marshaller implementation.
 */
public class TupleMarshallerImpl implements TupleMarshaller {
    private static final Object POISON_OBJECT = new Object();

    private final SchemaDescriptor schema;
    private final Supplier<QualifiedName> tableNameSupplier;

    private final int keyOnlyFixedLengthColumnSize;
    private final int valueOnlyFixedLengthColumnSize;

    /**
     * Creates marshaller for given schema.
     *
     * @param tableNameSupplier Table name supplier.
     * @param schema Schema.
     */
    public TupleMarshallerImpl(Supplier<QualifiedName> tableNameSupplier, SchemaDescriptor schema) {
        this.schema = schema;
        this.tableNameSupplier = tableNameSupplier;

        keyOnlyFixedLengthColumnSize = schema.keyColumns().stream()
                .map(Column::type)
                .filter(NativeType::fixedLength)
                .mapToInt(NativeType::sizeInBytes)
                .sum();

        valueOnlyFixedLengthColumnSize = schema.valueColumns().stream()
                .map(Column::type)
                .filter(NativeType::fixedLength)
                .mapToInt(NativeType::sizeInBytes)
                .sum();
    }

    @Override
    public int schemaVersion() {
        return schema.version();
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(Tuple tuple) throws MarshallerException {
        try {
            if (tuple instanceof SchemaAware && tuple instanceof BinaryTupleContainer) {
                SchemaDescriptor tupleSchema = ((SchemaAware) tuple).schema();
                BinaryTupleReader tupleReader = ((BinaryTupleContainer) tuple).binaryTuple();

                if (tupleSchema != null && tupleReader != null) {
                    if (tupleSchema.version() != schema.version()) {
                        throw new SchemaVersionMismatchException(schema.version(), tupleSchema.version());
                    }

                    if (!binaryTupleRebuildRequired(schema)) {
                        validateTuple(tuple, schema);

                        // BinaryTuple from client has matching schema version, and all values are valid. Use buffer as is.
                        var binaryRow = new BinaryRowImpl(schema.version(), tupleReader.byteBuffer());
                        return Row.wrapBinaryRow(schema, binaryRow);
                    }
                }
            }

            TuplePart part = TuplePart.KEY_VALUE;

            var valuesWithStatistics = new ValuesWithStatistics(schema.length());

            gatherStatistics(part, tuple, valuesWithStatistics);

            if (valuesWithStatistics.knownColumns != tuple.columnCount()) {
                throw new SchemaMismatchException(
                        String.format("Tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                schema.version(), extraColumnNames(tuple, schema)));
            }

            return buildRow(part, valuesWithStatistics);
        } catch (Exception ex) {
            String msg = format("Failed to serialize row for table {}. {}",
                    tableNameSupplier.get().toCanonicalForm(),
                    ex.getMessage());

            throw new MarshallerException(msg, ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(Tuple keyTuple, @Nullable Tuple valTuple) throws MarshallerException {
        boolean keyOnly = valTuple == null;

        try {
            var valuesWithStatistics = new ValuesWithStatistics(valTuple == null ? schema.keyColumns().size() : schema.length());

            gatherStatistics(TuplePart.KEY, keyTuple, valuesWithStatistics);

            if (valuesWithStatistics.knownColumns != keyTuple.columnCount()) {
                throw new SchemaMismatchException(
                        String.format("Key tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                schema.version(), extraColumnNames(keyTuple, true, schema)));
            }

            if (!keyOnly) {
                gatherStatistics(TuplePart.VALUE, valTuple, valuesWithStatistics);

                if ((valuesWithStatistics.knownColumns - keyTuple.columnCount()) != valTuple.columnCount()) {
                    throw new SchemaMismatchException(
                            String.format("Value tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                    schema.version(), extraColumnNames(valTuple, false, schema)));
                }
            }

            return buildRow(keyOnly ? TuplePart.KEY : TuplePart.KEY_VALUE, valuesWithStatistics);
        } catch (Exception ex) {
            String msg = format("Failed to serialize {} for table {}. {}",
                    keyOnly ? "key tuple" : "row",
                    tableNameSupplier.get().toCanonicalForm(),
                    ex.getMessage());

            throw new MarshallerException(msg, ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Row marshalKey(Tuple keyTuple) throws MarshallerException {
        try {
            var valuesWithStatistics = new ValuesWithStatistics(schema.keyColumns().size());

            TuplePart part = TuplePart.KEY;

            gatherStatistics(part, keyTuple, valuesWithStatistics);

            if (valuesWithStatistics.knownColumns < keyTuple.columnCount()) {
                throw new SchemaMismatchException("Key tuple contains extra columns: " + extraColumnNames(keyTuple, true, schema));
            }

            return buildRow(part, valuesWithStatistics);
        } catch (Exception ex) {
            String msg = format("Failed to serialize key tuple for table {}. {}",
                    tableNameSupplier.get().toCanonicalForm(),
                    ex.getMessage());

            throw new MarshallerException(msg, ex);
        }
    }

    private Row buildRow(
            TuplePart part,
            ValuesWithStatistics values
    ) throws SchemaMismatchException {
        List<Column> columns = part.deriveColumnList(schema);
        RowAssembler rowBuilder = new RowAssembler(schema.version(), columns, values.estimatedValueSize, false);

        for (Column col : columns) {
            rowBuilder.appendValue(values.value(col.name()));
        }

        return part == TuplePart.KEY
                ? Row.wrapKeyOnlyBinaryRow(schema, rowBuilder.build())
                : Row.wrapBinaryRow(schema, rowBuilder.build());
    }

    void gatherStatistics(
            TuplePart part,
            Tuple tuple,
            ValuesWithStatistics targetTuple
    ) throws SchemaMismatchException {
        int estimatedValueSize = part.fixedSizeColumnsSize(keyOnlyFixedLengthColumnSize, valueOnlyFixedLengthColumnSize);
        int knownColumns = 0;
        for (Column col : part.deriveColumnList(schema)) {
            NativeType colType = col.type();

            Object val = TupleHelper.valueOrDefault(tuple, col.name(), POISON_OBJECT);

            if (val == POISON_OBJECT && col.positionInKey() != -1) {
                throw new SchemaMismatchException("Missed key column: " + col.name());
            }

            if (val == POISON_OBJECT) {
                val = col.defaultValue();
            } else {
                knownColumns++;
            }

            col.validate(val);

            if (val != null) {
                if (!colType.fixedLength()) {
                    try {
                        val = shrinkValue(val, col.type());

                        estimatedValueSize += getValueSize(val, colType);
                    } catch (ClassCastException e) {
                        throw new SchemaMismatchException(
                                String.format(
                                        "Invalid value type provided for column [name='%s', expected='%s', actual='%s']",
                                        col.name(),
                                        col.type().spec().javaClass().getName(),
                                        val.getClass().getName()),
                                e);
                    }
                }
            }

            targetTuple.values.put(col.name(), val);
        }

        targetTuple.estimatedValueSize += estimatedValueSize;
        targetTuple.knownColumns += knownColumns;
    }

    /**
     * Converts the passed value to a more compact form, if possible.
     *
     * @param value Field value.
     * @param type Mapped type.
     * @return Value in a more compact form, or the original value if it cannot be compacted.
     */
    private static <T> T shrinkValue(T value, NativeType type) {
        if (type.spec() == ColumnType.DECIMAL) {
            assert type instanceof DecimalNativeType;

            return (T) BinaryTupleCommon.shrinkDecimal((BigDecimal) value, ((DecimalNativeType) type).scale());
        }

        return value;
    }

    /**
     * Extracts columns.
     *
     * @param tuple  Tuple representing a Row.
     * @param schema Schema.
     * @return Extra columns.
     */
    private static Set<String> extraColumnNames(Tuple tuple, SchemaDescriptor schema) {
        Set<String> cols = new HashSet<>();

        for (int i = 0, len = tuple.columnCount(); i < len; i++) {
            String colName = tuple.columnName(i);

            if (schema.column(colName) == null) {
                cols.add(colName);
            }
        }

        return cols;
    }

    /**
     * Return column names that are missed in the schema.
     *
     * @param tuple    Key or value tuple.
     * @param keyTuple Key tuple flag. {@code True} if tuple is a key. {@code false} if tuple is value.
     * @param schema   Schema to check against.
     * @return Column names.
     */
    private static Set<String> extraColumnNames(Tuple tuple, boolean keyTuple, SchemaDescriptor schema) {
        Set<String> cols = new HashSet<>();

        for (int i = 0, len = tuple.columnCount(); i < len; i++) {
            String colName = tuple.columnName(i);

            Column col = schema.column(colName);

            if (col == null || (col.positionInKey() != -1) ^ keyTuple) {
                cols.add(colName);
            }
        }

        return cols;
    }

    /**
     * Determines whether binary tuple rebuild is required.
     *
     * @param schema Schema.
     * @return True if binary tuple rebuild is required; false if the tuple can be written to storage as is.
     */
    private static boolean binaryTupleRebuildRequired(SchemaDescriptor schema) {
        // Temporal columns require normalization according to the specified precision.
        return schema.hasTemporalColumns();
    }

    /**
     * Validates tuple against schema.
     *
     * @param tuple Tuple.
     * @param schema Schema.
     */
    private static void validateTuple(Tuple tuple, SchemaDescriptor schema) {
        for (int i = 0; i < schema.length(); i++) {
            Column col = schema.column(i);
            Object val = tuple.value(i);

            col.validate(val);
        }
    }

    /**
     * Container to keep columns values and related statistics which help
     * to build row with {@link RowAssembler}.
     */
    static class ValuesWithStatistics {
        private final Map<String, Object> values;

        private int estimatedValueSize;
        private int knownColumns;

        ValuesWithStatistics(int numValues) {
            values = newHashMap(numValues);
        }

        @Nullable Object value(String columnName) {
            return values.get(columnName);
        }

        @TestOnly
        int estimatedValueSize() {
            return estimatedValueSize;
        }
    }

    enum TuplePart {
        KEY {
            @Override
            int fixedSizeColumnsSize(int keyOnlySize, int valueOnlySize) {
                return keyOnlySize;
            }

            @Override
            List<Column> deriveColumnList(SchemaDescriptor schema) {
                return schema.keyColumns();
            }
        },

        VALUE {
            @Override
            int fixedSizeColumnsSize(int keyOnlySize, int valueOnlySize) {
                return valueOnlySize;
            }

            @Override
            List<Column> deriveColumnList(SchemaDescriptor schema) {
                return schema.valueColumns();
            }
        },

        KEY_VALUE {
            @Override
            int fixedSizeColumnsSize(int keyOnlySize, int valueOnlySize) {
                return keyOnlySize + valueOnlySize;
            }

            @Override
            List<Column> deriveColumnList(SchemaDescriptor schema) {
                return schema.columns();
            }
        };

        abstract int fixedSizeColumnsSize(int keyOnlySize, int valueOnlySize);

        abstract List<Column> deriveColumnList(SchemaDescriptor schema);
    }
}
