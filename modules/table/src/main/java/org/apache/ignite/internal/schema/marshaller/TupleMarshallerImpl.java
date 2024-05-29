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

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Tuple marshaller implementation.
 */
public class TupleMarshallerImpl implements TupleMarshaller {
    private static final Object POISON_OBJECT = new Object();

    private final SchemaDescriptor schema;

    /**
     * Creates marshaller for given schema.
     *
     * @param schema Schema.
     */
    public TupleMarshallerImpl(SchemaDescriptor schema) {
        this.schema = schema;
    }

    @Override
    public int schemaVersion() {
        return schema.version();
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(Tuple tuple) throws TupleMarshallerException {
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

            ValuesWithStatistics valuesWithStatistics = new ValuesWithStatistics();

            gatherStatistics(schema.columns(), tuple, valuesWithStatistics);

            if (valuesWithStatistics.knownColumns != tuple.columnCount()) {
                throw new SchemaMismatchException(
                        String.format("Tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                schema.version(), extraColumnNames(tuple, schema)));
            }

            return buildRow(false, valuesWithStatistics);
        } catch (Exception ex) {
            throw new TupleMarshallerException("Failed to marshal tuple.", ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(Tuple keyTuple, @Nullable Tuple valTuple) throws TupleMarshallerException {
        try {
            ValuesWithStatistics valuesWithStatistics = new ValuesWithStatistics();

            gatherStatistics(schema.keyColumns(), keyTuple, valuesWithStatistics);

            if (valuesWithStatistics.knownColumns != keyTuple.columnCount()) {
                throw new SchemaMismatchException(
                        String.format("Key tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                schema.version(), extraColumnNames(keyTuple, true, schema)));
            }

            boolean keyOnly = valTuple == null;
            if (!keyOnly) {
                gatherStatistics(schema.valueColumns(), valTuple, valuesWithStatistics);

                if ((valuesWithStatistics.knownColumns - keyTuple.columnCount()) != valTuple.columnCount()) {
                    throw new SchemaMismatchException(
                            String.format("Value tuple doesn't match schema: schemaVersion=%s, extraColumns=%s",
                                    schema.version(), extraColumnNames(valTuple, false, schema)));
                }
            }

            return buildRow(keyOnly, valuesWithStatistics);
        } catch (Exception ex) {
            throw new TupleMarshallerException("Failed to marshal tuple.", ex);
        }
    }

    /** {@inheritDoc} */
    @Override
    public Row marshalKey(Tuple keyTuple) throws TupleMarshallerException {
        try {
            ValuesWithStatistics valuesWithStatistics = new ValuesWithStatistics();

            gatherStatistics(schema.keyColumns(), keyTuple, valuesWithStatistics);

            if (valuesWithStatistics.knownColumns < keyTuple.columnCount()) {
                throw new SchemaMismatchException("Key tuple contains extra columns: " + extraColumnNames(keyTuple, true, schema));
            }

            return buildRow(true, valuesWithStatistics);
        } catch (Exception ex) {
            throw new TupleMarshallerException("Failed to marshal tuple.", ex);
        }
    }

    private Row buildRow(
            boolean keyOnly,
            ValuesWithStatistics values
    ) throws SchemaMismatchException {
        List<Column> columns = keyOnly ? schema.keyColumns() : schema.columns();
        RowAssembler rowBuilder = new RowAssembler(schema.version(), columns, values.estimatedValueSize);

        for (Column col : columns) {
            rowBuilder.appendValue(values.value(col.name()));
        }

        return keyOnly
                ? Row.wrapKeyOnlyBinaryRow(schema, rowBuilder.build())
                : Row.wrapBinaryRow(schema, rowBuilder.build());
    }

    void gatherStatistics(
            List<Column> columns,
            Tuple tuple,
            ValuesWithStatistics targetTuple
    ) throws SchemaMismatchException {
        int estimatedValueSize = 0;
        int knownColumns = 0;
        for (Column col : columns) {
            NativeType colType = col.type();

            Object val = tuple.valueOrDefault(col.name(), POISON_OBJECT);

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
                if (colType.spec().fixedLength()) {
                    estimatedValueSize += colType.sizeInBytes();
                } else {
                    val = MarshallerUtil.shrinkValue(val, col.type());

                    estimatedValueSize += getValueSize(val, colType);
                }
            }

            targetTuple.values.put(col.name(), val);
        }

        targetTuple.estimatedValueSize += estimatedValueSize;
        targetTuple.knownColumns += knownColumns;
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
        private final Map<String, Object> values = new HashMap<>();

        private int estimatedValueSize;
        private int knownColumns;

        @Nullable Object value(String columnName) {
            return values.get(columnName);
        }

        @TestOnly
        int estimatedValueSize() {
            return estimatedValueSize;
        }
    }
}
