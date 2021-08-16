/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

/**
 * Tuple marshaller implementation.
 */
public class TupleMarshallerImpl implements TupleMarshaller {
    /** Poison object. */
    private static final Object POISON_OBJECT = new Object();

    /** Schema manager. */
    private final SchemaRegistry schemaReg;

    /** Table manager. */
    private final TableManager tblMgr;

    /** Internal table. */
    private final InternalTable tbl;

    /**
     * Creates tuple marshaller.
     *
     * @param tblMgr Table manager.
     * @param tbl Internal table.
     * @param schemaReg Schema manager.
     */
    public TupleMarshallerImpl(TableManager tblMgr, InternalTable tbl, SchemaRegistry schemaReg) {
        this.schemaReg = schemaReg;
        this.tblMgr = tblMgr;
        this.tbl = tbl;
    }

    /** {@inheritDoc} */
    @Override public Row marshal(@NotNull Tuple tuple) {
        return marshal(tuple, tuple);
    }

    /** {@inheritDoc} */
    @Override public Row marshal(@NotNull Tuple keyTuple, @Nullable Tuple valTuple) {
        final SchemaDescriptor schema = valTuple != null ? schemaForTuple(valTuple) : schemaReg.schema();

        validate(keyTuple, schema.keyColumns());

        return marshal(schema, keyTuple, valTuple);
    }

    /** {@inheritDoc} */
    @Override public Row marshalKey(@NotNull Tuple tuple) {
        final SchemaDescriptor schema = schemaReg.schema();

        validate(tuple, schema.keyColumns());

        final RowAssembler rowBuilder = createAssembler(schema, tuple, null);

        Columns cols = schema.keyColumns();

        for (int i = 0; i < cols.length(); i++) {
            final Column col = cols.column(i);

            writeColumn(rowBuilder, col, tuple);
        }

        return new Row(schema, rowBuilder.build());
    }

    @NotNull private Row marshal(SchemaDescriptor schema, @NotNull Tuple keyTuple, @Nullable Tuple valTuple) {
        final RowAssembler rowBuilder = createAssembler(schema, keyTuple, valTuple);

        for (int i = 0; i < schema.keyColumns().length(); i++) {
            final Column col = schema.keyColumns().column(i);

            writeColumn(rowBuilder, col, keyTuple);
        }

        if (valTuple != null) {
            validate(valTuple, schema.valueColumns());

            for (int i = 0; i < schema.valueColumns().length(); i++) {
                final Column col = schema.valueColumns().column(i);

                writeColumn(rowBuilder, col, valTuple);
            }
        }

        return new Row(schema, rowBuilder.build());
    }

    /**
     * Returns the latest schema if it is compatible with the tuple,
     * or upgrade the schema if Live schema is on, otherwise fails.
     *
     * @param tuple Tuple to validate.
     * @return Schema the given tuple matches to.
     * @throws SchemaMismatchException If the tuple is incompatible and a schema can't be upgraded.
     */
    private SchemaDescriptor schemaForTuple(Tuple tuple) {
        SchemaDescriptor schema = schemaReg.schema();

        while (!isMatch(tuple, schema)) {
            Set<org.apache.ignite.schema.Column> extraCols = extraColumns(schema, tuple);

            if (tbl.schemaMode() == SchemaMode.STRICT_SCHEMA) {
                throw new SchemaMismatchException("Strict schema doesn't allow extra columns: colNames=" +
                    extraCols.stream().map(c -> c.name()).collect(Collectors.joining()));
            }

            createColumns(extraCols);

            assert schemaReg.lastSchemaVersion() > schema.version() : "Schema upgrade was async or delayed.";

            schema = schemaReg.schema();
        }

        return schema;
    }

    /**
     * Checks if the tuple match the schema.
     *
     * @return {@code True} if the tuple matches schema, {@code false} otherwise.
     * @throws InvalidTypeException If column of incompatible type found.
     */
    private boolean isMatch(Tuple tuple, SchemaDescriptor schema) {
        int schemaColsFound = 0;

        for (int i = 0; i < schema.length(); i++) {
            Column col = schema.column(i);

            Object val = tuple.valueOrDefault(col.name(), POISON_OBJECT);

            if (val == POISON_OBJECT) // Tuple has no value for column.
                continue;

            schemaColsFound++;

            col.validate(val); //TODO: validate default value???
        }

        return tuple.columnCount() == schemaColsFound;
    }

    /**
     * Extract columns from the tuple that are missed in schema.
     *
     * @param schema Schema to validate against.
     * @param tuple Tuple to validate.
     * @return Extra columns.
     */
    private Set<org.apache.ignite.schema.Column> extraColumns(SchemaDescriptor schema, Tuple tuple) {
        Set<org.apache.ignite.schema.Column> extraColumns = new HashSet<>();

        for (int i = 0; i < tuple.columnCount(); i++) {
            String colName = tuple.columnName(i);

            if (schema.column(colName) != null)
                continue;

            Object colValue = tuple.value(i);

            if (colValue == null) // Can't detect type of 'null'
                throw new InvalidTypeException("Live schema upgrade for 'null' value is not supported yet.");

            ColumnType colType = SchemaConfigurationConverter.columnType(colValue.getClass());

            if (colType == null) // No native support for type.
                throw new InvalidTypeException("Live schema upgrade for type [" + colValue.getClass() + "] is not supported.");

            extraColumns.add(SchemaBuilders.column(colName, colType).asNullable().build());
        }

        return extraColumns;
    }

    /**
     * Validates columns values.
     *
     * @param tuple Tuple to validate.
     * @param columns Columns to validate against.
     * @throws SchemaMismatchException If validation failed.
     */
    private void validate(Tuple tuple, Columns columns) {
        if (tuple instanceof SchemaAware) {
            SchemaAware t0 = ((SchemaAware)tuple);
            SchemaDescriptor expSchema = schemaReg.schema(t0.schema().version());

            //TODO: Does it make sense to check 'tableId' and 'version' equality instead of reference?
            if (Objects.equals(t0.schema(), expSchema))
                return;
        }

        for (int i = 0; i < columns.length(); i++) {
            Column col = columns.column(i);

            Object value = tuple.valueOrDefault(col.name(), POISON_OBJECT);

            if (value != POISON_OBJECT)
                col.validate(value);
        }
    }

    /**
     * Creates {@link RowAssembler} for key-value tuples.
     *
     * @param keyTuple Key tuple.
     * @param valTuple Value tuple.
     * @return Row assembler.
     */
    private RowAssembler createAssembler(SchemaDescriptor schema, Tuple keyTuple, Tuple valTuple) {
        TupleStatistics keyStat = tupleStatistics(schema.keyColumns(), keyTuple);
        TupleStatistics valStat = tupleStatistics(schema.valueColumns(), valTuple);

        return new RowAssembler(
            schema,
            keyStat.nonNullVarLenSize,
            keyStat.nonNullVarlen,
            valStat.nonNullVarLenSize,
            valStat.nonNullVarlen);
    }

    /**
     * @param rowAsm Row assembler.
     * @param col Column.
     * @param tup Tuple.
     */
    private void writeColumn(RowAssembler rowAsm, Column col, Tuple tup) {
        Object val = tup.valueOrDefault(col.name(), col.defaultValue());

        RowAssembler.writeValue(rowAsm, col, val);
    }

    /**
     * Analyze given tuple and gather statistics.
     *
     * @param cols Columns which statistics is calculated for.
     * @param tup Tuple to analyze.
     * @return Tuple statistics.
     */
    private TupleStatistics tupleStatistics(Columns cols, Tuple tup) {
        if (tup == null || !cols.hasVarlengthColumns())
            return TupleStatistics.ZERO_VARLEN_STATISTICS;

        int cnt = 0;
        int size = 0;

        for (int i = cols.firstVarlengthColumn(); i < cols.length(); i++) {
            Column col = cols.column(i);

            final Object val = tup.valueOrDefault(col.name(), col.defaultValue());

            if (val == null)
                continue;

            size += getValueSize(val, col.type());
            cnt++;
        }

        return new TupleStatistics(cnt, size);
    }

    /**
     * Updates the schema with new columns.
     *
     * @param extraCols Columns to add.
     */
    private void createColumns(Set<org.apache.ignite.schema.Column> newCols) {
        //TODO: Introduce internal TableManager and use UUID instead of names ???
        tblMgr.alterTable(tbl.tableName(), chng -> chng.changeColumns(cols -> {
            int colIdx = chng.columns().size();
            //TODO: avoid 'colIdx' or replace with correct last colIdx.

            for (org.apache.ignite.schema.Column column : newCols) {
                cols.create(String.valueOf(colIdx), colChg -> convert(column, colChg));
                colIdx++;
            }
        }));
    }

    /**
     * Tuple statistics record.
     */
    private static class TupleStatistics {
        /** Cached zero statistics. */
        static final TupleStatistics ZERO_VARLEN_STATISTICS = new TupleStatistics(0, 0);

        /** Number of non-null varlen columns. */
        int nonNullVarlen;

        /** Length of all non-null fields of varlen types. */
        int nonNullVarLenSize;

        TupleStatistics(int nonNullVarlen, int nonNullVarLenSize) {
            this.nonNullVarlen = nonNullVarlen;
            this.nonNullVarLenSize = nonNullVarLenSize;
        }
    }
}
