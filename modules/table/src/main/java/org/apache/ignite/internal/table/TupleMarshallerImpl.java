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

import java.util.Arrays;
import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

/**
 * Tuple marshaller implementation.
 */
public class TupleMarshallerImpl implements TupleMarshaller {
    /** Schema manager. */
    private final SchemaRegistry schemaReg;

    /**
     * Constructor.
     *
     * @param schemaReg Schema manager.
     */
    public TupleMarshallerImpl(SchemaRegistry schemaReg) {
        this.schemaReg = schemaReg;
    }

    /** {@inheritDoc} */
    @Override public Row marshal(@NotNull Tuple tuple) {
        return marshal(tuple, tuple);
    }

    /** {@inheritDoc} */
    @Override public Row marshal(@NotNull Tuple keyTuple, @Nullable Tuple valTuple) {
        final SchemaDescriptor schema = schemaReg.schema();

        validate(keyTuple, schema.keyColumns());

        TupleStatistics keyStat = tupleStatistics(schema.keyColumns(), keyTuple);
        TupleStatistics valStat = tupleStatistics(schema.valueColumns(), valTuple);

        final RowAssembler rowBuilder = createAssembler(schema, keyStat, valStat);

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

        return new Row(schema, new ByteBufferRow(rowBuilder.build()));
    }

    /**
     * Analyze given tuple and gather statistics.
     *
     * @param cols Columns which statistics is calculated for.
     * @param tup Tuple to analyze.
     * @return Tuple statistics.
     */
    private TupleStatistics tupleStatistics(Columns cols, Tuple tup) {
        if (tup == null)
            return new TupleStatistics();

        TupleStatistics chunk = new TupleStatistics();

        chunk.payloadLen = cols.fixsizeMaxLen();

        if (!cols.hasVarlengthColumns())
            return chunk;

        for (int i = cols.firstVarlengthColumn(); i < cols.length(); i++) {
            Column col = cols.column(i);

            Object val = (tup.contains(col.name())) ? tup.value(col.name()) : col.defaultValue();

            if (val == null)
                continue;

            chunk.nonNullVarlen++;
            chunk.payloadLen += getValueSize(val, col.type());
        }

        return chunk;
    }

    /**
     * Validates columns values.
     *
     * @param tuple Tuple to validate.
     * @param columns Columns to validate against.
     * @throws SchemaMismatchException If validation failed.
     */
    private void validate(Tuple tuple, Columns columns) {
        if (tuple instanceof TupleBuilderImpl) {
            TupleBuilderImpl t0 = (TupleBuilderImpl)tuple;

            SchemaDescriptor expSchema = schemaReg.schema(t0.schema().version());

            if (!Objects.equals(t0.schema(), expSchema))
                throw new SchemaMismatchException("Unexpected schema: [expected=" + expSchema + ", actual=" + t0.schema() + ']');
        }
        else {
            Arrays.stream(columns.columns()).forEach(c -> c.validate(tuple.value(c.name())));
        }
    }

    /**
     * Creates {@link RowAssembler} for key-value tuples.
     *
     * @param keyStat Key tuple statistics.
     * @param valStat Value tuple statistics.
     * @return Row assembler.
     */
    private RowAssembler createAssembler(SchemaDescriptor schema, TupleStatistics keyStat, TupleStatistics valStat) {
        return new RowAssembler(
            schema,
            keyStat.payloadLen,
            keyStat.nonNullVarlen,
            valStat.payloadLen,
            valStat.nonNullVarlen);
    }

    /**
     * @param rowAsm Row assembler.
     * @param col Column.
     * @param tup Tuple.
     */
    private void writeColumn(RowAssembler rowAsm, Column col, Tuple tup) {
        Object val = tup.contains(col.name()) ? tup.value(col.name()) : col.defaultValue();

        if (val == null) {
            rowAsm.appendNull();

            return;
        }

        switch (col.type().spec()) {
            case BYTE: {
                rowAsm.appendByte((byte)val);

                break;
            }
            case SHORT: {
                rowAsm.appendShort((short)val);

                break;
            }
            case INTEGER: {
                rowAsm.appendInt((int)val);

                break;
            }
            case LONG: {
                rowAsm.appendLong((long)val);

                break;
            }
            case FLOAT: {
                rowAsm.appendFloat((float)val);

                break;
            }
            case DOUBLE: {
                rowAsm.appendDouble((double)val);

                break;
            }
            case UUID: {
                rowAsm.appendUuid((UUID)val);

                break;
            }
            case STRING: {
                rowAsm.appendString((String)val);

                break;
            }
            case BYTES: {
                rowAsm.appendBytes((byte[])val);

                break;
            }
            case BITMASK: {
                rowAsm.appendBitmask((BitSet)val);

                break;
            }
            default:
                throw new IllegalStateException("Unexpected value: " + col.type());
        }
    }

    /**
     * Tuple statistics record.
     */
    private static class TupleStatistics {
        /** Payload length in bytes. */
        int payloadLen;

        /** Number of non-null varlen columns. */
        int nonNullVarlen;
    }
}
