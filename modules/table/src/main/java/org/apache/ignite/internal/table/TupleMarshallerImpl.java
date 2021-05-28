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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.TupleMarshaller;
import org.apache.ignite.internal.schema.SchemaRegistry;
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

        ChunkData keyChunk = chunkData(schema.keyColumns(), keyTuple);
        ChunkData valChunk = chunkData(schema.valueColumns(), valTuple);

        final RowAssembler rowBuilder = createAssembler(schema, keyChunk, valChunk);

        for (int i = 0; i < schema.keyColumns().length(); i++) {
            final Column col = schema.keyColumns().column(i);

            writeColumn(rowBuilder, col, keyChunk.data.get(i));
        }

        if (valChunk.data != null) {
            validate(valTuple, schema.valueColumns());

            for (int i = 0; i < schema.valueColumns().length(); i++) {
                final Column col = schema.valueColumns().column(i);

                writeColumn(rowBuilder, col, valChunk.data.get(i));
            }
        }

        return new Row(schema, new ByteBufferRow(rowBuilder.build()));
    }

    private ChunkData chunkData(Columns cols, Tuple tuple) {
        if (tuple == null)
            return new ChunkData();

        ChunkData chunk = new ChunkData();

        chunk.data = new HashMap<>();

        for (int i = 0; i < cols.length(); i++) {
            Column col = cols.column(i);

            Object val = (tuple.contains(col.name())) ? tuple.value(col.name()) : col.defaultValue();

            if (val == null)
                chunk.hasNulls = true;
            else {
                chunk.data.put(i, val);

                if (col.type().spec().fixedLength())
                    chunk.dataSize += col.type().sizeInBytes();
                else {
                    chunk.nonNullVarlen++;

                    chunk.dataSize += getValueSize(val, col.type());
                }
            }
        }

        return chunk;
    }

    class ChunkData {
        public boolean hasNulls;
        public int dataSize;
        public int nonNullVarlen;
        Map<Integer, Object> data;

    }

    /**
     *
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
     * @param keyTuple Key tuple.
     * @param valTuple Value tuple.
     * @return Row assembler.
     */
    private RowAssembler createAssembler(SchemaDescriptor schema, ChunkData keyChunk, ChunkData valChunk) {
        final int keyOffSize = RowAssembler.vartableOffSize(keyChunk.nonNullVarlen, keyChunk.dataSize);
        final int valOffSize = RowAssembler.vartableOffSize(valChunk.nonNullVarlen, valChunk.dataSize);

        int size = BinaryRow.HEADER_SIZE +
            (keyChunk.hasNulls ? schema.keyColumns().nullMapSize() : 0) +
            RowAssembler.vartableSize(valChunk.nonNullVarlen, valOffSize) +
            keyChunk.dataSize +
            (valChunk.hasNulls ? schema.valueColumns().nullMapSize() : 0) +
            RowAssembler.vartableSize(valChunk.nonNullVarlen, valOffSize) +
            valChunk.dataSize;

        return new RowAssembler(schema, size, keyOffSize, keyChunk.nonNullVarlen, valOffSize, valChunk.nonNullVarlen);
    }

    /**
     * @param rowAsm Row assembler.
     * @param col Column.
     * @param val Value.
     */
    private void writeColumn(RowAssembler rowAsm, Column col, Object val) {
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
}
