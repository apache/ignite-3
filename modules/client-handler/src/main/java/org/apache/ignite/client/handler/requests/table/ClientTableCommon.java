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

package org.apache.ignite.client.handler.requests.table;

import static org.apache.ignite.internal.client.proto.ClientMessageCommon.NO_VALUE;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.UUID;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.client.proto.ClientDataType;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.msgpack.core.MessageTypeException;

/**
 * Common table functionality.
 */
public class ClientTableCommon {
    /**
     * Writes a schema.
     *
     * @param packer    Packer.
     * @param schemaVer Schema version.
     * @param schema    Schema.
     */
    public static void writeSchema(ClientMessagePacker packer, int schemaVer, SchemaDescriptor schema) {
        packer.packInt(schemaVer);

        if (schema == null) {
            packer.packNil();

            return;
        }

        var colCnt = schema.columnNames().size();
        packer.packArrayHeader(colCnt);

        for (var colIdx = 0; colIdx < colCnt; colIdx++) {
            var col = schema.column(colIdx);

            packer.packArrayHeader(4);
            packer.packString(col.name());
            packer.packInt(getClientDataType(col.type().spec()));
            packer.packBoolean(schema.isKeyColumn(colIdx));
            packer.packBoolean(col.nullable());
        }
    }

    /**
     * Writes a tuple.
     *
     * @param packer Packer.
     * @param tuple  Tuple.
     */
    public static void writeTupleOrNil(ClientMessagePacker packer, Tuple tuple) {
        if (tuple == null) {
            packer.packNil();

            return;
        }

        var schema = ((SchemaAware) tuple).schema();

        writeTuple(packer, tuple, schema);
    }

    /**
     * Writes a tuple.
     *
     * @param packer Packer.
     * @param tuple  Tuple.
     */
    public static void writeTupleOrNil(ClientMessagePacker packer, Tuple tuple, TuplePart part) {
        if (tuple == null) {
            packer.packNil();

            return;
        }

        var schema = ((SchemaAware) tuple).schema();

        writeTuple(packer, tuple, schema, false, part);
    }

    /**
     * Writes a tuple.
     *
     * @param packer Packer.
     * @param tuple  Tuple.
     * @param schema Tuple schema.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuple(
            ClientMessagePacker packer,
            Tuple tuple,
            SchemaDescriptor schema
    ) {
        writeTuple(packer, tuple, schema, false, TuplePart.KEY_AND_VAL);
    }

    /**
     * Writes a tuple.
     *
     * @param packer     Packer.
     * @param tuple      Tuple.
     * @param schema     Tuple schema.
     * @param skipHeader Whether to skip the tuple header.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuple(
            ClientMessagePacker packer,
            Tuple tuple,
            SchemaDescriptor schema,
            boolean skipHeader
    ) {
        writeTuple(packer, tuple, schema, skipHeader, TuplePart.KEY_AND_VAL);
    }

    /**
     * Writes a tuple.
     *
     * @param packer     Packer.
     * @param tuple      Tuple.
     * @param schema     Tuple schema.
     * @param skipHeader Whether to skip the tuple header.
     * @param part       Which part of tuple to write.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuple(
            ClientMessagePacker packer,
            Tuple tuple,
            SchemaDescriptor schema,
            boolean skipHeader,
            TuplePart part
    ) {
        assert tuple != null;

        if (!skipHeader) {
            packer.packInt(schema.version());
        }

        if (part != TuplePart.VAL) {
            for (var col : schema.keyColumns().columns()) {
                writeColumnValue(packer, tuple, col);
            }
        }

        if (part != TuplePart.KEY) {
            for (var col : schema.valueColumns().columns()) {
                writeColumnValue(packer, tuple, col);
            }
        }
    }

    /**
     * Writes multiple tuples.
     *
     * @param packer         Packer.
     * @param tuples         Tuples.
     * @param schemaRegistry The registry.
     * @param skipHeader     Whether to skip the tuple header.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuples(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            SchemaRegistry schemaRegistry,
            boolean skipHeader) {
        writeTuples(packer, tuples, TuplePart.KEY_AND_VAL, schemaRegistry, skipHeader);
    }

    /**
     * Writes multiple tuples.
     *
     * @param packer         Packer.
     * @param tuples         Tuples.
     * @param part           Which part of tuple to write.
     * @param schemaRegistry The registry.
     * @param skipHeader     Whether to skip the tuple header.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuples(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            TuplePart part,
            SchemaRegistry schemaRegistry,
            boolean skipHeader
    ) {
        if (tuples == null || tuples.isEmpty()) {
            packer.packNil();

            return;
        }

        SchemaDescriptor schema = schemaRegistry.schema();

        packer.packInt(schema.version());
        packer.packInt(tuples.size());

        for (Tuple tuple : tuples) {
            assert tuple != null;
            assert schema.version() == ((SchemaAware) tuple).schema().version();

            writeTuple(packer, tuple, schema, skipHeader, part);
        }
    }

    /**
     * Writes multiple tuples with null flags.
     *
     * @param packer         Packer.
     * @param tuples         Tuples.
     * @param part           Which part of tuple to write.
     * @param schemaRegistry The registry.
     * @param skipHeader     Whether to skip the tuple header.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuplesNullable(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            TuplePart part,
            SchemaRegistry schemaRegistry,
            boolean skipHeader
    ) {
        if (tuples == null || tuples.isEmpty()) {
            packer.packNil();

            return;
        }

        SchemaDescriptor schema = schemaRegistry.schema();

        packer.packInt(schema.version());
        packer.packInt(tuples.size());

        for (Tuple tuple : tuples) {
            if (tuple == null) {
                packer.packBoolean(false);
                continue;
            }

            assert schema.version() == ((SchemaAware) tuple).schema().version();

            packer.packBoolean(true);
            writeTuple(packer, tuple, schema, skipHeader, part);
        }
    }

    /**
     * Reads a tuple.
     *
     * @param unpacker Unpacker.
     * @param table    Table.
     * @param keyOnly  Whether only key fields are expected.
     * @return Tuple.
     */
    public static Tuple readTuple(ClientMessageUnpacker unpacker, TableImpl table, boolean keyOnly) {
        SchemaDescriptor schema = readSchema(unpacker, table);

        return readTuple(unpacker, keyOnly, schema);
    }

    /**
     * Reads a tuple.
     *
     * @param unpacker Unpacker.
     * @param keyOnly  Whether only key fields are expected.
     * @param schema   Tuple schema.
     * @return Tuple.
     */
    public static Tuple readTuple(
            ClientMessageUnpacker unpacker,
            boolean keyOnly,
            SchemaDescriptor schema
    ) {
        var cnt = keyOnly ? schema.keyColumns().length() : schema.length();

        var tuple = Tuple.create(cnt);

        for (int i = 0; i < cnt; i++) {
            if (unpacker.tryUnpackNoValue()) {
                continue;
            }

            readAndSetColumnValue(unpacker, tuple, schema.column(i));
        }

        return tuple;
    }

    /**
     * Reads multiple tuples.
     *
     * @param unpacker Unpacker.
     * @param table    Table.
     * @param keyOnly  Whether only key fields are expected.
     * @return Tuples.
     */
    public static ArrayList<Tuple> readTuples(ClientMessageUnpacker unpacker, TableImpl table, boolean keyOnly) {
        SchemaDescriptor schema = readSchema(unpacker, table);

        var rowCnt = unpacker.unpackInt();
        var res = new ArrayList<Tuple>(rowCnt);

        for (int i = 0; i < rowCnt; i++) {
            res.add(readTuple(unpacker, keyOnly, schema));
        }

        return res;
    }

    /**
     * Reads schema.
     *
     * @param unpacker Unpacker.
     * @param table    Table.
     * @return Schema descriptor.
     */
    @NotNull
    public static SchemaDescriptor readSchema(ClientMessageUnpacker unpacker, TableImpl table) {
        var schemaId = unpacker.unpackInt();

        return table.schemaView().schema(schemaId);
    }

    /**
     * Reads a table.
     *
     * @param unpacker Unpacker.
     * @param tables   Ignite tables.
     * @return Table.
     * @throws IgniteException If an unspecified platform exception has happened internally. Is thrown when:
     *                         <ul>
     *                             <li>the node is stopping.</li>
     *                         </ul>
     */
    public static TableImpl readTable(ClientMessageUnpacker unpacker, IgniteTables tables) {
        UUID tableId = unpacker.unpackUuid();

        try {
            TableImpl table = ((IgniteTablesInternal) tables).table(tableId);

            if (table == null) {
                throw new ClientTableIdDoesNotExistException("Table does not exist: " + tableId);
            }

            return table;
        } catch (NodeStoppingException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Reads transaction.
     *
     * @param in Unpacker.
     * @param resources Resource registry.
     * @return Transaction, if present, or null.
     */
    public static @Nullable Transaction readTx(ClientMessageUnpacker in, ClientResourceRegistry resources) {
        if (in.tryUnpackNil()) {
            return null;
        }

        try {
            return resources.get(in.unpackLong()).get(Transaction.class);
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalException(e.getMessage(), e);
        }
    }

    private static void readAndSetColumnValue(ClientMessageUnpacker unpacker, Tuple tuple, Column col) {
        try {
            int type = getClientDataType(col.type().spec());
            Object val = unpacker.unpackObject(type);
            tuple.set(col.name(), val);
        } catch (MessageTypeException e) {
            throw new IgniteException("Incorrect value type for column '" + col.name() + "': " + e.getMessage(), e);
        }
    }

    private static int getClientDataType(NativeTypeSpec spec) {
        switch (spec) {
            case INT8:
                return ClientDataType.INT8;

            case INT16:
                return ClientDataType.INT16;

            case INT32:
                return ClientDataType.INT32;

            case INT64:
                return ClientDataType.INT64;

            case FLOAT:
                return ClientDataType.FLOAT;

            case DOUBLE:
                return ClientDataType.DOUBLE;

            case DECIMAL:
                return ClientDataType.DECIMAL;

            case NUMBER:
                return ClientDataType.NUMBER;

            case UUID:
                return ClientDataType.UUID;

            case STRING:
                return ClientDataType.STRING;

            case BYTES:
                return ClientDataType.BYTES;

            case BITMASK:
                return ClientDataType.BITMASK;

            case DATE:
                return ClientDataType.DATE;

            case TIME:
                return ClientDataType.TIME;

            case DATETIME:
                return ClientDataType.DATETIME;

            case TIMESTAMP:
                return ClientDataType.TIMESTAMP;

            default:
                throw new IgniteException("Unsupported native type: " + spec);
        }
    }

    private static void writeColumnValue(ClientMessagePacker packer, Tuple tuple, Column col) {
        var val = tuple.valueOrDefault(col.name(), NO_VALUE);

        if (val == null) {
            packer.packNil();
            return;
        }

        if (val == NO_VALUE) {
            packer.packNoValue();
            return;
        }

        switch (col.type().spec()) {
            case INT8:
                packer.packByte((byte) val);
                break;

            case INT16:
                packer.packShort((short) val);
                break;

            case INT32:
                packer.packInt((int) val);
                break;

            case INT64:
                packer.packLong((long) val);
                break;

            case FLOAT:
                packer.packFloat((float) val);
                break;

            case DOUBLE:
                packer.packDouble((double) val);
                break;

            case DECIMAL:
                packer.packDecimal((BigDecimal) val);
                break;

            case NUMBER:
                packer.packNumber((BigInteger) val);
                break;

            case UUID:
                packer.packUuid((UUID) val);
                break;

            case STRING:
                packer.packString((String) val);
                break;

            case BYTES:
                byte[] bytes = (byte[]) val;
                packer.packBinaryHeader(bytes.length);
                packer.writePayload(bytes);
                break;

            case BITMASK:
                packer.packBitSet((BitSet) val);
                break;

            case DATE:
                packer.packDate((LocalDate) val);
                break;

            case TIME:
                packer.packTime((LocalTime) val);
                break;

            case DATETIME:
                packer.packDateTime((LocalDateTime) val);
                break;

            case TIMESTAMP:
                packer.packTimestamp((Instant) val);
                break;

            default:
                throw new IgniteException("Data type not supported: " + col.type());
        }
    }
}
