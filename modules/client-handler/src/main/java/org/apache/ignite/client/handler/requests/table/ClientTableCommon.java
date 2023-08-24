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

package org.apache.ignite.client.handler.requests.table;

import static org.apache.ignite.internal.client.proto.ClientMessageCommon.NO_VALUE;
import static org.apache.ignite.lang.ErrorGroups.Client.PROTOCOL_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.TABLE_ID_NOT_FOUND_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ClientResourceRegistry;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleContainer;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.manager.IgniteTables;
import org.jetbrains.annotations.Nullable;

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
    static void writeSchema(ClientMessagePacker packer, int schemaVer, SchemaDescriptor schema) {
        packer.packInt(schemaVer);

        if (schema == null) {
            packer.packNil();

            return;
        }

        var colCnt = schema.columnNames().size();
        packer.packArrayHeader(colCnt);

        for (var colIdx = 0; colIdx < colCnt; colIdx++) {
            var col = schema.column(colIdx);

            packer.packArrayHeader(7);
            packer.packString(col.name());
            packer.packInt(getColumnType(col.type().spec()).ordinal());
            packer.packBoolean(schema.isKeyColumn(colIdx));
            packer.packBoolean(col.nullable());
            packer.packInt(schema.colocationIndex(col));
            packer.packInt(getDecimalScale(col.type()));
            packer.packInt(getPrecision(col.type()));
        }
    }

    /**
     * Writes a tuple.
     *
     * @param packer Packer.
     * @param tuple  Tuple.
     */
    public static void writeTupleOrNil(ClientMessagePacker packer, Tuple tuple, TuplePart part, SchemaRegistry schemaRegistry) {
        if (tuple == null) {
            packer.packInt(schemaRegistry.lastSchemaVersion());
            packer.packNil();

            return;
        }

        writeTuple(packer, tuple, false, part);
    }

    /**
     * Writes a tuple.
     *
     * @param packer     Packer.
     * @param tuple      Tuple.
     * @param skipHeader Whether to skip the tuple header.
     * @param part       Which part of tuple to write.
     * @throws IgniteException on failed serialization.
     */
    private static void writeTuple(
            ClientMessagePacker packer,
            Tuple tuple,
            boolean skipHeader,
            TuplePart part
    ) {
        assert tuple != null;
        assert tuple instanceof SchemaAware : "Tuple must be a SchemaAware: " + tuple.getClass();
        assert part != TuplePart.VAL : "TuplePart.VAL is not supported";

        var schema = ((SchemaAware) tuple).schema();

        assert schema != null : "Schema must not be null: " + tuple.getClass();

        if (!skipHeader) {
            packer.packInt(schema.version());
        }

        assert tuple instanceof BinaryTupleContainer : "Tuple must be a BinaryTupleContainer: " + tuple.getClass();
        BinaryTupleReader binaryTuple = ((BinaryTupleContainer) tuple).binaryTuple();

        int elementCount = part == TuplePart.KEY ? schema.keyColumns().length() : schema.length();

        if (binaryTuple != null) {
            assert elementCount == binaryTuple.elementCount() :
                    "Tuple element count mismatch: " + elementCount + " != " + binaryTuple.elementCount() + " (" + tuple.getClass() + ")";

            packer.packBinaryTuple(binaryTuple);
        } else {
            // Underlying binary tuple is not available or can't be used as is, pack columns one by one.
            var builder = new BinaryTupleBuilder(elementCount);

            for (var i = 0; i < elementCount; i++) {
                var col = schema.column(i);
                Object v = tuple.valueOrDefault(col.name(), NO_VALUE);

                ClientBinaryTupleUtils.appendValue(builder, getColumnType(col.type().spec()), col.name(), getDecimalScale(col.type()), v);
            }

            packer.packBinaryTuple(builder);
        }
    }

    /**
     * Writes multiple tuples.
     *
     * @param packer         Packer.
     * @param tuples         Tuples.
     * @param schemaRegistry The registry.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuples(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            SchemaRegistry schemaRegistry) {
        writeTuples(packer, tuples, TuplePart.KEY_AND_VAL, schemaRegistry);
    }

    /**
     * Writes multiple tuples.
     *
     * @param packer         Packer.
     * @param tuples         Tuples.
     * @param part           Which part of tuple to write.
     * @param schemaRegistry The registry.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuples(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            TuplePart part,
            SchemaRegistry schemaRegistry
    ) {
        if (tuples == null || tuples.isEmpty()) {
            packer.packInt(schemaRegistry.lastSchemaVersion());
            packer.packInt(0);

            return;
        }

        Integer schemaVer = null;

        for (Tuple tuple : tuples) {
            assert tuple != null;

            var tupleSchemaVer = ((SchemaAware) tuple).schema().version();

            if (schemaVer == null) {
                schemaVer = tupleSchemaVer;
                packer.packInt(tupleSchemaVer);
                packer.packInt(tuples.size());
            } else {
                assert schemaVer.equals(tupleSchemaVer) : "All tuples must have the same schema version";
            }

            writeTuple(packer, tuple, true, part);
        }
    }

    /**
     * Writes multiple tuples with null flags.
     *
     * @param packer         Packer.
     * @param tuples         Tuples.
     * @param part           Which part of tuple to write.
     * @param schemaRegistry The registry.
     * @throws IgniteException on failed serialization.
     */
    public static void writeTuplesNullable(
            ClientMessagePacker packer,
            Collection<Tuple> tuples,
            TuplePart part,
            SchemaRegistry schemaRegistry
    ) {
        if (tuples == null || tuples.isEmpty()) {
            packer.packInt(schemaRegistry.lastSchemaVersion());
            packer.packInt(0);

            return;
        }

        Integer schemaVer = null;

        for (Tuple tuple : tuples) {
            if (tuple != null) {
                schemaVer = ((SchemaAware) tuple).schema().version();
                break;
            }
        }

        packer.packInt(schemaVer == null ? schemaRegistry.lastSchemaVersion() : schemaVer);
        packer.packInt(tuples.size());

        for (Tuple tuple : tuples) {
            if (tuple == null) {
                packer.packBoolean(false);
                continue;
            }

            assert schemaVer.equals(((SchemaAware) tuple).schema().version()) : "All tuples must have the same schema version";

            packer.packBoolean(true);
            writeTuple(packer, tuple, true, part);
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

        // NOTE: noValueSet is only present for client -> server communication.
        // It helps disambiguate two cases: 1 - column value is not set, 2 - column value is set to null explicitly.
        // If the column has a default value, it should be applied only in case 1.
        // https://cwiki.apache.org/confluence/display/IGNITE/IEP-76+Thin+Client+Protocol+for+Ignite+3.0#IEP76ThinClientProtocolforIgnite3.0-NullvsNoValue
        var noValueSet = unpacker.unpackBitSet();
        var binaryTupleReader = new BinaryTupleReader(cnt, unpacker.readBinary());

        return new ClientTuple(schema, noValueSet, binaryTupleReader, 0, cnt);
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
    public static CompletableFuture<TableImpl> readTableAsync(ClientMessageUnpacker unpacker, IgniteTables tables) {
        int tableId = unpacker.unpackInt();

        try {
            return ((IgniteTablesInternal) tables).tableAsync(tableId)
                    .thenApply(t -> {
                        if (t == null) {
                            throw new IgniteException(TABLE_ID_NOT_FOUND_ERR, "Table does not exist: " + tableId);
                        }

                        return t;
                    });
        } catch (NodeStoppingException e) {
            throw new IgniteException(e.traceId(), e.code(), e.getMessage(), e);
        }
    }

    /**
     * Reads transaction.
     *
     * @param in Unpacker.
     * @param out Packer.
     * @param resources Resource registry.
     * @return Transaction, if present, or null.
     */
    public static @Nullable InternalTransaction readTx(
            ClientMessageUnpacker in, ClientMessagePacker out, ClientResourceRegistry resources) {
        if (in.tryUnpackNil()) {
            return null;
        }

        try {
            var tx = resources.get(in.unpackLong()).get(InternalTransaction.class);

            if (tx != null && tx.isReadOnly()) {
                // For read-only tx, override observable timestamp that we send to the client:
                // use readTimestamp() instead of now().
                out.meta(tx.readTimestamp());
            }

            return tx;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteException(e.traceId(), e.code(), e.getMessage(), e);
        }
    }

    /**
     * Gets client type by server type.
     *
     * @param spec Type spec.
     * @return Client type code.
     */
    public static ColumnType getColumnType(NativeTypeSpec spec) {
        ColumnType columnType = spec.asColumnTypeOrNull();

        if (columnType == null) {
            throw new IgniteException(PROTOCOL_ERR, "Unsupported native type: " + spec);
        }

        return columnType;
    }

    /**
     * Gets type scale.
     *
     * @param type Type.
     * @return Scale.
     */
    public static int getDecimalScale(NativeType type) {
        return type instanceof DecimalNativeType ? ((DecimalNativeType) type).scale() : 0;
    }

    /**
     * Gets type precision.
     *
     * @param type Type.
     * @return Precision.
     */
    public static int getPrecision(NativeType type) {
        if (type instanceof NumberNativeType) {
            return ((NumberNativeType) type).precision();
        }

        if (type instanceof TemporalNativeType) {
            return ((TemporalNativeType) type).precision();
        }

        if (type instanceof DecimalNativeType) {
            return ((DecimalNativeType) type).precision();
        }

        return 0;
    }
}
