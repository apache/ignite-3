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

package org.apache.ignite.internal.client.table;

import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.UNKNOWN_ERR;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.PayloadOutputChannel;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.tx.ClientTransaction;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client table API implementation.
 */
public class ClientTable implements Table {
    private final UUID id;

    private final String name;

    private final ReliableChannel ch;

    private final ConcurrentHashMap<Integer, ClientSchema> schemas = new ConcurrentHashMap<>();

    private volatile int latestSchemaVer = -1;

    private final Object latestSchemaLock = new Object();

    private volatile List<String> partitionAssignment = null;

    private volatile long partitionAssignmentVersion = -1;

    /**
     * Constructor.
     *
     * @param ch   Channel.
     * @param id   Table id.
     * @param name Table name.
     */
    public ClientTable(ReliableChannel ch, UUID id, String name) {
        assert ch != null;
        assert id != null;
        assert name != null && !name.isEmpty();

        this.ch = ch;
        this.id = id;
        this.name = name;
    }

    /**
     * Gets the table id.
     *
     * @return Table id.
     */
    public UUID tableId() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public <R> RecordView<R> recordView(Mapper<R> recMapper) {
        Objects.requireNonNull(recMapper);

        return new ClientRecordView<>(this, recMapper);
    }

    @Override
    public RecordView<Tuple> recordView() {
        return new ClientRecordBinaryView(this);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> KeyValueView<K, V> keyValueView(Mapper<K> keyMapper, Mapper<V> valMapper) {
        Objects.requireNonNull(keyMapper);
        Objects.requireNonNull(valMapper);

        return new ClientKeyValueView<>(this, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override
    public KeyValueView<Tuple, Tuple> keyValueView() {
        return new ClientKeyValueBinaryView(this);
    }

    private CompletableFuture<ClientSchema> getLatestSchema() {
        if (latestSchemaVer >= 0) {
            return CompletableFuture.completedFuture(schemas.get(latestSchemaVer));
        }

        return loadSchema(null);
    }

    private CompletableFuture<ClientSchema> getSchema(int ver) {
        var schema = schemas.get(ver);

        if (schema != null) {
            return CompletableFuture.completedFuture(schema);
        }

        return loadSchema(ver);
    }

    private CompletableFuture<ClientSchema> loadSchema(@Nullable Integer ver) {
        return ch.serviceAsync(ClientOp.SCHEMAS_GET, w -> {
            w.out().packUuid(id);

            if (ver == null) {
                w.out().packNil();
            } else {
                w.out().packArrayHeader(1);
                w.out().packInt(ver);
            }
        }, r -> {
            int schemaCnt = r.in().unpackMapHeader();

            if (schemaCnt == 0) {
                throw new IgniteException(UNKNOWN_ERR, "Schema not found: " + ver);
            }

            ClientSchema last = null;

            for (var i = 0; i < schemaCnt; i++) {
                last = readSchema(r.in());
            }

            return last;
        });
    }

    private ClientSchema readSchema(ClientMessageUnpacker in) {
        var schemaVer = in.unpackInt();
        var colCnt = in.unpackArrayHeader();

        var columns = new ClientColumn[colCnt];

        for (int i = 0; i < colCnt; i++) {
            var propCnt = in.unpackArrayHeader();

            assert propCnt >= 7;

            var name = in.unpackString();
            var type = in.unpackInt();
            var isKey = in.unpackBoolean();
            var isNullable = in.unpackBoolean();
            var isColocation = in.unpackBoolean();
            var scale = in.unpackInt();
            var precision = in.unpackInt();

            // Skip unknown extra properties, if any.
            in.skipValues(propCnt - 7);

            var column = new ClientColumn(name, type, isNullable, isKey, isColocation, i, scale, precision);
            columns[i] = column;
        }

        var schema = new ClientSchema(schemaVer, columns);

        schemas.put(schemaVer, schema);

        synchronized (latestSchemaLock) {
            if (schemaVer > latestSchemaVer) {
                latestSchemaVer = schemaVer;
            }
        }

        return schema;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return IgniteToStringBuilder.toString(ClientTable.class, this);
    }

    /**
     * Writes transaction, if present.
     *
     * @param tx  Transaction.
     * @param out Packer.
     */
    public static void writeTx(@Nullable Transaction tx, PayloadOutputChannel out) {
        if (tx == null) {
            out.out().packNil();
        } else {
            ClientTransaction clientTx = getClientTx(tx);

            if (clientTx.channel() != out.clientChannel()) {
                // Do not throw IgniteClientConnectionException to avoid retry kicking in.
                throw new IgniteException(CONNECTION_ERR, "Transaction context has been lost due to connection errors.");
            }

            out.out().packLong(clientTx.id());
        }
    }

    private static ClientTransaction getClientTx(@NotNull Transaction tx) {
        if (!(tx instanceof ClientTransaction)) {
            throw new IgniteException(UNKNOWN_ERR, "Unsupported transaction implementation: '"
                    + tx.getClass()
                    + "'. Use IgniteClient.transactions() to start transactions.");
        }

        return (ClientTransaction) tx;
    }

    <T> CompletableFuture<T> doSchemaOutInOpAsync(
            int opCode,
            BiConsumer<ClientSchema, PayloadOutputChannel> writer,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> reader
    ) {
        return doSchemaOutInOpAsync(opCode, writer, reader, null);
    }

    <T> CompletableFuture<T> doSchemaOutInOpAsync(
            int opCode,
            BiConsumer<ClientSchema, PayloadOutputChannel> writer,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> reader,
            @Nullable T defaultValue
    ) {
        return getLatestSchema()
                .thenCompose(schema ->
                        ch.serviceAsync(opCode,
                                w -> writer.accept(schema, w),
                                r -> readSchemaAndReadData(schema, r.in(), reader, defaultValue)))
                .thenCompose(t -> loadSchemaAndReadData(t, reader));
    }

    <T> CompletableFuture<T> doSchemaOutInOpAsync(
            int opCode,
            BiConsumer<ClientSchema, PayloadOutputChannel> writer,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> reader,
            @Nullable T defaultValue,
            @Nullable PartitionAwarenessProvider provider
    ) {
        CompletableFuture<ClientSchema> schemaFut = getLatestSchema();
        CompletableFuture<List<String>> partitionsFut = provider == null || !provider.isPartitionAwarenessEnabled()
                ? CompletableFuture.completedFuture(null)
                : getPartitionAssignment();

        return CompletableFuture.allOf(schemaFut, partitionsFut)
                .thenCompose(v -> {
                    ClientSchema schema = schemaFut.getNow(null);
                    String preferredNodeId = getPreferredNodeId(provider, partitionsFut.getNow(null), schema);

                    return ch.serviceAsync(opCode,
                            w -> writer.accept(schema, w),
                            r -> readSchemaAndReadData(schema, r.in(), reader, defaultValue),
                            null,
                            preferredNodeId);
                })
                .thenCompose(t -> loadSchemaAndReadData(t, reader));
    }

    /**
     * Performs a schema-based operation.
     *
     * @param opCode Op code.
     * @param writer Writer.
     * @param reader Reader.
     * @param <T> Result type.
     * @return Future representing pending completion of the operation.
     */
    public <T> CompletableFuture<T> doSchemaOutOpAsync(
            int opCode,
            BiConsumer<ClientSchema, PayloadOutputChannel> writer,
            Function<ClientMessageUnpacker, T> reader) {
        return getLatestSchema()
                .thenCompose(schema ->
                        ch.serviceAsync(opCode,
                                w -> writer.accept(schema, w),
                                r -> reader.apply(r.in())));
    }

    /**
     * Performs a schema-based operation.
     *
     * @param opCode Op code.
     * @param writer Writer.
     * @param reader Reader.
     * @param provider Partition awareness provider.
     * @param <T> Result type.
     * @return Future representing pending completion of the operation.
     */
    public <T> CompletableFuture<T> doSchemaOutOpAsync(
            int opCode,
            BiConsumer<ClientSchema, PayloadOutputChannel> writer,
            Function<ClientMessageUnpacker, T> reader,
            @Nullable PartitionAwarenessProvider provider) {

        CompletableFuture<ClientSchema> schemaFut = getLatestSchema();
        CompletableFuture<List<String>> partitionsFut = provider == null || !provider.isPartitionAwarenessEnabled()
                ? CompletableFuture.completedFuture(null)
                : getPartitionAssignment();

        return CompletableFuture.allOf(schemaFut, partitionsFut)
                .thenCompose(v -> {
                    ClientSchema schema = schemaFut.getNow(null);
                    String preferredNodeId = getPreferredNodeId(provider, partitionsFut.getNow(null), schema);

                    return ch.serviceAsync(opCode,
                            w -> writer.accept(schema, w),
                            r -> reader.apply(r.in()),
                            null,
                            preferredNodeId);
                });
    }

    private <T> @Nullable Object readSchemaAndReadData(
            ClientSchema knownSchema,
            ClientMessageUnpacker in,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> fn,
            @Nullable T defaultValue
    ) {
        if (in.tryUnpackNil()) {
            return defaultValue;
        }

        var schemaVer = in.unpackInt();

        var resSchema = schemaVer == knownSchema.version() ? knownSchema : schemas.get(schemaVer);

        if (resSchema != null) {
            return fn.apply(knownSchema, in);
        }

        // Schema is not yet known - request.
        // Retain unpacker - normally it is closed when this method exits.
        return new IgniteBiTuple<>(in.retain(), schemaVer);
    }

    private <T> CompletionStage<T> loadSchemaAndReadData(
            Object data,
            BiFunction<ClientSchema, ClientMessageUnpacker, T> fn
    ) {
        if (!(data instanceof IgniteBiTuple)) {
            return CompletableFuture.completedFuture((T) data);
        }

        var biTuple = (IgniteBiTuple<ClientMessageUnpacker, Integer>) data;

        var in = biTuple.getKey();
        var schemaId = biTuple.getValue();

        assert in != null;
        assert schemaId != null;

        var resFut = getSchema(schemaId).thenApply(schema -> fn.apply(schema, in));

        // Close unpacker.
        resFut.handle((tuple, err) -> {
            in.close();
            return null;
        });

        return resFut;
    }

    private CompletableFuture<List<String>> getPartitionAssignment() {
        var cached = partitionAssignment;

        if (cached != null && partitionAssignmentVersion == ch.partitionAssignmentVersion()) {
            return CompletableFuture.completedFuture(cached);
        }

        return loadPartitionAssignment();
    }

    private CompletableFuture<List<String>> loadPartitionAssignment() {
        partitionAssignmentVersion = ch.partitionAssignmentVersion();

        return ch.serviceAsync(ClientOp.PARTITION_ASSIGNMENT_GET,
                w -> w.out().packUuid(id),
                r -> {
                    int cnt = r.in().unpackArrayHeader();
                    List<String> res = new ArrayList<>(cnt);

                    for (int i = 0; i < cnt; i++) {
                        res.add(r.in().unpackString());
                    }

                    partitionAssignment = res;

                    return res;
                });
    }

    @Nullable
    private static String getPreferredNodeId(
            @Nullable PartitionAwarenessProvider provider,
            @Nullable List<String> partitions,
            ClientSchema schema) {
        if (provider == null) {
            return null;
        }

        @SuppressWarnings("resource")
        ClientChannel ch = provider.channel();

        if (ch != null) {
            return ch.protocolContext().clusterNode().id();
        }

        if (partitions == null || partitions.isEmpty()) {
            return null;
        }

        Integer hash = provider.getObjectHashCode(schema);

        if (hash == null) {
            return null;
        }

        return partitions.get(Math.abs(hash % partitions.size()));
    }
}
