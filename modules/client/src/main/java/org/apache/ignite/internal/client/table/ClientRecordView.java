package org.apache.ignite.internal.client.table;

import static org.apache.ignite.internal.client.proto.ClientDataType.BIGINTEGER;
import static org.apache.ignite.internal.client.proto.ClientDataType.BITMASK;
import static org.apache.ignite.internal.client.proto.ClientDataType.BOOLEAN;
import static org.apache.ignite.internal.client.proto.ClientDataType.BYTES;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATE;
import static org.apache.ignite.internal.client.proto.ClientDataType.DATETIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.DECIMAL;
import static org.apache.ignite.internal.client.proto.ClientDataType.DOUBLE;
import static org.apache.ignite.internal.client.proto.ClientDataType.FLOAT;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT16;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT32;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT64;
import static org.apache.ignite.internal.client.proto.ClientDataType.INT8;
import static org.apache.ignite.internal.client.proto.ClientDataType.NUMBER;
import static org.apache.ignite.internal.client.proto.ClientDataType.STRING;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIME;
import static org.apache.ignite.internal.client.proto.ClientDataType.TIMESTAMP;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientDataType;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.BinaryMode;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.MarshallerColumn;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.apache.ignite.internal.marshaller.Marshaller;

/**
 * Client record view implementation.
 */
public class ClientRecordView<R> implements RecordView<R> {
    /** Mapper. */
    private final Mapper<R> recMapper;

    /** Underlying table. */
    private final ClientTable tbl;

    public ClientRecordView(ClientTable tbl, Mapper<R> recMapper) {
        this.tbl = tbl;
        this.recMapper = recMapper;
    }

    /** {@inheritDoc} */
    @Override
    public R get(@NotNull R keyRec) {
        return getAsync(keyRec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAsync(@NotNull R keyRec) {
        Objects.requireNonNull(keyRec);

        // TODO: Read/write from POJO to messagepack directly without Tuple conversion - how?
        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> {
                    out.packIgniteUuid(tbl.tableId());
                    out.packInt(schema.version());

                    try {
                        // TODO: Cache marshallers per schema.
                        Marshaller keyMarsh = getMarshaller(schema, TuplePart.KEY);

                        keyMarsh.writeObject(keyRec, new ClientMarshallerWriter(out));
                    } catch (MarshallerException e) {
                        // TODO: ???
                        throw new RuntimeException(e);
                    }
                },
                (inSchema, in) -> {
                    // TODO: Copy key columns from key object
                    var marsh = getMarshaller(inSchema, TuplePart.VAL);

                    try {
                        return (R) marsh.readObject(new ClientMarshallerReader(in), null);
                    } catch (MarshallerException e) {
                        // TODO: ???
                        throw new RuntimeException(e);
                    }
                });
    }

    @Override
    public Collection<R> getAll(@NotNull Collection<R> keyRecs) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<R>> getAllAsync(@NotNull Collection<R> keyRecs) {
        return null;
    }

    @Override
    public void upsert(@NotNull R rec) {

    }

    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@NotNull R rec) {
        return null;
    }

    @Override
    public void upsertAll(@NotNull Collection<R> recs) {

    }

    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<R> recs) {
        return null;
    }

    @Override
    public R getAndUpsert(@NotNull R rec) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<R> getAndUpsertAsync(@NotNull R rec) {
        return null;
    }

    @Override
    public boolean insert(@NotNull R rec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull R rec) {
        return null;
    }

    @Override
    public Collection<R> insertAll(@NotNull Collection<R> recs) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<R>> insertAllAsync(@NotNull Collection<R> recs) {
        return null;
    }

    @Override
    public boolean replace(@NotNull R rec) {
        return false;
    }

    @Override
    public boolean replace(@NotNull R oldRec, @NotNull R newRec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R rec) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R oldRec, @NotNull R newRec) {
        return null;
    }

    @Override
    public R getAndReplace(@NotNull R rec) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<R> getAndReplaceAsync(@NotNull R rec) {
        return null;
    }

    @Override
    public boolean delete(@NotNull R keyRec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull R keyRec) {
        return null;
    }

    @Override
    public boolean deleteExact(@NotNull R rec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull R rec) {
        return null;
    }

    @Override
    public R getAndDelete(@NotNull R rec) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<R> getAndDeleteAsync(@NotNull R rec) {
        return null;
    }

    @Override
    public Collection<R> deleteAll(@NotNull Collection<R> recs) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllAsync(@NotNull Collection<R> recs) {
        return null;
    }

    @Override
    public Collection<R> deleteAllExact(@NotNull Collection<R> recs) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(@NotNull Collection<R> recs) {
        return null;
    }

    @Override
    public <T extends Serializable> T invoke(@NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(@NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    @Override
    public <T extends Serializable> Map<R, T> invokeAll(@NotNull Collection<R> keyRecs, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(@NotNull Collection<R> keyRecs,
            InvokeProcessor<R, R, T> proc) {
        return null;
    }

    @Override
    public @Nullable Transaction transaction() {
        return null;
    }

    @Override
    public RecordView<R> withTransaction(Transaction tx) {
        return null;
    }

    private Marshaller getMarshaller(ClientSchema schema, TuplePart part) {
        ClientColumn[] schemaCols = schema.columns();

        int colCount = schemaCols.length;
        int firstColIdx = 0;

        if (part == TuplePart.KEY) {
            colCount = schema.keyColumnCount();
        } else if (part == TuplePart.VAL) {
            colCount = schemaCols.length - schema.keyColumnCount();
            firstColIdx = schema.keyColumnCount();
        }

        MarshallerColumn[] cols = new MarshallerColumn[colCount];

        for (int i = 0; i < colCount; i++) {
            var col = schemaCols[i  + firstColIdx];

            cols[i] = new MarshallerColumn(col.name(), mode(col.type()));
        }

        return Marshaller.createMarshaller(cols, recMapper, part == TuplePart.KEY);
    }

    private static BinaryMode mode(int dataType) {
        switch (dataType) {
            case BOOLEAN:
                throw new IgniteException("TODO: " + dataType);

            case INT8:
                // TODO: P_BYTE?
                return BinaryMode.BYTE;

            case INT16:
                return BinaryMode.SHORT;

            case INT32:
                return BinaryMode.INT;

            case INT64:
                return BinaryMode.LONG;

            case FLOAT:
                return BinaryMode.FLOAT;

            case DOUBLE:
                return BinaryMode.DOUBLE;

            case ClientDataType.UUID:
                return BinaryMode.UUID;

            case STRING:
                return BinaryMode.STRING;

            case BYTES:
                return BinaryMode.BYTE_ARR;

            case DECIMAL:
                return BinaryMode.DECIMAL;

            case BIGINTEGER:
                return BinaryMode.NUMBER;

            case BITMASK:
                return BinaryMode.BITSET;

            case NUMBER:
                return BinaryMode.NUMBER;

            case DATE:
                return BinaryMode.DATE;

            case TIME:
                return BinaryMode.TIME;

            case DATETIME:
                return BinaryMode.DATETIME;

            case TIMESTAMP:
                return BinaryMode.TIMESTAMP;

            default:
                throw new IgniteException("Unknown client data type: " + dataType);
        }
    }
}
