package org.apache.ignite.internal.client.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallerUtil;
import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Client record view implementation.
 */
public class ClientRecordView<R> implements RecordView<R> {
    /** Mapper. */
    private final Mapper<R> recMapper;

    /** Underlying table. */
    private final ClientTable tbl;

    /** Simple mapping mode. */
    private final boolean isSimpleMapping;

    /**
     * Constructor.
     *
     * @param tbl Underlying table.
     * @param recMapper Mapper.
     */
    public ClientRecordView(ClientTable tbl, Mapper<R> recMapper) {
        this.tbl = tbl;
        this.recMapper = recMapper;

        isSimpleMapping = MarshallerUtil.mode(recMapper.targetType()) != null;
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> writeRec(keyRec, schema, out, TuplePart.KEY),
                (inSchema, in) -> readValRec(keyRec, inSchema, in));
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> getAll(@NotNull Collection<R> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> getAllAsync(@NotNull Collection<R> keyRecs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void upsert(@NotNull R rec) {
        upsertAsync(rec).join();
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@NotNull R rec) {
        Objects.requireNonNull(rec);

        return tbl.doSchemaOutOpAsync(
                ClientOp.TUPLE_UPSERT,
                (s, w) -> writeRec(rec, s, w, TuplePart.KEY_AND_VAL),
                r -> null);
    }

    /** {@inheritDoc} */
    @Override
    public void upsertAll(@NotNull Collection<R> recs) {

    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public R getAndUpsert(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndUpsertAsync(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean insert(@NotNull R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> insertAll(@NotNull Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> insertAllAsync(@NotNull Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public boolean replace(@NotNull R oldRec, @NotNull R newRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull R oldRec, @NotNull R newRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public R getAndReplace(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndReplaceAsync(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean delete(@NotNull R keyRec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull R keyRec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public boolean deleteExact(@NotNull R rec) {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public R getAndDelete(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<R> getAndDeleteAsync(@NotNull R rec) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAll(@NotNull Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllAsync(@NotNull Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<R> deleteAllExact(@NotNull Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Collection<R>> deleteAllExactAsync(@NotNull Collection<R> recs) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> T invoke(@NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(@NotNull R keyRec, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public <T extends Serializable> Map<R, T> invokeAll(@NotNull Collection<R> keyRecs, InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<R, T>> invokeAllAsync(@NotNull Collection<R> keyRecs,
            InvokeProcessor<R, R, T> proc) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Transaction transaction() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public RecordView<R> withTransaction(Transaction tx) {
        return null;
    }

    private void writeRec(@NotNull R rec, ClientSchema schema, ClientMessagePacker out, TuplePart part) {
        out.packIgniteUuid(tbl.tableId());
        out.packInt(schema.version());

        try {
            schema.getMarshaller(recMapper, part).writeObject(rec, new ClientMarshallerWriter(out));
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }

    private R readValRec(@NotNull R keyRec, ClientSchema inSchema, ClientMessageUnpacker in) {
        if (isSimpleMapping) {
            return keyRec;
        }

        try {
            var res = (R) inSchema.getMarshaller(recMapper, TuplePart.VAL).readObject(new ClientMarshallerReader(in), null);

            inSchema.getMarshaller(recMapper, TuplePart.KEY).copyObject(keyRec, res);

            return res;
        } catch (MarshallerException e) {
            throw new IgniteClientException(e.getMessage(), e);
        }
    }
}
