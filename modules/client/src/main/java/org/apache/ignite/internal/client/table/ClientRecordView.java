package org.apache.ignite.internal.client.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.marshaller.ClientMarshallerReader;
import org.apache.ignite.internal.marshaller.ClientMarshallerWriter;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
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

        return tbl.doSchemaOutInOpAsync(
                ClientOp.TUPLE_GET,
                (schema, out) -> {
                    out.packIgniteUuid(tbl.tableId());
                    out.packInt(schema.version());

                    try {
                        Marshaller keyMarsh = schema.getKeyMarshaller(recMapper);

                        keyMarsh.writeObject(keyRec, new ClientMarshallerWriter(out));
                    } catch (MarshallerException e) {
                        // TODO: ???
                        throw new RuntimeException(e);
                    }
                },
                (inSchema, in) -> {
                    var marsh = inSchema.getValMarshaller(recMapper);

                    try {
                        var res = (R) marsh.readObject(new ClientMarshallerReader(in), null);

                        if (!marsh.isSimple()) {
                            Marshaller keyMarsh = inSchema.getKeyMarshaller(recMapper);

                            keyMarsh.copyObject(keyRec, res);
                        }

                        return res;
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
}
