package org.apache.ignite.client.fakes;

import org.apache.ignite.table.InvokeProcessor;
import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.TupleBuilder;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class FakeTable implements Table {
    private final String tableName;

    public FakeTable(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public @NotNull String tableName() {
        return tableName;
    }

    @Override
    public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return null;
    }

    @Override
    public <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return null;
    }

    @Override
    public KeyValueBinaryView kvView() {
        return null;
    }

    @Override
    public TupleBuilder tupleBuilder() {
        return null;
    }

    @Override
    public Tuple get(@NotNull Tuple keyRec) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Tuple> getAsync(@NotNull Tuple keyRec) {
        return null;
    }

    @Override
    public Collection<Tuple> getAll(@NotNull Collection<Tuple> keyRecs) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> getAllAsync(@NotNull Collection<Tuple> keyRecs) {
        return null;
    }

    @Override
    public void upsert(@NotNull Tuple rec) {

    }

    @Override
    public @NotNull CompletableFuture<Void> upsertAsync(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public void upsertAll(@NotNull Collection<Tuple> recs) {

    }

    @Override
    public @NotNull CompletableFuture<Void> upsertAllAsync(@NotNull Collection<Tuple> recs) {
        return null;
    }

    @Override
    public Tuple getAndUpsert(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Tuple> getAndUpsertAsync(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public boolean insert(@NotNull Tuple rec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> insertAsync(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public Collection<Tuple> insertAll(@NotNull Collection<Tuple> recs) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> insertAllAsync(@NotNull Collection<Tuple> recs) {
        return null;
    }

    @Override
    public boolean replace(@NotNull Tuple rec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public boolean replace(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> replaceAsync(@NotNull Tuple oldRec, @NotNull Tuple newRec) {
        return null;
    }

    @Override
    public Tuple getAndReplace(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Tuple> getAndReplaceAsync(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public boolean delete(@NotNull Tuple keyRec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> deleteAsync(@NotNull Tuple keyRec) {
        return null;
    }

    @Override
    public boolean deleteExact(@NotNull Tuple rec) {
        return false;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> deleteExactAsync(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public Tuple getAndDelete(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Tuple> getAndDeleteAsync(@NotNull Tuple rec) {
        return null;
    }

    @Override
    public Collection<Tuple> deleteAll(@NotNull Collection<Tuple> recs) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllAsync(@NotNull Collection<Tuple> recs) {
        return null;
    }

    @Override
    public Collection<Tuple> deleteAllExact(@NotNull Collection<Tuple> recs) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<Tuple>> deleteAllExactAsync(@NotNull Collection<Tuple> recs) {
        return null;
    }

    @Override
    public <T extends Serializable> T invoke(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        return null;
    }

    @Override
    public @NotNull <T extends Serializable> CompletableFuture<T> invokeAsync(@NotNull Tuple keyRec, InvokeProcessor<Tuple, Tuple, T> proc) {
        return null;
    }

    @Override
    public <T extends Serializable> Map<Tuple, T> invokeAll(@NotNull Collection<Tuple> keyRecs, InvokeProcessor<Tuple, Tuple, T> proc) {
        return null;
    }

    @Override
    public @NotNull <T extends Serializable> CompletableFuture<Map<Tuple, T>> invokeAllAsync(@NotNull Collection<Tuple> keyRecs, InvokeProcessor<Tuple, Tuple, T> proc) {
        return null;
    }
}
