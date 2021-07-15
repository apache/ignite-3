package org.apache.ignite.client.fakes;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class FakeInternalTable implements InternalTable {
    private final String tableName;

    private final UUID tableId;

    public FakeInternalTable(String tableName, UUID tableId) {
        this.tableName = tableName;
        this.tableId = tableId;
    }

    @Override
    public @NotNull UUID tableId() {
        return tableId;
    }

    @Override
    public @NotNull String tableName() {
        return tableName;
    }

    @Override
    public @NotNull CompletableFuture<BinaryRow> get(BinaryRow keyRow) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Void> upsert(BinaryRow row) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> insert(BinaryRow row) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> replace(BinaryRow row) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<BinaryRow> getAndReplace(BinaryRow row) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> delete(BinaryRow keyRow) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Boolean> deleteExact(BinaryRow oldRow) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<BinaryRow> getAndDelete(BinaryRow row) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows) {
        return null;
    }

    @Override
    public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows) {
        return null;
    }
}
