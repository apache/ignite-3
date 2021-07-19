package org.apache.ignite.client.fakes;

import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.schema.SchemaMode;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

    @Override public @NotNull UUID tableId() {
        return tableId;
    }

    @Override public @NotNull String tableName() {
        return tableName;
    }

    @Override public @NotNull SchemaMode schemaMode() {
        return null;
    }

    @Override public void schema(SchemaMode schemaMode) {

    }

    @Override public CompletableFuture<BinaryRow> get(BinaryRow keyRow, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Void> upsert(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Boolean> insert(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Boolean> replace(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<BinaryRow> getAndReplace(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Boolean> delete(BinaryRow keyRow, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Boolean> deleteExact(BinaryRow oldRow, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<BinaryRow> getAndDelete(BinaryRow row, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows, @Nullable Transaction tx) {
        return null;
    }

    @Override public CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows, @Nullable Transaction tx) {
        return null;
    }
}
