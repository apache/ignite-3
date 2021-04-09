package org.apache.ignite.internal.table.distributed.storage;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.PutCommand;
import org.apache.ignite.internal.table.distributed.command.response.TableRowResponse;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;

/**
 * Storage of table rows.
 */
public class InternalTableImpl implements InternalTable {

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(InternalTableImpl.class);

    /** Table id. */
    private final UUID tblId;

    /** Partition map. */
    private Map<Integer, RaftGroupService> partitionMap;

    /** Partitions. */
    private int partitions;

    /**
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     */
    public InternalTableImpl(
        UUID tableId,
        Map<Integer, RaftGroupService> partMap,
        int partitions
    ) {
        this.tblId = tableId;
        this.partitionMap = partMap;
        this.partitions = partitions;
    }

    @Override public @NotNull CompletableFuture<BinaryRow> get(BinaryRow keyRow) {
//        keyRow.writeTo();
        return partitionMap.get(extractAndWrapKey(keyRow).hashCode() % partitions).<TableRowResponse>run(new GetCommand(keyRow))
            .thenApply(response -> response.getValue());
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> getAll(Collection<BinaryRow> keyRows) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> upsert(BinaryRow row) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Void> upsertAll(Collection<BinaryRow> rows) {
        return null;
    }

    @Override public @NotNull CompletableFuture<BinaryRow> getAndUpsert(BinaryRow row) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> insert(BinaryRow row) {
        return partitionMap.get(extractAndWrapKey(row).hashCode() % partitions).run(new PutCommand(row))
            .thenApply(response -> true);
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> insertAll(Collection<BinaryRow> rows) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow row) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> replace(BinaryRow oldRow, BinaryRow newRow) {
        return null;
    }

    @Override public @NotNull CompletableFuture<BinaryRow> getAndReplace(BinaryRow row) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> delete(BinaryRow keyRow) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Boolean> deleteExact(BinaryRow oldRow) {
        return null;
    }

    @Override public @NotNull CompletableFuture<BinaryRow> getAndDelete(BinaryRow row) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAll(Collection<BinaryRow> rows) {
        return null;
    }

    @Override public @NotNull CompletableFuture<Collection<BinaryRow>> deleteAllExact(Collection<BinaryRow> rows) {
        return null;
    }

    /**
     * @param row Row.
     * @return Extracted key.
     */
    @NotNull private KeyWrapper extractAndWrapKey(@NotNull BinaryRow row) {
        final byte[] bytes = new byte[row.keySlice().capacity()];
        row.keySlice().get(bytes);

        return new KeyWrapper(bytes, row.hash());
    }

    /**
     * Wrapper provides correct byte[] comparison.
     */
    private static class KeyWrapper {
        /** Data. */
        private final byte[] data;

        /** Hash. */
        private final int hash;

        /**
         * Constructor.
         *
         * @param data Wrapped data.
         */
        KeyWrapper(byte[] data, int hash) {
            assert data != null;

            this.data = data;
            this.hash = hash;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            KeyWrapper wrapper = (KeyWrapper)o;
            return Arrays.equals(data, wrapper.data);
        }
    }
}
