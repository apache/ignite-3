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

package org.apache.ignite.internal.storage.pagememory.mv;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteCursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract implementation of {@link MvPartitionStorage} using Page Memory.
 *
 * @see MvPartitionStorage
 */
public abstract class AbstractPageMemoryMvPartitionStorage implements MvPartitionStorage {
    private static final byte[] TOMBSTONE_PAYLOAD = new byte[0];

    private static final Predicate<BinaryRow> MATCH_ALL = row -> true;

    private static final Predicate<Timestamp> ALWAYS_LOAD_VALUE = timestamp -> true;

    private final int partitionId;
    private final int groupId;

    private final VersionChainTree versionChainTree;
    protected final RowVersionFreeList rowVersionFreeList;
    private final DataPageReader rowVersionDataPageReader;

    /**
     * Constructor.
     *
     * @param partitionId Partition id.
     * @param tableView Table configuration.
     * @param pageMemory Page memory.
     * @param rowVersionFreeList Free list for {@link RowVersion}.
     * @param versionChainTree Table tree for {@link VersionChain}.
     */
    protected AbstractPageMemoryMvPartitionStorage(
            int partitionId,
            TableView tableView,
            PageMemory pageMemory,
            RowVersionFreeList rowVersionFreeList,
            VersionChainTree versionChainTree
    ) {
        this.partitionId = partitionId;

        this.rowVersionFreeList = rowVersionFreeList;
        this.versionChainTree = versionChainTree;

        groupId = tableView.tableId();

        rowVersionDataPageReader = new DataPageReader(pageMemory, groupId, IoStatisticsHolderNoOp.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow read(RowId rowId, UUID txId) throws TxIdMismatchException, StorageException {
        VersionChain versionChain = findVersionChain(rowId);

        if (versionChain == null) {
            return null;
        }

        return findLatestRowVersion(versionChain, txId, MATCH_ALL);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow read(RowId rowId, Timestamp timestamp) throws StorageException {
        VersionChain versionChain = findVersionChain(rowId);

        if (versionChain == null) {
            return null;
        }

        return findRowVersionByTimestamp(versionChain, timestamp);
    }

    private @Nullable VersionChain findVersionChain(RowId rowId) {
        try {
            return versionChainTree.findOne(new VersionChainKey(rowId));
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Version chain lookup failed", e);
        }
    }

    private @Nullable ByteBufferRow findLatestRowVersion(VersionChain versionChain, UUID txId, Predicate<BinaryRow> keyFilter) {
        RowVersion rowVersion = readRowVersion(versionChain.headLink(), ALWAYS_LOAD_VALUE);

        ByteBufferRow row = rowVersionToBinaryRow(rowVersion);

        if (!keyFilter.test(row)) {
            return null;
        }

        throwIfChainBelongsToAnotherTx(versionChain, txId);

        return row;
    }

    private RowVersion readRowVersion(long nextLink, Predicate<Timestamp> loadValue) {
        ReadRowVersion read = new ReadRowVersion(partitionId);

        try {
            rowVersionDataPageReader.traverse(nextLink, read, loadValue);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Row version lookup failed");
        }

        return read.result();
    }

    private void throwIfChainBelongsToAnotherTx(VersionChain versionChain, UUID txId) {
        if (versionChain.transactionId() != null && !txId.equals(versionChain.transactionId())) {
            throw new TxIdMismatchException(txId, versionChain.transactionId());
        }
    }

    private @Nullable ByteBufferRow rowVersionToBinaryRow(RowVersion rowVersion) {
        if (rowVersion.isTombstone()) {
            return null;
        }

        return new ByteBufferRow(rowVersion.value());
    }

    private @Nullable ByteBufferRow findRowVersionInChain(
            VersionChain versionChain,
            @Nullable UUID transactionId,
            @Nullable Timestamp timestamp,
            Predicate<BinaryRow> keyFilter
    ) {
        assert transactionId != null ^ timestamp != null;

        if (transactionId != null) {
            return findLatestRowVersion(versionChain, transactionId, keyFilter);
        } else {
            ByteBufferRow row = findRowVersionByTimestamp(versionChain, timestamp);

            return keyFilter.test(row) ? row : null;
        }
    }

    private @Nullable ByteBufferRow findRowVersionByTimestamp(VersionChain versionChain, Timestamp timestamp) {
        if (!versionChain.hasCommittedVersions()) {
            return null;
        }

        long newestCommittedLink = versionChain.newestCommittedLink();

        ScanVersionChainByTimestamp scanByTimestamp = new ScanVersionChainByTimestamp(partitionId);

        try {
            rowVersionDataPageReader.traverse(newestCommittedLink, scanByTimestamp, timestamp);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot search for a row version", e);
        }

        return scanByTimestamp.result();
    }

    /** {@inheritDoc} */
    @Override
    public RowId insert(BinaryRow row, UUID txId) throws StorageException {
        RowId rowId = new RowId(partitionId);

        addWrite(rowId, row, txId);

        return rowId;
    }

    private RowVersion insertRowVersion(@Nullable BinaryRow row, long nextPartitionlessLink) {
        // TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        byte[] rowBytes = row == null ? TOMBSTONE_PAYLOAD : row.bytes();

        RowVersion rowVersion = new RowVersion(partitionId, nextPartitionlessLink, ByteBuffer.wrap(rowBytes));

        insertRowVersion(rowVersion);

        return rowVersion;
    }

    private void insertRowVersion(RowVersion rowVersion) {
        try {
            rowVersionFreeList.insertDataRow(rowVersion);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot store a row version", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId) throws TxIdMismatchException, StorageException {

        VersionChain currentChain = findVersionChain(rowId);

        if (currentChain == null) {
            RowVersion newVersion = insertRowVersion(row, RowVersion.NULL_LINK);

            VersionChain versionChain = new VersionChain(rowId, txId, newVersion.link(), RowVersion.NULL_LINK);

            updateVersionChain(versionChain);

            return null;
        }

        throwIfChainBelongsToAnotherTx(currentChain, txId);

        RowVersion newVersion = insertRowVersion(row, currentChain.newestCommittedLink());

        BinaryRow res = null;

        if (currentChain.isUncommitted()) {
            RowVersion currentVersion = readRowVersion(currentChain.headLink(), ALWAYS_LOAD_VALUE);

            res = rowVersionToBinaryRow(currentVersion);

            // as we replace an uncommitted version with new one, we need to remove old uncommitted version
            removeRowVersion(currentVersion);
        }

        VersionChain chainReplacement = new VersionChain(rowId, txId, newVersion.link(), newVersion.nextLink());

        updateVersionChain(chainReplacement);

        return res;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        VersionChain currentVersionChain = findVersionChain(rowId);

        if (currentVersionChain == null || currentVersionChain.transactionId() == null) {
            // Row doesn't exist or the chain doesn't contain an uncommitted write intent.
            return null;
        }

        RowVersion latestVersion = readRowVersion(currentVersionChain.headLink(), ALWAYS_LOAD_VALUE);

        assert latestVersion.isUncommitted();

        removeRowVersion(latestVersion);

        if (latestVersion.hasNextLink()) {
            // Next can be safely replaced with any value (like 0), because this field is only used when there
            // is some uncommitted value, but when we add an uncommitted value, we 'fix' such placeholder value
            // (like 0) by replacing it with a valid value.
            VersionChain versionChainReplacement = new VersionChain(rowId, null, latestVersion.nextLink(), RowVersion.NULL_LINK);

            updateVersionChain(versionChainReplacement);
        } else {
            // it was the only version, let's remove the chain as well
            removeVersionChain(currentVersionChain);
        }

        return rowVersionToBinaryRow(latestVersion);
    }

    private void removeVersionChain(VersionChain currentVersionChain) {
        try {
            versionChainTree.remove(currentVersionChain);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot remove chain version", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(RowId rowId, Timestamp timestamp) throws StorageException {
        VersionChain currentVersionChain = findVersionChain(rowId);

        if (currentVersionChain == null || currentVersionChain.transactionId() == null) {
            // Row doesn't exist or the chain doesn't contain an uncommitted write intent.
            return;
        }

        long chainLink = currentVersionChain.headLink();

        try {
            rowVersionFreeList.updateTimestamp(chainLink, timestamp);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot update timestamp", e);
        }

        try {
            VersionChain updatedVersionChain = new VersionChain(
                    currentVersionChain.rowId(),
                    null,
                    currentVersionChain.headLink(),
                    currentVersionChain.nextLink()
            );

            versionChainTree.putx(updatedVersionChain);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot update transaction ID", e);
        }
    }

    private void removeRowVersion(RowVersion currentVersion) {
        try {
            rowVersionFreeList.removeDataRowByLink(currentVersion.link());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot update row version", e);
        }
    }

    private void updateVersionChain(VersionChain newVersionChain) {
        try {
            versionChainTree.putx(newVersionChain);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot update version chain");
        }
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, UUID txId) throws TxIdMismatchException, StorageException {
        return internalScan(keyFilter, txId, null);
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, Timestamp timestamp) throws StorageException {
        return internalScan(keyFilter, null, timestamp);
    }

    private Cursor<BinaryRow> internalScan(Predicate<BinaryRow> keyFilter, @Nullable UUID transactionId, @Nullable Timestamp timestamp) {
        assert transactionId != null ^ timestamp != null;

        IgniteCursor<VersionChain> treeCursor;

        try {
            treeCursor = versionChainTree.find(null, null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Find failed", e);
        }

        return new ScanCursor(treeCursor, keyFilter, transactionId, timestamp);
    }

    /** {@inheritDoc} */
    @Override
    public long rowsCount() {
        try {
            return versionChainTree.size();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error occurred while fetching the size.", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void forEach(BiConsumer<RowId, BinaryRow> consumer) {
        // No-op. Nothing to recover for a volatile storage. See usages and a comment about PK index rebuild.
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        versionChainTree.close();
    }

    /**
     * Removes all data from this storage and frees all associated resources.
     *
     * @throws StorageException If failed to destroy the data or storage is already stopped.
     */
    public void destroy() {
        // TODO: IGNITE-17132 Implement it
    }

    private class ScanCursor implements Cursor<BinaryRow> {
        private final IgniteCursor<VersionChain> treeCursor;

        private final Predicate<BinaryRow> keyFilter;

        private final @Nullable UUID transactionId;

        private final @Nullable Timestamp timestamp;

        private BinaryRow nextRow = null;

        private boolean iterationExhausted = false;

        public ScanCursor(
                IgniteCursor<VersionChain> treeCursor,
                Predicate<BinaryRow> keyFilter,
                @Nullable UUID transactionId,
                @Nullable Timestamp timestamp
        ) {
            this.treeCursor = treeCursor;
            this.keyFilter = keyFilter;
            this.transactionId = transactionId;
            this.timestamp = timestamp;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            if (nextRow != null) {
                return true;
            }

            if (iterationExhausted) {
                return false;
            }

            while (true) {
                boolean positionedToNext = tryAdvanceTreeCursor();

                if (!positionedToNext) {
                    iterationExhausted = true;
                    return false;
                }

                VersionChain chain = getCurrentChainFromTreeCursor();
                ByteBufferRow row = findRowVersionInChain(chain, transactionId, timestamp, keyFilter);

                if (row != null) {
                    nextRow = row;
                    return true;
                }
            }
        }

        private boolean tryAdvanceTreeCursor() {
            try {
                return treeCursor.next();
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error when trying to advance tree cursor", e);
            }
        }

        private VersionChain getCurrentChainFromTreeCursor() {
            try {
                return treeCursor.get();
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Failed to get element from tree cursor", e);
            }
        }

        /** {@inheritDoc} */
        @Override
        public BinaryRow next() {
            if (!hasNext()) {
                throw new NoSuchElementException("The cursor is exhausted");
            }

            assert nextRow != null;

            BinaryRow row = nextRow;
            nextRow = null;

            return row;
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            // no-op
        }
    }
}
