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
import java.util.Objects;
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
public class AbstractPageMemoryMvPartitionStorage implements MvPartitionStorage {
    private static final byte[] TOMBSTONE_PAYLOAD = new byte[0];

    private static final Predicate<BinaryRow> MATCH_ALL = row -> true;

    private static final Predicate<Timestamp> ALWAYS_LOAD_VALUE = timestamp -> true;
    private static final Predicate<Timestamp> NEVER_LOAD_VALUE = timestamp -> false;
    private static final Predicate<Timestamp> LOAD_VALUE_WHEN_UNCOMMITTED = RowVersion::isUncommitted;

    private final int partId;
    private final int groupId;

    private final VersionChainFreeList versionChainFreeList;
    private final VersionChainTree versionChainTree;
    private final VersionChainDataPageReader versionChainDataPageReader;
    private final RowVersionFreeList rowVersionFreeList;
    private final DataPageReader rowVersionDataPageReader;

    private final ThreadLocal<ReadRowVersion> readRowVersionCache = ThreadLocal.withInitial(ReadRowVersion::new);
    private final ThreadLocal<ScanVersionChainByTimestamp> scanVersionChainByTimestampCache = ThreadLocal.withInitial(
            ScanVersionChainByTimestamp::new
    );

    /**
     * Last applied index value.
     */
    private volatile long lastAppliedIndex = 0;

    /**
     * Constructor.
     *
     * @param partId Partition id.
     * @param tableView Table configuration.
     * @param pageMemory Page memory.
     * @param versionChainFreeList Free list for {@link VersionChain}.
     * @param rowVersionFreeList Free list for {@link RowVersion}.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @throws StorageException If there is an error while creating the mv partition storage.
     */
    public AbstractPageMemoryMvPartitionStorage(
            int partId,
            TableView tableView,
            PageMemory pageMemory,
            VersionChainFreeList versionChainFreeList,
            RowVersionFreeList rowVersionFreeList,
            VersionChainTree versionChainTree
    ) {
        this.partId = partId;

        this.versionChainFreeList = versionChainFreeList;
        this.rowVersionFreeList = rowVersionFreeList;
        this.versionChainTree = versionChainTree;

        groupId = tableView.tableId();

        versionChainDataPageReader = new VersionChainDataPageReader(pageMemory, groupId, IoStatisticsHolderNoOp.INSTANCE);
        rowVersionDataPageReader = new DataPageReader(pageMemory, groupId, IoStatisticsHolderNoOp.INSTANCE);
    }

    /** {@inheritDoc} */
    @Override
    public long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public void lastAppliedIndex(long lastAppliedIndex) throws StorageException {
        this.lastAppliedIndex = lastAppliedIndex;
    }

    /** {@inheritDoc} */
    @Override
    public long persistedIndex() {
        return lastAppliedIndex;
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
            return versionChainDataPageReader.getRowByLink(versionChainLinkFrom(rowId));
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Version chain lookup failed", e);
        }
    }

    private long versionChainLinkFrom(RowId rowId) {
        if (rowId.partitionId() != partId) {
            throw new IllegalArgumentException("I own partition " + partId + " but I was given RowId with partition "
                    + rowId.partitionId());
        }

        LinkRowId linkRowId = (LinkRowId) rowId;

        return linkRowId.versionChainLink();
    }

    private @Nullable ByteBufferRow findLatestRowVersion(VersionChain versionChain, UUID txId, Predicate<BinaryRow> keyFilter) {
        RowVersion rowVersion = findLatestRowVersion(versionChain, ALWAYS_LOAD_VALUE);

        ByteBufferRow row = rowVersionToBinaryRow(rowVersion);

        if (!keyFilter.test(row)) {
            return null;
        }

        throwIfChainBelongsToAnotherTx(versionChain, txId);

        return row;
    }

    private RowVersion findLatestRowVersion(VersionChain versionChain, Predicate<Timestamp> loadValue) {
        long nextLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(versionChain.headLink(), partId);

        return readRowVersion(nextLink, loadValue);
    }

    private RowVersion readRowVersion(long nextLink, Predicate<Timestamp> loadValue) {
        ReadRowVersion read = freshReadRowVersion();

        try {
            rowVersionDataPageReader.traverse(nextLink, read, loadValue);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Row version lookup failed");
        }

        return read.result();
    }

    private ReadRowVersion freshReadRowVersion() {
        ReadRowVersion traversal = readRowVersionCache.get();

        traversal.reset();

        return traversal;
    }

    private void throwIfChainBelongsToAnotherTx(VersionChain versionChain, UUID txId) {
        if (versionChain.transactionId() != null && !txId.equals(versionChain.transactionId())) {
            throw new TxIdMismatchException();
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

        long newestCommittedRowPartitionlessLink = versionChain.newestCommittedPartitionlessLink();
        long newestCommittedLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(newestCommittedRowPartitionlessLink, partId);

        ScanVersionChainByTimestamp scanByTimestamp = freshScanByTimestamp();

        try {
            rowVersionDataPageReader.traverse(newestCommittedLink, scanByTimestamp, timestamp);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot search for a row version", e);
        }

        return scanByTimestamp.result();
    }

    private ScanVersionChainByTimestamp freshScanByTimestamp() {
        ScanVersionChainByTimestamp traversal = scanVersionChainByTimestampCache.get();

        traversal.reset();

        return traversal;
    }

    /** {@inheritDoc} */
    @Override
    public LinkRowId insert(BinaryRow row, UUID txId) throws StorageException {
        RowVersion rowVersion = insertRowVersion(Objects.requireNonNull(row), RowVersion.NULL_LINK);

        VersionChain versionChain = new VersionChain(
                partId,
                txId,
                PartitionlessLinks.removePartitionIdFromLink(rowVersion.link()),
                RowVersion.NULL_LINK
        );

        try {
            versionChainFreeList.insertDataRow(versionChain);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot store a version chain", e);
        }

        try {
            versionChainTree.putx(versionChain);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot put a version chain to the tree", e);
        }

        return new LinkRowId(versionChain.link());
    }

    private RowVersion insertRowVersion(@Nullable BinaryRow row, long nextPartitionlessLink) {
        // TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        byte[] rowBytes = row == null ? TOMBSTONE_PAYLOAD : row.bytes();

        RowVersion rowVersion = new RowVersion(partId, nextPartitionlessLink, ByteBuffer.wrap(rowBytes));

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
        VersionChain currentChain = findVersionChainForModification(rowId);

        throwIfChainBelongsToAnotherTx(currentChain, txId);

        RowVersion currentVersion = findLatestRowVersion(currentChain, LOAD_VALUE_WHEN_UNCOMMITTED);
        RowVersion newVersion = insertRowVersion(row, currentVersion.isUncommitted() ? currentVersion.nextLink() : currentChain.headLink());

        if (currentVersion.isUncommitted()) {
            // as we replace an uncommitted version with new one, we need to remove old uncommitted version
            removeRowVersion(currentVersion);
        }

        VersionChain chainReplacement = new VersionChain(
                partId,
                txId,
                PartitionlessLinks.removePartitionIdFromLink(newVersion.link()),
                currentChain.headLink()
        );

        updateVersionChain(currentChain, chainReplacement);

        if (currentVersion.isUncommitted()) {
            return rowVersionToBinaryRow(currentVersion);
        } else {
            return null;
        }
    }

    private VersionChain findVersionChainForModification(RowId rowId) {
        VersionChain currentChain = findVersionChain(rowId);

        if (currentChain == null) {
            throw new RowIdIsInvalidForModificationsException();
        }

        return currentChain;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        VersionChain currentVersionChain = findVersionChainForModification(rowId);

        if (currentVersionChain.transactionId() == null) {
            //the chain doesn't contain an uncommitted write intent
            return null;
        }

        RowVersion latestVersion = findLatestRowVersion(currentVersionChain, ALWAYS_LOAD_VALUE);

        assert latestVersion.isUncommitted();

        removeRowVersion(latestVersion);

        if (latestVersion.hasNextLink()) {
            // This load can be avoided, see the comment below.
            RowVersion latestCommittedVersion = readNextInChainOrderHeaderOnly(latestVersion);

            VersionChain versionChainReplacement = VersionChain.withoutTxId(
                    partId,
                    currentVersionChain.link(),
                    latestVersion.nextLink(),
                    // Next can be safely replaced with any value (like -1), because this field is only used when there
                    // is some uncommitted value, but when we add an uncommitted value, we 'fix' such placeholder value
                    // (like -1) by replacing it with a valid value. But it seems that this optimization is not critical
                    // as aborts are pretty rare; let's strive for internal consistency for now and write the correct value.
                    latestCommittedVersion.nextLink()
            );
            updateVersionChain(currentVersionChain, versionChainReplacement);
        } else {
            // it was the only version, let's remove the chain as well
            removeVersionChain(currentVersionChain);
        }

        return rowVersionToBinaryRow(latestVersion);
    }

    /**
     * Reads next row version in chain order (that is, the predecessor of the given version in creation order); payload is not loaded.
     *
     * @param rowVersion Version from which to start.
     * @return Next row version in chain order (that is, the predecessor of the given version in creation order).
     */
    private RowVersion readNextInChainOrderHeaderOnly(RowVersion rowVersion) {
        long preLatestVersionLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(rowVersion.nextLink(), partId);

        return readRowVersion(preLatestVersionLink, NEVER_LOAD_VALUE);
    }

    private void removeVersionChain(VersionChain currentVersionChain) {
        try {
            versionChainFreeList.removeDataRowByLink(currentVersionChain.link());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot remove chain version", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void commitWrite(RowId rowId, Timestamp timestamp) throws StorageException {
        VersionChain currentVersionChain = findVersionChainForModification(rowId);

        long chainLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(currentVersionChain.headLink(), partId);

        if (currentVersionChain.transactionId() == null) {
            //the chain doesn't contain an uncommitted write intent
            return;
        }

        try {
            rowVersionFreeList.updateTimestamp(chainLink, timestamp);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot update timestamp", e);
        }

        try {
            versionChainFreeList.updateTransactionId(currentVersionChain.link(), null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot update transaction ID", e);
        }
    }

    private void removeRowVersion(RowVersion currentVersion) {
        try {
            rowVersionFreeList.removeDataRowByLink(currentVersion.link());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot update row version");
        }
    }

    private void updateVersionChain(VersionChain currentVersionChain, VersionChain versionChainReplacement) {
        try {
            boolean updatedInPlace = versionChainFreeList.updateDataRow(currentVersionChain.link(), versionChainReplacement);

            if (!updatedInPlace) {
                throw new StorageException("Only in-place updates are supported");
            }
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
