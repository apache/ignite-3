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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.PageMemoryDataRegion;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.NoUncommittedVersionException;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageUtils;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.tx.Timestamp;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteCursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link MvPartitionStorage} using Page Memory.
 *
 * @see MvPartitionStorage
 */
public class PageMemoryMvPartitionStorage implements MvPartitionStorage {
    private static final byte[] TOMBSTONE_PAYLOAD = new byte[0];

    private static final Predicate<BinaryRow> MATCH_ALL = row -> true;

    private static final Predicate<Timestamp> ALWAYS_LOAD_VALUE = timestamp -> true;
    private static final Predicate<Timestamp> LOAD_VALUE_WHEN_UNCOMMITTED = RowVersion::isUncommitted;

    private final int partitionId;
    private final int groupId;

    private final VersionChainFreeList versionChainFreeList;
    private final VersionChainTree versionChainTree;
    private final VersionChainDataPageReader versionChainDataPageReader;
    private final RowVersionFreeList rowVersionFreeList;
    private final DataPageReader rowVersionDataPageReader;

    private final ThreadLocal<ReadLatestRowVersion> readLatestRowVersionCache = ThreadLocal.withInitial(ReadLatestRowVersion::new);
    private final ThreadLocal<ScanVersionChainByTimestamp> scanVersionChainByTimestampCache = ThreadLocal.withInitial(
            ScanVersionChainByTimestamp::new
    );

    /**
     * Constructor.
     */
    public PageMemoryMvPartitionStorage(
            int partitionId,
            TableView tableConfig,
            PageMemoryDataRegion dataRegion,
            VersionChainFreeList versionChainFreeList,
            RowVersionFreeList rowVersionFreeList
    ) {
        this.partitionId = partitionId;

        this.versionChainFreeList = versionChainFreeList;
        this.rowVersionFreeList = rowVersionFreeList;

        groupId = StorageUtils.groupId(tableConfig);

        try {
            versionChainTree = createVersionChainTree(partitionId, tableConfig, dataRegion, versionChainFreeList);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error occurred while creating the partition storage", e);
        }

        versionChainDataPageReader = new VersionChainDataPageReader(dataRegion.pageMemory(), groupId, IoStatisticsHolderNoOp.INSTANCE);
        rowVersionDataPageReader = new DataPageReader(dataRegion.pageMemory(), groupId, IoStatisticsHolderNoOp.INSTANCE);
    }

    private VersionChainTree createVersionChainTree(
            int partitionId,
            TableView tableConfig,
            PageMemoryDataRegion dataRegion,
            VersionChainFreeList versionChainFreeList1
    ) throws IgniteInternalCheckedException {
        // TODO: IGNITE-16641 It is necessary to do getting the tree root for the persistent case.
        long metaPageId = dataRegion.pageMemory().allocatePage(groupId, partitionId, FLAG_AUX);

        // TODO: IGNITE-16641 It is necessary to take into account the persistent case.
        boolean initNew = true;

        return new VersionChainTree(
                groupId,
                tableConfig.name(),
                dataRegion.pageMemory(),
                PageLockListenerNoOp.INSTANCE,
                new AtomicLong(),
                metaPageId,
                versionChainFreeList1,
                partitionId,
                initNew
        );
    }

    @Override
    public @Nullable BinaryRow read(RowId rowId, UUID txId) throws TxIdMismatchException, StorageException {
        VersionChain versionChain = findVersionChain(rowId);
        if (versionChain == null) {
            return null;
        }

        return findLatestRowVersion(versionChain, txId, MATCH_ALL);
    }

    @Override
    public @Nullable BinaryRow read(RowId rowId, Timestamp timestamp) throws StorageException {
        VersionChain versionChain = findVersionChain(rowId);
        if (versionChain == null) {
            return null;
        }

        return findRowVersionByTimestamp(versionChain, timestamp);
    }

    @Nullable
    private VersionChain findVersionChain(RowId rowId) {
        try {
            return versionChainDataPageReader.getRowByLink(versionChainLinkFrom(rowId));
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Version chain lookup failed", e);
        }
    }

    private long versionChainLinkFrom(RowId rowId) {
        if (rowId.partitionId() != partitionId) {
            throw new IllegalArgumentException("I own partition " + partitionId + " but I was given RowId with partition "
                    + rowId.partitionId());
        }

        LinkRowId linkRowId = (LinkRowId) rowId;

        return linkRowId.versionChainLink();
    }

    @Nullable
    private ByteBufferRow findLatestRowVersion(VersionChain versionChain, UUID txId, Predicate<BinaryRow> keyFilter) {
        RowVersion rowVersion = findLatestRowVersion(versionChain, ALWAYS_LOAD_VALUE);
        ByteBufferRow row = rowVersionToBinaryRow(rowVersion);

        if (!keyFilter.test(row)) {
            return null;
        }

        throwIfChainBelongsToAnotherTx(versionChain, txId);

        return row;
    }

    private RowVersion findLatestRowVersion(VersionChain versionChain, Predicate<Timestamp> loadValue) {
        long nextLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(versionChain.headLink(), partitionId);

        ReadLatestRowVersion read = freshReadLatestRowVersion();

        try {
            rowVersionDataPageReader.traverse(nextLink, read, loadValue);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Row version lookup failed");
        }

        return read.result();
    }

    private ReadLatestRowVersion freshReadLatestRowVersion() {
        ReadLatestRowVersion traversal = readLatestRowVersionCache.get();
        traversal.reset();
        return traversal;
    }

    private void throwIfChainBelongsToAnotherTx(VersionChain versionChain, UUID txId) {
        if (versionChain.transactionId() != null && !txId.equals(versionChain.transactionId())) {
            throw new TxIdMismatchException();
        }
    }

    @Nullable
    private ByteBufferRow rowVersionToBinaryRow(RowVersion rowVersion) {
        if (rowVersion.isTombstone()) {
            return null;
        }

        return new ByteBufferRow(rowVersion.value());
    }

    @Nullable
    private ByteBufferRow findRowVersionInChain(
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

    @Nullable
    private ByteBufferRow findRowVersionByTimestamp(VersionChain versionChain, Timestamp timestamp) {
        long nextRowPartitionlessLink = versionChain.headLink();
        long nextLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(nextRowPartitionlessLink, partitionId);

        ScanVersionChainByTimestamp scanByTimestamp = freshScanByTimestamp();

        try {
            rowVersionDataPageReader.traverse(nextLink, scanByTimestamp, timestamp);
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

    @Override
    public LinkRowId insert(BinaryRow row, UUID txId) throws StorageException {
        RowVersion rowVersion = insertRowVersion(Objects.requireNonNull(row), RowVersion.NULL_LINK);

        VersionChain versionChain = new VersionChain(partitionId, txId, PartitionlessLinks.removePartitionIdFromLink(rowVersion.link()));

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
                partitionId,
                txId,
                PartitionlessLinks.removePartitionIdFromLink(newVersion.link())
        );

        updateVersionChain(currentChain, chainReplacement);

        if (currentVersion.isUncommitted()) {
            return rowVersionToBinaryRow(currentVersion);
        } else {
            return null;
        }
    }

    @NotNull
    private VersionChain findVersionChainForModification(RowId rowId) {
        VersionChain currentChain = findVersionChain(rowId);
        if (currentChain == null) {
            throw new RowIdIsInvalidForModificationsException();
        }
        return currentChain;
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        VersionChain currentVersionChain = findVersionChainForModification(rowId);

        if (currentVersionChain.transactionId() == null) {
            throw new NoUncommittedVersionException();
        }

        RowVersion currentVersion = findLatestRowVersion(currentVersionChain, ALWAYS_LOAD_VALUE);
        assert currentVersion.isUncommitted();

        removeRowVersion(currentVersion);

        if (currentVersion.hasNextLink()) {
            VersionChain versionChainReplacement = VersionChain.withoutTxId(
                    partitionId,
                    currentVersionChain.link(),
                    currentVersion.nextLink()
            );
            updateVersionChain(currentVersionChain, versionChainReplacement);
        } else {
            // it was the only version, let's remove the chain as well
            removeVersionChain(currentVersionChain);
        }

        return rowVersionToBinaryRow(currentVersion);
    }

    private void removeVersionChain(VersionChain currentVersionChain) {
        try {
            versionChainFreeList.removeDataRowByLink(currentVersionChain.link());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot remove chain version", e);
        }
    }

    @Override
    public void commitWrite(RowId rowId, Timestamp timestamp) throws StorageException {
        VersionChain currentVersionChain = findVersionChainForModification(rowId);
        long chainLink = PartitionlessLinks.addPartitionIdToPartititionlessLink(currentVersionChain.headLink(), partitionId);

        assert currentVersionChain.transactionId() != null;

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

    @Override
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, UUID txId) throws TxIdMismatchException, StorageException {
        return internalScan(keyFilter, txId, null);
    }

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

    @Override
    public void close() {
        versionChainFreeList.close();
        versionChainTree.close();
        rowVersionFreeList.close();
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

        @Override
        public void close() {
            // no-op
        }
    }
}
