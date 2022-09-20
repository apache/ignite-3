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

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.HashIndexView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.AbstractPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexTree;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
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

    private static final Predicate<HybridTimestamp> ALWAYS_LOAD_VALUE = timestamp -> true;

    protected final int partitionId;

    protected final int groupId;

    protected final AbstractPageMemoryTableStorage tableStorage;

    protected final VersionChainTree versionChainTree;

    protected final RowVersionFreeList rowVersionFreeList;

    protected final IndexColumnsFreeList indexFreeList;

    protected final IndexMetaTree indexMetaTree;

    protected final DataPageReader rowVersionDataPageReader;

    protected final ConcurrentMap<UUID, HashIndexStorage> indexes = new ConcurrentHashMap<>();

    /**
     * Constructor.
     *
     * @param partitionId Partition id.
     * @param tableStorage Table storage instance.
     * @param rowVersionFreeList Free list for {@link RowVersion}.
     * @param indexFreeList Free list fot {@link IndexColumns}.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     */
    protected AbstractPageMemoryMvPartitionStorage(
            int partitionId,
            AbstractPageMemoryTableStorage tableStorage,
            RowVersionFreeList rowVersionFreeList,
            IndexColumnsFreeList indexFreeList,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree
    ) {
        this.partitionId = partitionId;
        this.tableStorage = tableStorage;

        this.rowVersionFreeList = rowVersionFreeList;
        this.indexFreeList = indexFreeList;

        this.versionChainTree = versionChainTree;
        this.indexMetaTree = indexMetaTree;

        PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

        groupId = tableStorage.configuration().value().tableId();

        rowVersionDataPageReader = new DataPageReader(pageMemory, groupId, IoStatisticsHolderNoOp.INSTANCE);
    }

    /**
     * Starts a partition by initializing its internal structures.
     */
    public void start() {
        try {
            IgniteCursor<IndexMeta> cursor = indexMetaTree.find(null, null);

            NamedListView<? extends TableIndexView> indicesCfgView = tableStorage.configuration().value().indices();

            while (cursor.next()) {
                IndexMeta indexMeta = cursor.get();

                TableIndexView indexCfgView = getByInternalId(indicesCfgView, indexMeta.id());

                if (indexCfgView instanceof HashIndexView) {
                    createOrRestoreHashIndex(indexMeta);
                } else if (indexCfgView instanceof SortedIndexView) {
                    throw new UnsupportedOperationException("Not implemented yet");
                } else {
                    assert indexCfgView == null;

                    //TODO IGNITE-17626 Drop the index synchronously.
                }
            }
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to process SQL indexes during the partition start", e);
        }
    }

    /**
     * Returns a hash index instance, creating index it if necessary.
     *
     * @param indexId Index UUID.
     */
    public HashIndexStorage getOrCreateHashIndex(UUID indexId) {
        return indexes.computeIfAbsent(indexId, uuid -> createOrRestoreHashIndex(new IndexMeta(indexId, 0L)));
    }

    private PageMemoryHashIndexStorage createOrRestoreHashIndex(IndexMeta indexMeta) {
        TableView tableView = tableStorage.configuration().value();

        var indexDescriptor = new HashIndexDescriptor(indexMeta.id(), tableView);

        try {
            PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

            boolean initNew = indexMeta.metaPageId() == 0L;

            long metaPageId = initNew
                    ? pageMemory.allocatePage(groupId, partitionId, PageIdAllocator.FLAG_AUX)
                    : indexMeta.metaPageId();

            HashIndexTree hashIndexTree = new HashIndexTree(
                    groupId,
                    tableView.name(),
                    partitionId,
                    pageMemory,
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    metaPageId,
                    rowVersionFreeList,
                    initNew
            );

            if (initNew) {
                boolean replaced = indexMetaTree.putx(new IndexMeta(indexMeta.id(), metaPageId));

                assert !replaced;
            }

            return new PageMemoryHashIndexStorage(indexDescriptor, indexFreeList, hashIndexTree);
        } catch (IgniteInternalCheckedException e) {
            throw new RuntimeException(e);
        }
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
    public @Nullable BinaryRow read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
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

    private RowVersion readRowVersion(long nextLink, Predicate<HybridTimestamp> loadValue) {
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
            @Nullable HybridTimestamp timestamp,
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

    private @Nullable ByteBufferRow findRowVersionByTimestamp(VersionChain versionChain, HybridTimestamp timestamp) {
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
            RowVersion newVersion = insertRowVersion(row, NULL_LINK);

            VersionChain versionChain = new VersionChain(rowId, txId, newVersion.link(), NULL_LINK);

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
            VersionChain versionChainReplacement = new VersionChain(rowId, null, latestVersion.nextLink(), NULL_LINK);

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
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
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
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, HybridTimestamp timestamp) throws StorageException {
        return internalScan(keyFilter, null, timestamp);
    }

    private Cursor<BinaryRow> internalScan(Predicate<BinaryRow> keyFilter, @Nullable UUID txId, @Nullable HybridTimestamp timestamp) {
        assert txId != null ^ timestamp != null;

        IgniteCursor<VersionChain> treeCursor;

        try {
            treeCursor = versionChainTree.find(null, null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Find failed", e);
        }

        return new ScanCursor(treeCursor, keyFilter, txId, timestamp);
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

        indexMetaTree.close();
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

        private final @Nullable HybridTimestamp timestamp;

        private BinaryRow nextRow = null;

        private boolean iterationExhausted = false;

        public ScanCursor(
                IgniteCursor<VersionChain> treeCursor,
                Predicate<BinaryRow> keyFilter,
                @Nullable UUID transactionId,
                @Nullable HybridTimestamp timestamp
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
