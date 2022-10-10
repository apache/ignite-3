/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.HashIndexView;
import org.apache.ignite.configuration.schemas.table.SortedIndexView;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.index.BinaryTupleComparator;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.AbstractPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexTree;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexTree;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.CursorUtils;
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

    private final TablesConfiguration tablesConfiguration;

    protected final DataPageReader rowVersionDataPageReader;

    protected final ConcurrentMap<UUID, PageMemoryHashIndexStorage> hashIndexes = new ConcurrentHashMap<>();

    protected final ConcurrentMap<UUID, PageMemorySortedIndexStorage> sortedIndexes = new ConcurrentHashMap<>();

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
            IndexMetaTree indexMetaTree,
            TablesConfiguration tablesCfg
    ) {
        this.partitionId = partitionId;
        this.tableStorage = tableStorage;

        this.rowVersionFreeList = rowVersionFreeList;
        this.indexFreeList = indexFreeList;

        this.versionChainTree = versionChainTree;
        this.indexMetaTree = indexMetaTree;

        tablesConfiguration = tablesCfg;

        PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

        groupId = tableStorage.configuration().value().tableId();

        rowVersionDataPageReader = new DataPageReader(pageMemory, groupId, IoStatisticsHolderNoOp.INSTANCE);
    }

    /**
     * Starts a partition by initializing its internal structures.
     */
    public void start() {
        try {
            Cursor<IndexMeta> cursor = indexMetaTree.find(null, null);

            NamedListView<TableIndexView> indexesCfgView = tablesConfiguration.indexes().value();

            while (cursor.hasNext()) {
                IndexMeta indexMeta = cursor.next();

                TableIndexView indexCfgView = getByInternalId(indexesCfgView, indexMeta.id());

                if (indexCfgView instanceof HashIndexView) {
                    createOrRestoreHashIndex(indexMeta);
                } else if (indexCfgView instanceof SortedIndexView) {
                    createOrRestoreSortedIndex(indexMeta);
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
    public PageMemoryHashIndexStorage getOrCreateHashIndex(UUID indexId) {
        return hashIndexes.computeIfAbsent(indexId, uuid -> createOrRestoreHashIndex(new IndexMeta(indexId, 0L)));
    }

    /**
     * Returns a sorted index instance, creating index it if necessary.
     *
     * @param indexId Index UUID.
     */
    public PageMemorySortedIndexStorage getOrCreateSortedIndex(UUID indexId) {
        return sortedIndexes.computeIfAbsent(indexId, uuid -> createOrRestoreSortedIndex(new IndexMeta(indexId, 0L)));
    }

    private PageMemoryHashIndexStorage createOrRestoreHashIndex(IndexMeta indexMeta) {
        var indexDescriptor = new HashIndexDescriptor(indexMeta.id(), tablesConfiguration.value());

        try {
            PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

            boolean initNew = indexMeta.metaPageId() == 0L;

            long metaPageId = initNew
                    ? pageMemory.allocatePage(groupId, partitionId, PageIdAllocator.FLAG_AUX)
                    : indexMeta.metaPageId();

            String tableName = tableStorage.configuration().value().name();

            HashIndexTree hashIndexTree = new HashIndexTree(
                    groupId,
                    tableName,
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

    private PageMemorySortedIndexStorage createOrRestoreSortedIndex(IndexMeta indexMeta) {
        var indexDescriptor = new SortedIndexDescriptor(indexMeta.id(), tablesConfiguration.value());

        try {
            PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

            boolean initNew = indexMeta.metaPageId() == 0L;

            long metaPageId = initNew
                    ? pageMemory.allocatePage(groupId, partitionId, PageIdAllocator.FLAG_AUX)
                    : indexMeta.metaPageId();

            String tableName = tableStorage.configuration().value().name();

            SortedIndexTree sortedIndexTree = new SortedIndexTree(
                    groupId,
                    tableName,
                    partitionId,
                    pageMemory,
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    metaPageId,
                    rowVersionFreeList,
                    initNew,
                    new BinaryTupleComparator(indexDescriptor)
            );

            if (initNew) {
                boolean replaced = indexMetaTree.putx(new IndexMeta(indexMeta.id(), metaPageId));

                assert !replaced;
            }

            return new PageMemorySortedIndexStorage(indexDescriptor, indexFreeList, sortedIndexTree);
        } catch (IgniteInternalCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads either the committed value from the storage or the uncommitted value belonging to given transaction.
     *
     * @param rowId Row id.
     * @param txId Transaction id.
     * @return Read result that corresponds to the key or {@code null} if value is not found.
     * @throws TxIdMismatchException If there's another pending update associated with different transaction id.
     * @throws StorageException If failed to read data from the storage.
     */
    // TODO: IGNITE-17864 Optimize scan(HybridTimestamp.MAX_VALUE) and read(HybridTimestamp.MAX_VALUE)
    @Deprecated
    public @Nullable BinaryRow read(RowId rowId, UUID txId) throws TxIdMismatchException, StorageException {
        if (rowId.partitionId() != partitionId) {
            throw new IllegalArgumentException(
                    String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
        }

        VersionChain versionChain = findVersionChain(rowId);

        if (versionChain == null) {
            return null;
        }

        return findLatestRowVersion(versionChain, txId, MATCH_ALL);
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        if (rowId.partitionId() != partitionId) {
            throw new IllegalArgumentException(
                    String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
        }

        VersionChain versionChain = findVersionChain(rowId);

        if (versionChain == null) {
            return ReadResult.EMPTY;
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

    private @Nullable BinaryRow findLatestRowVersion(VersionChain versionChain, UUID txId, Predicate<BinaryRow> keyFilter) {
        RowVersion rowVersion = readRowVersion(versionChain.headLink(), ALWAYS_LOAD_VALUE);

        ByteBufferRow row = rowVersionToBinaryRow(rowVersion);

        if (keyFilter != null && !keyFilter.test(row)) {
            return null;
        }

        if (versionChain.isUncommitted()) {
            UUID chainTxId = versionChain.transactionId();

            assert chainTxId != null;

            throwIfChainBelongsToAnotherTx(versionChain, txId);

            return row;
        }

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
        assert versionChain.isUncommitted();

        if (!txId.equals(versionChain.transactionId())) {
            throw new TxIdMismatchException(txId, versionChain.transactionId());
        }
    }

    private @Nullable ByteBufferRow rowVersionToBinaryRow(RowVersion rowVersion) {
        if (rowVersion.isTombstone()) {
            return null;
        }

        return new ByteBufferRow(rowVersion.value());
    }

    /**
     * Finds row version by timestamp. See {@link MvPartitionStorage#read(RowId, HybridTimestamp)} for details on the API.
     *
     * @param versionChain Version chain.
     * @param timestamp Timestamp.
     * @return Read result.
     */
    private ReadResult findRowVersionByTimestamp(VersionChain versionChain, HybridTimestamp timestamp) {
        assert timestamp != null;

        long headLink = versionChain.headLink();

        if (versionChain.isUncommitted()) {
            // We have a write-intent.
            if (!versionChain.hasCommittedVersions()) {
                // We *only* have a write-intent, return it.
                RowVersion rowVersion = readRowVersion(headLink, ALWAYS_LOAD_VALUE);

                assert rowVersion.isUncommitted();

                UUID transactionId = versionChain.transactionId();
                UUID commitTableId = versionChain.commitTableId();
                int commitPartitionId = versionChain.commitPartitionId();

                BinaryRow row;

                if (rowVersion.isTombstone()) {
                    row = null;
                } else {
                    row = new ByteBufferRow(rowVersion.value());
                }

                return ReadResult.createFromWriteIntent(row, transactionId, commitTableId, null, commitPartitionId);
            }
        }

        return walkVersionChain(versionChain, timestamp);
    }

    /**
     * Walks version chain to find a row by timestamp. See {@link MvPartitionStorage#read(RowId, HybridTimestamp)} for details.
     *
     * @param chainHead Version chain head.
     * @param timestamp Timestamp.
     * @return Read result.
     */
    private ReadResult walkVersionChain(VersionChain chainHead, HybridTimestamp timestamp) {
        assert chainHead.hasCommittedVersions();

        boolean hasWriteIntent = chainHead.isUncommitted();

        RowVersion firstCommit;

        if (hasWriteIntent) {
            // First commit can only match if its timestamp matches query timestamp.
            firstCommit = readRowVersion(chainHead.nextLink(), rowTimestamp -> timestamp.compareTo(rowTimestamp) == 0);
        } else {
            firstCommit = readRowVersion(chainHead.headLink(), rowTimestamp -> timestamp.compareTo(rowTimestamp) >= 0);
        }

        assert firstCommit.isCommitted();
        assert firstCommit.timestamp() != null;

        if (hasWriteIntent && timestamp.compareTo(firstCommit.timestamp()) > 0) {
            // It's the latest commit in chain, query ts is greater than commit ts and there is a write-intent.
            // So we just return write-intent.
            RowVersion rowVersion = readRowVersion(chainHead.headLink(), ALWAYS_LOAD_VALUE);

            assert rowVersion.isUncommitted();

            UUID transactionId = chainHead.transactionId();
            UUID commitTableId = chainHead.commitTableId();
            int commitPartitionId = chainHead.commitPartitionId();

            BinaryRow row;

            if (rowVersion.isTombstone()) {
                row = null;
            } else {
                row = new ByteBufferRow(rowVersion.value());
            }

            return ReadResult.createFromWriteIntent(row, transactionId, commitTableId, firstCommit.timestamp(), commitPartitionId);
        }

        RowVersion curCommit = firstCommit;

        do {
            assert curCommit.timestamp() != null;

            int compareResult = timestamp.compareTo(curCommit.timestamp());

            if (compareResult >= 0) {
                // This commit has timestamp matching the query ts, meaning that commit is the one we are looking for.
                BinaryRow row;

                if (curCommit.isTombstone()) {
                    row = null;
                } else {
                    row = new ByteBufferRow(curCommit.value());
                }

                return ReadResult.createFromCommitted(row);
            }

            if (!curCommit.hasNextLink()) {
                curCommit = null;
            } else {
                curCommit = readRowVersion(curCommit.nextLink(), rowTimestamp -> timestamp.compareTo(rowTimestamp) >= 0);
            }
        } while (curCommit != null);

        return ReadResult.EMPTY;
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
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        VersionChain currentChain = findVersionChain(rowId);

        if (currentChain == null) {
            RowVersion newVersion = insertRowVersion(row, NULL_LINK);

            VersionChain versionChain = VersionChain.createUncommitted(rowId, txId, commitTableId, commitPartitionId, newVersion.link(),
                    NULL_LINK);

            updateVersionChain(versionChain);

            return null;
        }

        if (currentChain.isUncommitted()) {
            throwIfChainBelongsToAnotherTx(currentChain, txId);
        }

        RowVersion newVersion = insertRowVersion(row, currentChain.newestCommittedLink());

        BinaryRow res = null;

        if (currentChain.isUncommitted()) {
            RowVersion currentVersion = readRowVersion(currentChain.headLink(), ALWAYS_LOAD_VALUE);

            res = rowVersionToBinaryRow(currentVersion);

            // as we replace an uncommitted version with new one, we need to remove old uncommitted version
            removeRowVersion(currentVersion);
        }

        VersionChain chainReplacement = VersionChain.createUncommitted(rowId, txId, commitTableId, commitPartitionId, newVersion.link(),
                newVersion.nextLink());

        updateVersionChain(chainReplacement);

        return res;
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

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
            VersionChain versionChainReplacement = VersionChain.createCommitted(rowId, latestVersion.nextLink(), NULL_LINK);

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

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

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
            VersionChain updatedVersionChain = VersionChain.createCommitted(
                    currentVersionChain.rowId(),
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

    @Override
    public Cursor<BinaryRow> scanVersions(RowId rowId) throws StorageException {
        try {
            VersionChain versionChain = versionChainTree.findOne(new VersionChainKey(rowId));

            if (versionChain == null) {
                return CursorUtils.emptyCursor();
            }

            RowVersion head = readRowVersion(versionChain.headLink(), ALWAYS_LOAD_VALUE);

            Stream<RowVersion> stream = Stream.iterate(head, Objects::nonNull, rowVersion ->
                    rowVersion.nextLink() == 0 ? null : readRowVersion(rowVersion.nextLink(), ALWAYS_LOAD_VALUE)
            );

            return Cursor.fromIterator(stream.map(rowVersion -> new ByteBufferRow(rowVersion.value())).iterator());
        } catch (IgniteInternalCheckedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Scans the partition and returns a cursor of values. All filtered values must either be uncommitted in the current transaction
     * or already committed in a different transaction.
     *
     * @param keyFilter Key filter. Binary rows passed to the filter may or may not have a value, filter should only check keys.
     * @param txId Transaction id.
     * @return Cursor.
     * @throws StorageException If failed to read data from the storage.
     */
    // TODO: IGNITE-17864 Optimize scan(HybridTimestamp.MAX_VALUE) and read(HybridTimestamp.MAX_VALUE)
    @Deprecated
    public Cursor<BinaryRow> scan(Predicate<BinaryRow> keyFilter, UUID txId) throws TxIdMismatchException, StorageException {
        return internalScan(keyFilter, txId);
    }

    @Override
    public PartitionTimestampCursor scan(Predicate<BinaryRow> keyFilter, HybridTimestamp timestamp) throws StorageException {
        assert timestamp != null;

        Cursor<VersionChain> treeCursor;

        try {
            treeCursor = versionChainTree.find(null, null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Find failed", e);
        }

        return new TimestampCursor(treeCursor, keyFilter, timestamp);
    }

    private Cursor<BinaryRow> internalScan(Predicate<BinaryRow> keyFilter, UUID txId) {
        assert txId != null;

        Cursor<VersionChain> treeCursor;

        try {
            treeCursor = versionChainTree.find(null, null);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Find failed", e);
        }

        return new TransactionIdCursor(treeCursor, keyFilter, txId);
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        try (Cursor<VersionChain> cursor = versionChainTree.find(new VersionChainKey(lowerBound), null)) {
            return cursor.hasNext() ? cursor.next().rowId() : null;
        } catch (Exception e) {
            throw new StorageException("Error occurred while trying to read a row id", e);
        }
    }

    @Override
    public long rowsCount() {
        try {
            return versionChainTree.size();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error occurred while fetching the size.", e);
        }
    }

    @Override
    public void forEach(BiConsumer<RowId, BinaryRow> consumer) {
        // No-op. Nothing to recover for a volatile storage. See usages and a comment about PK index rebuild.
    }

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

    /**
     * Implementation of the {@link PartitionTimestampCursor} over the page memory storage.
     * See {@link PartitionTimestampCursor} for the details on the API.
     */
    private class TimestampCursor implements PartitionTimestampCursor {
        private final Cursor<VersionChain> treeCursor;

        private final Predicate<BinaryRow> keyFilter;

        private final HybridTimestamp timestamp;

        @Nullable
        private ReadResult nextRead = null;

        @Nullable
        private VersionChain currentChain = null;

        private boolean iterationExhausted = false;

        public TimestampCursor(Cursor<VersionChain> treeCursor, Predicate<BinaryRow> keyFilter, HybridTimestamp timestamp) {
            this.treeCursor = treeCursor;
            this.keyFilter = keyFilter;
            this.timestamp = timestamp;
        }

        @Override
        public boolean hasNext() {
            if (nextRead != null) {
                return true;
            }

            if (iterationExhausted) {
                return false;
            }

            currentChain = null;

            while (true) {
                if (!treeCursor.hasNext()) {
                    iterationExhausted = true;

                    return false;
                }

                VersionChain chain = treeCursor.next();
                ReadResult res = findRowVersionByTimestamp(chain, timestamp);

                if (res.isEmpty()) {
                    continue;
                }

                if (keyFilter != null && !keyFilter.test(res.binaryRow())) {
                    continue;
                }

                nextRead = res;
                currentChain = chain;

                return true;
            }
        }

        @Override
        public ReadResult next() {
            if (!hasNext()) {
                throw new NoSuchElementException("The cursor is exhausted");
            }

            assert nextRead != null;

            ReadResult res = nextRead;

            nextRead = null;

            return res;
        }

        @Override
        public void close() {
            // No-op.
        }

        @Override
        public @Nullable BinaryRow committed(HybridTimestamp timestamp) {
            if (currentChain == null) {
                throw new IllegalStateException();
            }

            ReadResult res = findRowVersionByTimestamp(currentChain, timestamp);

            // We don't check if row conforms the key filter here, because we've already checked it.
            return res.binaryRow();
        }
    }

    /**
     * Implementation of the cursor that iterates over the page memory storage with the respect to the transaction id.
     * Scans the partition and returns a cursor of values. All filtered values must either be uncommitted in the current transaction
     * or already committed in a different transaction.
     */
    private class TransactionIdCursor implements Cursor<BinaryRow> {
        private final Cursor<VersionChain> treeCursor;

        private final Predicate<BinaryRow> keyFilter;

        private final @Nullable UUID transactionId;

        private BinaryRow nextRow = null;

        private boolean iterationExhausted = false;

        public TransactionIdCursor(
                Cursor<VersionChain> treeCursor,
                Predicate<BinaryRow> keyFilter,
                @Nullable UUID transactionId
        ) {
            this.treeCursor = treeCursor;
            this.keyFilter = keyFilter;
            this.transactionId = transactionId;
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
                if (!treeCursor.hasNext()) {
                    iterationExhausted = true;
                    return false;
                }

                VersionChain chain = treeCursor.next();
                BinaryRow row = findLatestRowVersion(chain, transactionId, keyFilter);

                if (row != null) {
                    nextRow = row;
                    return true;
                }
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
            // No-op.
        }
    }
}
