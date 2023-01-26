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
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInRunnableOrRebalancedState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInRunnableState;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.schema.TableRow;
import org.apache.ignite.internal.schema.configuration.index.HashIndexView;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
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
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract implementation of partition storage using Page Memory.
 */
public abstract class AbstractPageMemoryMvPartitionStorage implements MvPartitionStorage {
    private static final byte[] TOMBSTONE_PAYLOAD = new byte[0];

    private static final Predicate<HybridTimestamp> ALWAYS_LOAD_VALUE = timestamp -> true;

    protected final int partitionId;

    protected final int groupId;

    protected final AbstractPageMemoryTableStorage tableStorage;

    protected volatile VersionChainTree versionChainTree;

    protected volatile RowVersionFreeList rowVersionFreeList;

    protected volatile IndexColumnsFreeList indexFreeList;

    protected volatile IndexMetaTree indexMetaTree;

    protected final DataPageReader rowVersionDataPageReader;

    protected final ConcurrentMap<UUID, PageMemoryHashIndexStorage> hashIndexes = new ConcurrentHashMap<>();

    protected final ConcurrentMap<UUID, PageMemorySortedIndexStorage> sortedIndexes = new ConcurrentHashMap<>();

    /** Busy lock. */
    protected final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    protected final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /**
     * Constructor.
     *
     * @param partitionId Partition ID.
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
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

            try (Cursor<IndexMeta> cursor = indexMetaTree.find(null, null)) {
                NamedListView<TableIndexView> indexesCfgView = tableStorage.tablesConfiguration().indexes().value();

                for (IndexMeta indexMeta : cursor) {
                    TableIndexView indexCfgView = getByInternalId(indexesCfgView, indexMeta.id());

                    if (indexCfgView instanceof HashIndexView) {
                        hashIndexes.put(indexCfgView.id(), createOrRestoreHashIndex(indexMeta));
                    } else if (indexCfgView instanceof SortedIndexView) {
                        sortedIndexes.put(indexCfgView.id(), createOrRestoreSortedIndex(indexMeta));
                    } else {
                        assert indexCfgView == null;

                        //TODO: IGNITE-17626 Drop the index synchronously.
                    }
                }

                return null;
            } catch (Exception e) {
                throw new StorageException(
                        IgniteStringFormatter.format("Failed to process SQL indexes during partition start: [{}]", createStorageInfo()),
                        e
                );
            }
        });
    }

    /**
     * Returns the partition ID.
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * Returns a hash index instance, creating index it if necessary.
     *
     * @param indexId Index UUID.
     */
    public PageMemoryHashIndexStorage getOrCreateHashIndex(UUID indexId) {
        return busy(() -> hashIndexes.computeIfAbsent(indexId, uuid -> createOrRestoreHashIndex(new IndexMeta(indexId, 0L))));
    }

    /**
     * Returns a sorted index instance, creating index it if necessary.
     *
     * @param indexId Index UUID.
     */
    public PageMemorySortedIndexStorage getOrCreateSortedIndex(UUID indexId) {
        return busy(() -> sortedIndexes.computeIfAbsent(indexId, uuid -> createOrRestoreSortedIndex(new IndexMeta(indexId, 0L))));
    }

    private PageMemoryHashIndexStorage createOrRestoreHashIndex(IndexMeta indexMeta) {
        throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

        var indexDescriptor = new HashIndexDescriptor(indexMeta.id(), tableStorage.tablesConfiguration().value());

        HashIndexTree hashIndexTree = createHashIndexTree(indexDescriptor, indexMeta);

        return new PageMemoryHashIndexStorage(indexDescriptor, indexFreeList, hashIndexTree);
    }

    HashIndexTree createHashIndexTree(HashIndexDescriptor indexDescriptor, IndexMeta indexMeta) {
        try {
            PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

            boolean initNew = indexMeta.metaPageId() == 0L;

            long metaPageId = initNew
                    ? pageMemory.allocatePage(groupId, partitionId, PageIdAllocator.FLAG_AUX)
                    : indexMeta.metaPageId();

            HashIndexTree hashIndexTree = new HashIndexTree(
                    groupId,
                    tableStorage.getTableName(),
                    partitionId,
                    pageMemory,
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    metaPageId,
                    rowVersionFreeList,
                    indexDescriptor,
                    initNew
            );

            if (initNew) {
                boolean replaced = indexMetaTree.putx(new IndexMeta(indexMeta.id(), metaPageId));

                assert !replaced;
            }

            return hashIndexTree;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    IgniteStringFormatter.format(
                            "Error creating hash index tree: [{}, indexId={}]",
                            createStorageInfo(),
                            indexMeta.id()
                    ),
                    e
            );
        }
    }

    private PageMemorySortedIndexStorage createOrRestoreSortedIndex(IndexMeta indexMeta) {
        throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

        var indexDescriptor = new SortedIndexDescriptor(indexMeta.id(), tableStorage.tablesConfiguration().value());

        SortedIndexTree sortedIndexTree = createSortedIndexTree(indexDescriptor, indexMeta);

        return new PageMemorySortedIndexStorage(indexDescriptor, indexFreeList, sortedIndexTree);
    }

    SortedIndexTree createSortedIndexTree(SortedIndexDescriptor indexDescriptor, IndexMeta indexMeta) {
        try {
            PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

            boolean initNew = indexMeta.metaPageId() == 0L;

            long metaPageId = initNew
                    ? pageMemory.allocatePage(groupId, partitionId, PageIdAllocator.FLAG_AUX)
                    : indexMeta.metaPageId();

            SortedIndexTree sortedIndexTree = new SortedIndexTree(
                    groupId,
                    tableStorage.getTableName(),
                    partitionId,
                    pageMemory,
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    metaPageId,
                    rowVersionFreeList,
                    indexDescriptor,
                    initNew
            );

            if (initNew) {
                boolean replaced = indexMetaTree.putx(new IndexMeta(indexMeta.id(), metaPageId));

                assert !replaced;
            }

            return sortedIndexTree;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    IgniteStringFormatter.format(
                            "Error creating sorted index tree: [table={}, partitionId={}, indexId={}]",
                            tableStorage.getTableName(),
                            partitionId,
                            indexMeta.id()
                    ),
                    e
            );
        }
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

            if (rowId.partitionId() != partitionId) {
                throw new IllegalArgumentException(
                        String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
            }

            VersionChain versionChain = findVersionChain(rowId);

            if (versionChain == null) {
                return ReadResult.empty(rowId);
            }

            if (lookingForLatestVersion(timestamp)) {
                return findLatestRowVersion(versionChain);
            } else {
                return findRowVersionByTimestamp(versionChain, timestamp);
            }
        });
    }

    private boolean lookingForLatestVersion(HybridTimestamp timestamp) {
        return timestamp == HybridTimestamp.MAX_VALUE;
    }

    private @Nullable VersionChain findVersionChain(RowId rowId) {
        try {
            return versionChainTree.findOne(new VersionChainKey(rowId));
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Version chain lookup failed", e);
        }
    }

    private ReadResult findLatestRowVersion(VersionChain versionChain) {
        RowVersion rowVersion = readRowVersion(versionChain.headLink(), ALWAYS_LOAD_VALUE);

        if (versionChain.isUncommitted()) {
            assert versionChain.transactionId() != null;

            HybridTimestamp newestCommitTs = null;

            if (versionChain.hasCommittedVersions()) {
                long newestCommitLink = versionChain.newestCommittedLink();
                newestCommitTs = readRowVersion(newestCommitLink, ALWAYS_LOAD_VALUE).timestamp();
            }

            return writeIntentToResult(versionChain, rowVersion, newestCommitTs);
        } else {
            TableRow row = rowVersionToTableRow(rowVersion);

            return ReadResult.createFromCommitted(versionChain.rowId(), row, rowVersion.timestamp());
        }
    }

    RowVersion readRowVersion(long rowVersionLink, Predicate<HybridTimestamp> loadValue) {
        ReadRowVersion read = new ReadRowVersion(partitionId);

        try {
            rowVersionDataPageReader.traverse(rowVersionLink, read, loadValue);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Row version lookup failed", e);
        }

        return read.result();
    }

    private void throwIfChainBelongsToAnotherTx(VersionChain versionChain, UUID txId) {
        assert versionChain.isUncommitted();

        if (!txId.equals(versionChain.transactionId())) {
            throw new TxIdMismatchException(txId, versionChain.transactionId());
        }
    }

    private @Nullable TableRow rowVersionToTableRow(RowVersion rowVersion) {
        if (rowVersion.isTombstone()) {
            return null;
        }

        return new TableRow(rowVersion.value());
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

                return writeIntentToResult(versionChain, rowVersion, null);
            }
        }

        return walkVersionChain(versionChain, timestamp);
    }

    /**
     * Walks version chain to find a row by timestamp. See {@link MvPartitionStorage#read(RowId, HybridTimestamp)} for details.
     *
     * @param chain Version chain head.
     * @param timestamp Timestamp.
     * @return Read result.
     */
    private ReadResult walkVersionChain(VersionChain chain, HybridTimestamp timestamp) {
        assert chain.hasCommittedVersions();

        boolean hasWriteIntent = chain.isUncommitted();

        RowVersion firstCommit;

        if (hasWriteIntent) {
            // First commit can only match if its timestamp matches query timestamp.
            firstCommit = readRowVersion(chain.nextLink(), rowTimestamp -> timestamp.compareTo(rowTimestamp) == 0);
        } else {
            firstCommit = readRowVersion(chain.headLink(), rowTimestamp -> timestamp.compareTo(rowTimestamp) >= 0);
        }

        assert firstCommit.isCommitted();
        assert firstCommit.timestamp() != null;

        if (hasWriteIntent && timestamp.compareTo(firstCommit.timestamp()) > 0) {
            // It's the latest commit in chain, query ts is greater than commit ts and there is a write-intent.
            // So we just return write-intent.
            RowVersion rowVersion = readRowVersion(chain.headLink(), ALWAYS_LOAD_VALUE);

            return writeIntentToResult(chain, rowVersion, firstCommit.timestamp());
        }

        RowVersion curCommit = firstCommit;

        do {
            assert curCommit.timestamp() != null;

            int compareResult = timestamp.compareTo(curCommit.timestamp());

            if (compareResult >= 0) {
                // This commit has timestamp matching the query ts, meaning that commit is the one we are looking for.
                TableRow row;

                if (curCommit.isTombstone()) {
                    row = null;
                } else {
                    row = new TableRow(curCommit.value());
                }

                return ReadResult.createFromCommitted(chain.rowId(), row, curCommit.timestamp());
            }

            if (!curCommit.hasNextLink()) {
                curCommit = null;
            } else {
                curCommit = readRowVersion(curCommit.nextLink(), rowTimestamp -> timestamp.compareTo(rowTimestamp) >= 0);
            }
        } while (curCommit != null);

        return ReadResult.empty(chain.rowId());
    }

    private ReadResult writeIntentToResult(VersionChain chain, RowVersion rowVersion, @Nullable HybridTimestamp lastCommittedTimestamp) {
        assert rowVersion.isUncommitted();

        UUID transactionId = chain.transactionId();
        UUID commitTableId = chain.commitTableId();
        int commitPartitionId = chain.commitPartitionId();

        TableRow row = rowVersionToTableRow(rowVersion);

        return ReadResult.createFromWriteIntent(
                chain.rowId(),
                row,
                transactionId,
                commitTableId,
                commitPartitionId,
                lastCommittedTimestamp
        );
    }

    private RowVersion insertRowVersion(@Nullable TableRow row, long nextPartitionlessLink) {
        byte[] rowBytes = rowBytes(row);

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

    private static byte[] rowBytes(@Nullable TableRow row) {
        // TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        return row == null ? TOMBSTONE_PAYLOAD : row.bytes();
    }

    @Override
    public @Nullable TableRow addWrite(RowId rowId, @Nullable TableRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalancedState(state.get(), this::createStorageInfo);

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

            TableRow res = null;

            if (currentChain.isUncommitted()) {
                RowVersion currentVersion = readRowVersion(currentChain.headLink(), ALWAYS_LOAD_VALUE);

                res = rowVersionToTableRow(currentVersion);

                // as we replace an uncommitted version with new one, we need to remove old uncommitted version
                removeRowVersion(currentVersion);
            }

            VersionChain chainReplacement = VersionChain.createUncommitted(rowId, txId, commitTableId, commitPartitionId, newVersion.link(),
                    newVersion.nextLink());

            updateVersionChain(chainReplacement);

            return res;
        });
    }

    @Override
    public @Nullable TableRow abortWrite(RowId rowId) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

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

            return rowVersionToTableRow(latestVersion);
        });
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

        busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalancedState(state.get(), this::createStorageInfo);

            VersionChain currentVersionChain = findVersionChain(rowId);

            if (currentVersionChain == null || currentVersionChain.transactionId() == null) {
                // Row doesn't exist or the chain doesn't contain an uncommitted write intent.
                return null;
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

            return null;
        });
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
            throw new StorageException("Cannot update version chain", e);
        }
    }

    @Override
    public void addWriteCommitted(RowId rowId, @Nullable TableRow row, HybridTimestamp commitTimestamp) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalancedState(state.get(), this::createStorageInfo);

            VersionChain currentChain = findVersionChain(rowId);

            if (currentChain != null && currentChain.isUncommitted()) {
                // This means that there is a bug in our code as the caller must make sure that no write intent exists
                // below this write.
                throw new StorageException("Write intent exists for " + rowId);
            }

            long nextLink = currentChain == null ? NULL_LINK : currentChain.newestCommittedLink();
            RowVersion newVersion = insertCommittedRowVersion(row, commitTimestamp, nextLink);

            VersionChain chainReplacement = VersionChain.createCommitted(rowId, newVersion.link(), newVersion.nextLink());

            updateVersionChain(chainReplacement);

            return null;
        });
    }

    private RowVersion insertCommittedRowVersion(@Nullable TableRow row, HybridTimestamp commitTimestamp, long nextPartitionlessLink) {
        byte[] rowBytes = rowBytes(row);

        RowVersion rowVersion = new RowVersion(partitionId, commitTimestamp, nextPartitionlessLink, ByteBuffer.wrap(rowBytes));

        insertRowVersion(rowVersion);

        return rowVersion;
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

            return new ScanVersionsCursor(rowId);
        });
    }

    private static ReadResult rowVersionToResultNotFillingLastCommittedTs(VersionChain versionChain, RowVersion rowVersion) {
        TableRow row = new TableRow(rowVersion.value());

        if (rowVersion.isCommitted()) {
            return ReadResult.createFromCommitted(versionChain.rowId(), row, rowVersion.timestamp());
        } else {
            return ReadResult.createFromWriteIntent(
                    versionChain.rowId(),
                    row,
                    versionChain.transactionId(),
                    versionChain.commitTableId(),
                    versionChain.commitPartitionId(),
                    null
            );
        }
    }

    @Override
    public PartitionTimestampCursor scan(HybridTimestamp timestamp) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

            Cursor<VersionChain> treeCursor;

            try {
                treeCursor = versionChainTree.find(null, null);
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Find failed", e);
            }

            if (lookingForLatestVersion(timestamp)) {
                return new LatestVersionsCursor(treeCursor);
            } else {
                return new TimestampCursor(treeCursor, timestamp);
            }
        });
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

            try (Cursor<VersionChain> cursor = versionChainTree.find(new VersionChainKey(lowerBound), null)) {
                return cursor.hasNext() ? cursor.next().rowId() : null;
            } catch (Exception e) {
                throw new StorageException("Error occurred while trying to read a row id", e);
            }
        });
    }

    @Override
    public long rowsCount() {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);

            try {
                return versionChainTree.size();
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error occurred while fetching the size.", e);
            }
        });
    }

    private abstract class BasePartitionTimestampCursor implements PartitionTimestampCursor {
        protected final Cursor<VersionChain> treeCursor;

        @Nullable
        protected ReadResult nextRead = null;

        @Nullable
        protected VersionChain currentChain = null;

        protected BasePartitionTimestampCursor(Cursor<VersionChain> treeCursor) {
            this.treeCursor = treeCursor;
        }

        @Override
        public final ReadResult next() {
            return busy(() -> {
                throwExceptionIfStorageNotInRunnableState(state.get(), AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

                if (!hasNext()) {
                    throw new NoSuchElementException("The cursor is exhausted");
                }

                assert nextRead != null;

                ReadResult res = nextRead;

                nextRead = null;

                return res;
            });
        }

        @Override
        public void close() {
            treeCursor.close();
        }

        @Override
        public @Nullable TableRow committed(HybridTimestamp timestamp) {
            return busy(() -> {
                throwExceptionIfStorageNotInRunnableState(state.get(), AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

                if (currentChain == null) {
                    throw new IllegalStateException();
                }

                ReadResult result = findRowVersionByTimestamp(currentChain, timestamp);

                if (result.isEmpty()) {
                    return null;
                }

                // We don't check if row conforms the key filter here, because we've already checked it.
                return result.tableRow();
            });
        }
    }

    /**
     * Implementation of the {@link PartitionTimestampCursor} over the page memory storage. See {@link PartitionTimestampCursor} for the
     * details on the API.
     */
    private class TimestampCursor extends BasePartitionTimestampCursor {
        private final HybridTimestamp timestamp;

        private boolean iterationExhausted = false;

        public TimestampCursor(Cursor<VersionChain> treeCursor, HybridTimestamp timestamp) {
            super(treeCursor);

            this.timestamp = timestamp;
        }

        @Override
        public boolean hasNext() {
            return busy(() -> {
                throwExceptionIfStorageNotInRunnableState(state.get(), AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

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
                    ReadResult result = findRowVersionByTimestamp(chain, timestamp);

                    if (result.isEmpty() && !result.isWriteIntent()) {
                        continue;
                    }

                    nextRead = result;
                    currentChain = chain;

                    return true;
                }
            });
        }
    }

    /**
     * Implementation of the cursor that iterates over the page memory storage with the respect to the transaction id. Scans the partition
     * and returns a cursor of values. All filtered values must either be uncommitted in the current transaction or already committed in a
     * different transaction.
     */
    private class LatestVersionsCursor extends BasePartitionTimestampCursor {
        private boolean iterationExhausted = false;

        public LatestVersionsCursor(Cursor<VersionChain> treeCursor) {
            super(treeCursor);
        }

        @Override
        public boolean hasNext() {
            return busy(() -> {
                throwExceptionIfStorageNotInRunnableState(state.get(), AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

                if (nextRead != null) {
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
                    ReadResult result = findLatestRowVersion(chain);

                    if (result.isEmpty() && !result.isWriteIntent()) {
                        continue;
                    }

                    nextRead = result;
                    currentChain = chain;

                    return true;
                }
            });
        }
    }

    private class ScanVersionsCursor implements Cursor<ReadResult> {
        final RowId rowId;

        @Nullable
        private Boolean hasNext;

        @Nullable
        private VersionChain versionChain;

        @Nullable
        private RowVersion rowVersion;

        private ScanVersionsCursor(RowId rowId) {
            this.rowId = rowId;
        }

        @Override
        public void close() {
            // No-op.
        }

        @Override
        public boolean hasNext() {
            return busy(() -> {
                advanceIfNeeded();

                return hasNext;
            });
        }

        @Override
        public ReadResult next() {
            return busy(() -> {
                advanceIfNeeded();

                if (!hasNext) {
                    throw new NoSuchElementException();
                }

                hasNext = null;

                return rowVersionToResultNotFillingLastCommittedTs(versionChain, rowVersion);
            });
        }

        private void advanceIfNeeded() {
            throwExceptionIfStorageNotInRunnableState(state.get(), AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

            if (hasNext != null) {
                return;
            }

            if (versionChain == null) {
                try {
                    versionChain = versionChainTree.findOne(new VersionChainKey(rowId));
                } catch (IgniteInternalCheckedException e) {
                    throw new StorageException(e);
                }

                rowVersion = versionChain == null ? null : readRowVersion(versionChain.headLink(), ALWAYS_LOAD_VALUE);
            } else {
                rowVersion = !rowVersion.hasNextLink() ? null : readRowVersion(rowVersion.nextLink(), ALWAYS_LOAD_VALUE);
            }

            hasNext = rowVersion != null;
        }
    }

    /**
     * Closes the partition in preparation for its destruction.
     */
    public void closeForDestruction() {
        close(true);
    }

    @Override
    public void close() {
        close(false);
    }

    /**
     * Closes the storage.
     *
     * @param goingToDestroy If the closure is in preparation for destruction.
     */
    private void close(boolean goingToDestroy) {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLOSED)) {
            StorageState state = this.state.get();

            assert state == StorageState.CLOSED : IgniteStringFormatter.format("{}, state={}", createStorageInfo(), state);

            return;
        }

        busyLock.block();

        try {
            IgniteUtils.closeAll(getResourcesToClose(goingToDestroy));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    /**
     * Returns resources that should be closed on {@link #close()}.
     *
     * @param goingToDestroy If the closure is in preparation for destruction.
     */
    protected List<AutoCloseable> getResourcesToClose(boolean goingToDestroy) {
        List<AutoCloseable> resources = new ArrayList<>();

        resources.add(versionChainTree::close);
        resources.add(indexMetaTree::close);

        hashIndexes.values().forEach(index -> resources.add(index::close));
        sortedIndexes.values().forEach(index -> resources.add(index::close));

        // We do not clear hashIndexes and sortedIndexes here because we leave the decision about when to clear them
        // to the subclasses.

        return resources;
    }

    /**
     * Performs a supplier using a {@link #busyLock}.
     *
     * @param <V> Type of the returned value.
     * @param supplier Supplier.
     * @return Value.
     * @throws StorageClosedException If the storage is closed.
     */
    protected <V> V busy(Supplier<V> supplier) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            return supplier.get();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Creates a summary info of the storage in the format "table=user, partitionId=1".
     */
    public String createStorageInfo() {
        return IgniteStringFormatter.format("table={}, partitionId={}", tableStorage.getTableName(), partitionId);
    }

    /**
     * Prepares the storage and its indexes for rebalancing.
     *
     * <p>Stops ongoing operations on the storage and its indexes.
     *
     * @throws StorageRebalanceException If there was an error when starting the rebalance.
     */
    public void startRebalance() {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            IgniteUtils.closeAll(getResourcesToCloseOnCleanup());

            hashIndexes.values().forEach(PageMemoryHashIndexStorage::startRebalance);
            sortedIndexes.values().forEach(PageMemorySortedIndexStorage::startRebalance);
        } catch (Exception e) {
            throw new StorageRebalanceException(
                    IgniteStringFormatter.format("Error on start of rebalancing: [{}]", createStorageInfo()),
                    e
            );
        } finally {
            busyLock.unblock();
        }
    }

    /**
     * Completes the rebalancing of the storage and its indexes.
     *
     * @throws StorageRebalanceException If there is an error while completing the storage and its indexes rebalance.
     */
    public void completeRebalance() {
        if (!state.compareAndSet(StorageState.REBALANCE, StorageState.RUNNABLE)) {
            throwExceptionDependingOnStorageStateOnRebalance(state.get(), createStorageInfo());
        }

        hashIndexes.values().forEach(PageMemoryHashIndexStorage::completeRebalance);
        sortedIndexes.values().forEach(PageMemorySortedIndexStorage::completeRebalance);
    }

    /**
     * Sets the last applied index and term on rebalance.
     *
     * @param lastAppliedIndex Last applied index value.
     * @param lastAppliedTerm Last applied term value.
     */
    public abstract void lastAppliedOnRebalance(long lastAppliedIndex, long lastAppliedTerm) throws StorageException;

    /**
     * Returns resources that will have to close on cleanup.
     */
    abstract List<AutoCloseable> getResourcesToCloseOnCleanup();

    /**
     * Sets the RAFT group configuration on rebalance.
     */
    public abstract void committedGroupConfigurationOnRebalance(RaftGroupConfiguration config);

    /**
     * Prepares the storage and its indexes for cleanup.
     *
     * <p>After cleanup (successful or not), method {@link #finishCleanup()} must be called.
     */
    public void startCleanup() throws Exception {
        if (!state.compareAndSet(StorageState.RUNNABLE, StorageState.CLEANUP)) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            IgniteUtils.closeAll(getResourcesToCloseOnCleanup());

            hashIndexes.values().forEach(PageMemoryHashIndexStorage::startCleanup);
            sortedIndexes.values().forEach(PageMemorySortedIndexStorage::startCleanup);
        } finally {
            busyLock.unblock();
        }
    }

    /**
     * Finishes cleanup up the storage and its indexes.
     */
    public void finishCleanup() {
        if (state.compareAndSet(StorageState.CLEANUP, StorageState.RUNNABLE)) {
            hashIndexes.values().forEach(PageMemoryHashIndexStorage::finishCleanup);
            sortedIndexes.values().forEach(PageMemorySortedIndexStorage::finishCleanup);
        }
    }
}
