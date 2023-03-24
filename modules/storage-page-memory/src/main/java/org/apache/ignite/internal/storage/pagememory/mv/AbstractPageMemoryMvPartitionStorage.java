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

import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionDependingOnStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwExceptionIfStorageNotInRunnableOrRebalanceState;
import static org.apache.ignite.internal.storage.util.StorageUtils.throwStorageExceptionIfItCause;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.tree.BplusTree.TreeRowMapClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.configuration.index.HashIndexView;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.BinaryRowAndRowId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
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
import org.apache.ignite.internal.storage.pagememory.mv.FindRowVersion.RowVersionFilter;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcRowVersion;
import org.apache.ignite.internal.storage.util.ReentrantLockByRowId;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.storage.util.StorageUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.CursorUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract implementation of partition storage using Page Memory.
 *
 * <p>A few words about parallel operations with version chains:
 * <ul>
 *     <li>All update operations (including creation) must first be synchronized by row ID using
 *     {@link #inUpdateVersionChainLock(RowId, Supplier)};</li>
 *     <li>Reads and updates of version chains (or a single version) must be synchronized by the {@link #versionChainTree}, for example for
 *     reading you can use {@link #findVersionChain(RowId, Function)} or
 *     {@link AbstractPartitionTimestampCursor#createVersionChainCursorIfMissing()}, and for updates you can use {@link InvokeClosure}
 *     for example {@link AddWriteInvokeClosure} or {@link CommitWriteInvokeClosure}.</li>
 * </ul>
 */
public abstract class AbstractPageMemoryMvPartitionStorage implements MvPartitionStorage {
    private static final byte[] TOMBSTONE_PAYLOAD = new byte[0];

    static final Predicate<HybridTimestamp> ALWAYS_LOAD_VALUE = timestamp -> true;

    static final Predicate<HybridTimestamp> DONT_LOAD_VALUE = timestamp -> false;

    protected final int partitionId;

    protected final int groupId;

    protected final AbstractPageMemoryTableStorage tableStorage;

    protected volatile VersionChainTree versionChainTree;

    protected volatile RowVersionFreeList rowVersionFreeList;

    protected volatile IndexColumnsFreeList indexFreeList;

    protected volatile IndexMetaTree indexMetaTree;

    protected volatile GcQueue gcQueue;

    protected final DataPageReader rowVersionDataPageReader;

    protected final ConcurrentMap<UUID, PageMemoryHashIndexStorage> hashIndexes = new ConcurrentHashMap<>();

    protected final ConcurrentMap<UUID, PageMemorySortedIndexStorage> sortedIndexes = new ConcurrentHashMap<>();

    /** Busy lock. */
    protected final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    protected final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /** Version chain update lock by row ID. */
    protected final ReentrantLockByRowId updateVersionChainLockByRowId = new ReentrantLockByRowId();

    /**
     * Constructor.
     *
     * @param partitionId Partition ID.
     * @param tableStorage Table storage instance.
     * @param rowVersionFreeList Free list for {@link RowVersion}.
     * @param indexFreeList Free list fot {@link IndexColumns}.
     * @param versionChainTree Table tree for {@link VersionChain}.
     * @param indexMetaTree Tree that contains SQL indexes' metadata.
     * @param gcQueue Garbage collection queue.
     */
    protected AbstractPageMemoryMvPartitionStorage(
            int partitionId,
            AbstractPageMemoryTableStorage tableStorage,
            RowVersionFreeList rowVersionFreeList,
            IndexColumnsFreeList indexFreeList,
            VersionChainTree versionChainTree,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue
    ) {
        this.partitionId = partitionId;
        this.tableStorage = tableStorage;

        this.rowVersionFreeList = rowVersionFreeList;
        this.indexFreeList = indexFreeList;

        this.versionChainTree = versionChainTree;
        this.indexMetaTree = indexMetaTree;

        this.gcQueue = gcQueue;

        PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

        groupId = tableStorage.configuration().value().tableId();

        rowVersionDataPageReader = new DataPageReader(pageMemory, groupId, IoStatisticsHolderNoOp.INSTANCE);
    }

    /**
     * Starts a partition by initializing its internal structures.
     */
    public void start() {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            try (Cursor<IndexMeta> cursor = indexMetaTree.find(null, null)) {
                NamedListView<TableIndexView> indexesCfgView = tableStorage.tablesConfiguration().indexes().value();

                for (IndexMeta indexMeta : cursor) {
                    TableIndexView indexCfgView = indexesCfgView.get(indexMeta.indexId());

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
                throw new StorageException("Failed to process SQL indexes during partition start: [{}]", e, createStorageInfo());
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
        return busy(() -> hashIndexes.computeIfAbsent(indexId, uuid -> createOrRestoreHashIndex(createIndexMetaForNewIndex(indexId))));
    }

    /**
     * Returns a sorted index instance, creating index it if necessary.
     *
     * @param indexId Index UUID.
     */
    public PageMemorySortedIndexStorage getOrCreateSortedIndex(UUID indexId) {
        return busy(() -> sortedIndexes.computeIfAbsent(indexId, uuid -> createOrRestoreSortedIndex(createIndexMetaForNewIndex(indexId))));
    }

    private PageMemoryHashIndexStorage createOrRestoreHashIndex(IndexMeta indexMeta) {
        throwExceptionIfStorageNotInRunnableState();

        var indexDescriptor = new HashIndexDescriptor(indexMeta.indexId(), tableStorage.tablesConfiguration().value());

        HashIndexTree hashIndexTree = createHashIndexTree(indexDescriptor, indexMeta);

        return new PageMemoryHashIndexStorage(
                indexDescriptor,
                indexFreeList,
                hashIndexTree,
                indexMetaTree,
                indexMeta.lastBuiltRowIdUuid()
        );
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
                boolean replaced = indexMetaTree.putx(new IndexMeta(indexMeta.indexId(), metaPageId, indexMeta.lastBuiltRowIdUuid()));

                assert !replaced;
            }

            return hashIndexTree;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating hash index tree: [{}, indexId={}]", e, createStorageInfo(), indexMeta.indexId());
        }
    }

    private PageMemorySortedIndexStorage createOrRestoreSortedIndex(IndexMeta indexMeta) {
        throwExceptionIfStorageNotInRunnableState();

        var indexDescriptor = new SortedIndexDescriptor(indexMeta.indexId(), tableStorage.tablesConfiguration().value());

        SortedIndexTree sortedIndexTree = createSortedIndexTree(indexDescriptor, indexMeta);

        return new PageMemorySortedIndexStorage(
                indexDescriptor,
                indexFreeList,
                sortedIndexTree,
                indexMetaTree,
                indexMeta.lastBuiltRowIdUuid()
        );
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
                boolean replaced = indexMetaTree.putx(new IndexMeta(indexDescriptor.id(), metaPageId, indexMeta.lastBuiltRowIdUuid()));

                assert !replaced;
            }

            return sortedIndexTree;
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error creating sorted index tree: [{}, indexId={}]", e, createStorageInfo(), indexMeta.indexId());
        }
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            if (rowId.partitionId() != partitionId) {
                throw new IllegalArgumentException(
                        String.format("RowId partition [%d] is not equal to storage partition [%d].", rowId.partitionId(), partitionId));
            }

            return findVersionChain(rowId, versionChain -> {
                if (versionChain == null) {
                    return ReadResult.empty(rowId);
                }

                if (lookingForLatestVersion(timestamp)) {
                    return findLatestRowVersion(versionChain);
                } else {
                    return findRowVersionByTimestamp(versionChain, timestamp);
                }
            });
        });
    }

    private boolean lookingForLatestVersion(HybridTimestamp timestamp) {
        return timestamp == HybridTimestamp.MAX_VALUE;
    }

    ReadResult findLatestRowVersion(VersionChain versionChain) {
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
            BinaryRow row = rowVersionToBinaryRow(rowVersion);

            return ReadResult.createFromCommitted(versionChain.rowId(), row, rowVersion.timestamp());
        }
    }

    RowVersion readRowVersion(long rowVersionLink, Predicate<HybridTimestamp> loadValue) {
        ReadRowVersion read = new ReadRowVersion(partitionId);

        try {
            rowVersionDataPageReader.traverse(rowVersionLink, read, loadValue);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Row version lookup failed: [link={}, {}]", e, rowVersionLink, createStorageInfo());
        }

        return read.result();
    }

    @Nullable RowVersion findRowVersion(VersionChain versionChain, RowVersionFilter filter, boolean loadValueBytes) {
        assert versionChain.hasHeadLink();

        FindRowVersion findRowVersion = new FindRowVersion(partitionId, loadValueBytes);

        try {
            rowVersionDataPageReader.traverse(versionChain.headLink(), findRowVersion, filter);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(
                    "Error when looking up row version in version chain: [rowId={}, headLink={}, {}]",
                    e,
                    versionChain.rowId(), versionChain.headLink(), createStorageInfo()
            );
        }

        return findRowVersion.getResult();
    }

    @Nullable BinaryRow rowVersionToBinaryRow(RowVersion rowVersion) {
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
    ReadResult findRowVersionByTimestamp(VersionChain versionChain, HybridTimestamp timestamp) {
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
                BinaryRow row;

                if (curCommit.isTombstone()) {
                    row = null;
                } else {
                    row = new ByteBufferRow(curCommit.value());
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

        BinaryRow row = rowVersionToBinaryRow(rowVersion);

        return ReadResult.createFromWriteIntent(
                chain.rowId(),
                row,
                transactionId,
                commitTableId,
                commitPartitionId,
                lastCommittedTimestamp
        );
    }

    void insertRowVersion(RowVersion rowVersion) {
        try {
            rowVersionFreeList.insertDataRow(rowVersion);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot store a row version: [row={}, {}]", e, rowVersion, createStorageInfo());
        }
    }

    static byte[] rowBytes(@Nullable BinaryRow row) {
        // TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        return row == null ? TOMBSTONE_PAYLOAD : row.bytes();
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return inUpdateVersionChainLock(rowId, () -> {
                try {
                    AddWriteInvokeClosure addWrite = new AddWriteInvokeClosure(rowId, row, txId, commitTableId, commitPartitionId, this);

                    versionChainTree.invoke(new VersionChainKey(rowId), null, addWrite);

                    addWrite.afterCompletion();

                    return addWrite.getPreviousUncommittedRowVersion();
                } catch (IgniteInternalCheckedException e) {
                    throwStorageExceptionIfItCause(e);

                    if (e.getCause() instanceof TxIdMismatchException) {
                        throw (TxIdMismatchException) e.getCause();
                    }

                    throw new StorageException("Error while executing addWrite: [rowId={}, {}]", e, rowId, createStorageInfo());
                }
            });
        });
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            return inUpdateVersionChainLock(rowId, () -> {
                try {
                    AbortWriteInvokeClosure abortWrite = new AbortWriteInvokeClosure(rowId, this);

                    versionChainTree.invoke(new VersionChainKey(rowId), null, abortWrite);

                    abortWrite.afterCompletion();

                    return abortWrite.getPreviousUncommittedRowVersion();
                } catch (IgniteInternalCheckedException e) {
                    throwStorageExceptionIfItCause(e);

                    throw new StorageException("Error while executing abortWrite: [rowId={}, {}]", e, rowId, createStorageInfo());
                }
            });
        });
    }

    @Override
    public void commitWrite(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return inUpdateVersionChainLock(rowId, () -> {
                try {
                    CommitWriteInvokeClosure commitWrite = new CommitWriteInvokeClosure(rowId, timestamp, this);

                    versionChainTree.invoke(new VersionChainKey(rowId), null, commitWrite);

                    commitWrite.afterCompletion();

                    return null;
                } catch (IgniteInternalCheckedException e) {
                    throwStorageExceptionIfItCause(e);

                    throw new StorageException("Error while executing commitWrite: [rowId={}, {}]", e, rowId, createStorageInfo());
                }
            });
        });
    }

    void removeRowVersion(RowVersion rowVersion) {
        try {
            rowVersionFreeList.removeDataRowByLink(rowVersion.link());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot remove row version: [row={}, {}]", e, rowVersion, createStorageInfo());
        }
    }

    @Override
    public void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp) throws StorageException {
        assert rowId.partitionId() == partitionId : rowId;

        busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            return inUpdateVersionChainLock(rowId, () -> {
                try {
                    AddWriteCommittedInvokeClosure addWriteCommitted = new AddWriteCommittedInvokeClosure(rowId, row, commitTimestamp,
                            this);

                    versionChainTree.invoke(new VersionChainKey(rowId), null, addWriteCommitted);

                    addWriteCommitted.afterCompletion();

                    return null;
                } catch (IgniteInternalCheckedException e) {
                    throwStorageExceptionIfItCause(e);

                    throw new StorageException("Error while executing addWriteCommitted: [rowId={}, {}]", e, rowId, createStorageInfo());
                }
            });
        });
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            return findVersionChain(rowId, versionChain -> {
                if (versionChain == null) {
                    return CursorUtils.emptyCursor();
                }

                return new ScanVersionsCursor(versionChain, this);
            });
        });
    }

    static ReadResult rowVersionToResultNotFillingLastCommittedTs(VersionChain versionChain, RowVersion rowVersion) {
        RowId rowId = versionChain.rowId();

        if (rowVersion.isCommitted()) {
            if (rowVersion.isTombstone()) {
                return ReadResult.empty(rowId);
            } else {
                BinaryRow row = new ByteBufferRow(rowVersion.value());

                return ReadResult.createFromCommitted(rowId, row, rowVersion.timestamp());
            }
        } else {
            BinaryRow row = rowVersion.isTombstone() ? null : new ByteBufferRow(rowVersion.value());

            return ReadResult.createFromWriteIntent(
                    rowId,
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
            throwExceptionIfStorageNotInRunnableState();

            if (lookingForLatestVersion(timestamp)) {
                return new LatestVersionsCursor(this);
            } else {
                return new TimestampCursor(this, timestamp);
            }
        });
    }

    @Override
    public @Nullable RowId closestRowId(RowId lowerBound) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

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
            throwExceptionIfStorageNotInRunnableState();

            try {
                return versionChainTree.size();
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error occurred while fetching the size.", e);
            }
        });
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
        StorageState previous = state.getAndSet(StorageState.CLOSED);

        if (previous == StorageState.CLOSED) {
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
        resources.add(gcQueue::close);

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
    public abstract void committedGroupConfigurationOnRebalance(byte[] config);

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

    void throwExceptionIfStorageNotInRunnableState() {
        StorageUtils.throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);
    }

    /**
     * Searches version chain by row ID and converts the found version chain to the result if found.
     *
     * @param rowId Row ID.
     * @param mapper Function for converting the version chain to a result, function is executed under the read lock of the page on which
     *      the version chain is located. If the version chain is not found, then {@code null} will be passed to the function.
     */
    <T> @Nullable T findVersionChain(RowId rowId, Function<VersionChain, T> mapper) {
        try {
            return versionChainTree.findOne(new VersionChainKey(rowId), new TreeRowMapClosure<>() {
                @Override
                public T map(VersionChain treeRow) {
                    return mapper.apply(treeRow);
                }
            }, null);
        } catch (IgniteInternalCheckedException e) {
            throwStorageExceptionIfItCause(e);

            throw new StorageException("Row version lookup failed: [rowId={}, {}]", e, rowId, createStorageInfo());
        }
    }

    /**
     * Organizes external synchronization of update operations for the same version chain.
     */
    protected <T> T inUpdateVersionChainLock(RowId rowId, Supplier<T> supplier) {
        return updateVersionChainLockByRowId.inLock(rowId, supplier);
    }

    @Override
    public @Nullable BinaryRowAndRowId pollForVacuum(HybridTimestamp lowWatermark) {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            while (true) {
                // TODO: IGNITE-18867 Get and delete in one call
                GcRowVersion head = gcQueue.getFirst();

                // Garbage collection queue is empty.
                if (head == null) {
                    return null;
                }

                HybridTimestamp rowTimestamp = head.getTimestamp();

                // There are no versions in the garbage collection queue before watermark.
                if (rowTimestamp.compareTo(lowWatermark) > 0) {
                    return null;
                }

                RowId rowId = head.getRowId();

                // If no one has processed the head of the gc queue in parallel, then we must release the lock after executing
                // WriteClosure#execute in MvPartitionStorage#runConsistently so that the indexes can be deleted consistently.
                updateVersionChainLockByRowId.acquireLock(rowId);

                // Someone processed the element in parallel.
                if (!gcQueue.remove(rowId, rowTimestamp, head.getLink())) {
                    updateVersionChainLockByRowId.releaseLock(rowId);

                    continue;
                }

                RowVersion removedRowVersion = removeWriteOnGc(rowId, rowTimestamp, head.getLink());

                return new BinaryRowAndRowId(rowVersionToBinaryRow(removedRowVersion), rowId);
            }
        });
    }

    private RowVersion removeWriteOnGc(RowId rowId, HybridTimestamp rowTimestamp, long rowLink) {
        RemoveWriteOnGcInvokeClosure removeWriteOnGc = new RemoveWriteOnGcInvokeClosure(rowId, rowTimestamp, rowLink, this);

        try {
            versionChainTree.invoke(new VersionChainKey(rowId), null, removeWriteOnGc);
        } catch (IgniteInternalCheckedException e) {
            throwStorageExceptionIfItCause(e);

            throw new StorageException(
                    "Error removing row version from version chain on garbage collection: [rowId={}, rowTimestamp={}, {}]",
                    e,
                    rowId, rowTimestamp, createStorageInfo()
            );
        }

        removeWriteOnGc.afterCompletion();

        return removeWriteOnGc.getResult();
    }

    IndexMeta createIndexMetaForNewIndex(UUID indexId) {
        return new IndexMeta(indexId, 0L, RowId.lowestRowId(partitionId).uuid());
    }
}
