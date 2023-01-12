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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.NULL_LINK;
import static org.apache.ignite.internal.storage.pagememory.PageMemoryStorageUtils.inBusyLock;
import static org.apache.ignite.internal.storage.pagememory.PageMemoryStorageUtils.throwExceptionDependingStorageStateOnRebalance;
import static org.apache.ignite.internal.storage.pagememory.PageMemoryStorageUtils.throwExceptionIfStorageInProgressOfRebalance;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.PageIdAllocator;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.metric.IoStatisticsHolderNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.configuration.index.HashIndexView;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.TxIdMismatchException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;
import org.apache.ignite.internal.storage.pagememory.AbstractPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.StorageState;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexTree;
import org.apache.ignite.internal.storage.pagememory.index.hash.PageMemoryHashIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.index.sorted.PageMemorySortedIndexStorage;
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexTree;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract implementation of partition storage using Page Memory.
 */
public abstract class AbstractPageMemoryMvPartitionStorage implements MvPartitionStorage {
    private static final byte[] TOMBSTONE_PAYLOAD = new byte[0];

    private static final Predicate<HybridTimestamp> ALWAYS_LOAD_VALUE = timestamp -> true;

    protected static final VarHandle STATE;

    static {
        try {
            STATE = MethodHandles.lookup().findVarHandle(AbstractPageMemoryMvPartitionStorage.class, "state", StorageState.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    protected final int partitionId;

    protected final int groupId;

    protected final AbstractPageMemoryTableStorage tableStorage;

    protected final VersionChainTree versionChainTree;

    protected final RowVersionFreeList rowVersionFreeList;

    protected final IndexColumnsFreeList indexFreeList;

    protected final IndexMetaTree indexMetaTree;

    protected final DataPageReader rowVersionDataPageReader;

    protected final ConcurrentMap<UUID, PageMemoryHashIndexStorage> hashIndexes = new ConcurrentHashMap<>();

    protected final ConcurrentMap<UUID, PageMemorySortedIndexStorage> sortedIndexes = new ConcurrentHashMap<>();

    /** Busy lock. */
    protected final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Current state of the storage. */
    protected volatile StorageState state = StorageState.RUNNABLE;

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
            try (Cursor<IndexMeta> cursor = indexMetaTree.find(null, null)) {
                NamedListView<TableIndexView> indexesCfgView = tableStorage.tablesConfiguration().indexes().value();

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

                return null;
            } catch (Exception e) {
                throw new StorageException("Failed to process SQL indexes during the partition start", e);
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
        var indexDescriptor = new HashIndexDescriptor(indexMeta.id(), tableStorage.tablesConfiguration().value());

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
                    indexDescriptor,
                    initNew
            );

            if (initNew) {
                boolean replaced = indexMetaTree.putx(new IndexMeta(indexMeta.id(), metaPageId));

                assert !replaced;
            }

            return new PageMemoryHashIndexStorage(indexDescriptor, indexFreeList, hashIndexTree);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(e);
        }
    }

    private PageMemorySortedIndexStorage createOrRestoreSortedIndex(IndexMeta indexMeta) {
        var indexDescriptor = new SortedIndexDescriptor(indexMeta.id(), tableStorage.tablesConfiguration().value());

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
                    indexDescriptor,
                    initNew
            );

            if (initNew) {
                boolean replaced = indexMetaTree.putx(new IndexMeta(indexMeta.id(), metaPageId));

                assert !replaced;
            }

            return new PageMemorySortedIndexStorage(indexDescriptor, indexFreeList, sortedIndexTree);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public ReadResult read(RowId rowId, HybridTimestamp timestamp) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state, this::createStorageInfo);

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

    private static boolean lookingForLatestVersion(HybridTimestamp timestamp) {
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
            ByteBufferRow row = rowVersionToBinaryRow(rowVersion);

            return ReadResult.createFromCommitted(versionChain.rowId(), row, rowVersion.timestamp());
        }
    }

    private RowVersion readRowVersion(long nextLink, Predicate<HybridTimestamp> loadValue) {
        ReadRowVersion read = new ReadRowVersion(partitionId);

        try {
            rowVersionDataPageReader.traverse(nextLink, read, loadValue);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Row version lookup failed", e);
        }

        return read.result();
    }

    private static void throwIfChainBelongsToAnotherTx(VersionChain versionChain, UUID txId) {
        assert versionChain.isUncommitted();

        if (!txId.equals(versionChain.transactionId())) {
            throw new TxIdMismatchException(txId, versionChain.transactionId());
        }
    }

    private static @Nullable ByteBufferRow rowVersionToBinaryRow(RowVersion rowVersion) {
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

    private static ReadResult writeIntentToResult(
            VersionChain chain,
            RowVersion rowVersion,
            @Nullable HybridTimestamp lastCommittedTimestamp
    ) {
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

    private RowVersion insertRowVersion(@Nullable BinaryRow row, long nextPartitionlessLink) {
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

    private static byte[] rowBytes(@Nullable BinaryRow row) {
        // TODO IGNITE-16913 Add proper way to write row bytes into array without allocations.
        return row == null ? TOMBSTONE_PAYLOAD : row.bytes();
    }

    @Override
    public @Nullable BinaryRow addWrite(RowId rowId, @Nullable BinaryRow row, UUID txId, UUID commitTableId, int commitPartitionId)
            throws TxIdMismatchException, StorageException {
        return busy(() -> {
            assert rowId.partitionId() == partitionId : rowId;

            VersionChain currentChain = findVersionChain(rowId);

            if (currentChain == null) {
                RowVersion newVersion = insertRowVersion(row, NULL_LINK);

                VersionChain versionChain = VersionChain.createUncommitted(rowId, txId, commitTableId, commitPartitionId,
                        newVersion.link(), NULL_LINK);

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

            VersionChain chainReplacement = VersionChain.createUncommitted(rowId, txId, commitTableId, commitPartitionId,
                    newVersion.link(), newVersion.nextLink());

            updateVersionChain(chainReplacement);

            return res;
        });
    }

    @Override
    public @Nullable BinaryRow abortWrite(RowId rowId) throws StorageException {
        return busy(() -> {
            assert rowId.partitionId() == partitionId : rowId;

            throwExceptionIfStorageInProgressOfRebalance(state, this::createStorageInfo);

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
        busy(() -> {
            assert rowId.partitionId() == partitionId : rowId;

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
    public void addWriteCommitted(RowId rowId, @Nullable BinaryRow row, HybridTimestamp commitTimestamp) throws StorageException {
        busy(() -> {
            assert rowId.partitionId() == partitionId : rowId;

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

    private RowVersion insertCommittedRowVersion(BinaryRow row, HybridTimestamp commitTimestamp, long nextPartitionlessLink) {
        byte[] rowBytes = rowBytes(row);

        RowVersion rowVersion = new RowVersion(partitionId, commitTimestamp, nextPartitionlessLink, ByteBuffer.wrap(rowBytes));

        insertRowVersion(rowVersion);

        return rowVersion;
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageInProgressOfRebalance(state, this::createStorageInfo);

            return new ScanVersionsCursor(rowId);
        });
    }

    private static ReadResult rowVersionToResultNotFillingLastCommittedTs(VersionChain versionChain, RowVersion rowVersion) {
        ByteBufferRow row = new ByteBufferRow(rowVersion.value());

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
            throwExceptionIfStorageInProgressOfRebalance(state, this::createStorageInfo);

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
            throwExceptionIfStorageInProgressOfRebalance(state, this::createStorageInfo);

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
            throwExceptionIfStorageInProgressOfRebalance(state, this::createStorageInfo);

            try {
                return versionChainTree.size();
            } catch (IgniteInternalCheckedException e) {
                throw new StorageException("Error occurred while fetching the size", e);
            }
        });
    }

    private abstract class BasePartitionTimestampCursor implements PartitionTimestampCursor {
        final Cursor<VersionChain> treeCursor;

        @Nullable
        ReadResult nextRead = null;

        @Nullable
        VersionChain currentChain = null;

        private BasePartitionTimestampCursor(Cursor<VersionChain> treeCursor) {
            this.treeCursor = treeCursor;
        }

        @Override
        public final ReadResult next() {
            return busy(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state, AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

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
        public @Nullable BinaryRow committed(HybridTimestamp timestamp) {
            return busy(() -> {
                throwExceptionIfStorageInProgressOfRebalance(state, AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

                if (currentChain == null) {
                    throw new IllegalStateException();
                }

                ReadResult result = findRowVersionByTimestamp(currentChain, timestamp);

                if (result.isEmpty()) {
                    return null;
                }

                // We don't check if row conforms the key filter here, because we've already checked it.
                return result.binaryRow();
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

        private TimestampCursor(Cursor<VersionChain> treeCursor, HybridTimestamp timestamp) {
            super(treeCursor);

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
                Boolean hasNext = busy(() -> {
                    throwExceptionIfStorageInProgressOfRebalance(state, AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

                    if (!treeCursor.hasNext()) {
                        iterationExhausted = true;

                        return false;
                    }

                    VersionChain chain = treeCursor.next();
                    ReadResult result = findRowVersionByTimestamp(chain, timestamp);

                    if (result.isEmpty() && !result.isWriteIntent()) {
                        return null;
                    }

                    nextRead = result;
                    currentChain = chain;

                    return true;
                });

                if (hasNext != null) {
                    return hasNext;
                }
            }
        }
    }

    /**
     * Implementation of the cursor that iterates over the page memory storage with the respect to the transaction id. Scans the partition
     * and returns a cursor of values. All filtered values must either be uncommitted in the current transaction or already committed in a
     * different transaction.
     */
    private class LatestVersionsCursor extends BasePartitionTimestampCursor {
        private boolean iterationExhausted = false;

        private LatestVersionsCursor(Cursor<VersionChain> treeCursor) {
            super(treeCursor);
        }

        @Override
        public boolean hasNext() {
            if (nextRead != null) {
                return true;
            }

            if (iterationExhausted) {
                return false;
            }

            while (true) {
                Boolean hasNext = busy(() -> {
                    if (!treeCursor.hasNext()) {
                        iterationExhausted = true;
                        return false;
                    }

                    VersionChain chain = treeCursor.next();
                    ReadResult result = findLatestRowVersion(chain);

                    if (result.isEmpty() && !result.isWriteIntent()) {
                        return null;
                    }

                    nextRead = result;
                    currentChain = chain;

                    return true;
                });

                if (hasNext != null) {
                    return hasNext;
                }
            }
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
            throwExceptionIfStorageInProgressOfRebalance(state, AbstractPageMemoryMvPartitionStorage.this::createStorageInfo);

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
                rowVersion = readRowVersion(rowVersion.nextLink(), ALWAYS_LOAD_VALUE);
            }

            hasNext = rowVersion != null;
        }
    }

    @Override
    public void close() {
        if (!STATE.compareAndSet(this, StorageState.RUNNABLE, StorageState.CLOSED)) {
            StorageState state = this.state;

            assert state == StorageState.CLOSED : state;

            return;
        }

        busyLock.block();

        versionChainTree.close();
        indexMetaTree.close();

        closeAdditionalResources();

        for (PageMemoryHashIndexStorage hashIndexStorage : hashIndexes.values()) {
            hashIndexStorage.close();
        }

        for (PageMemorySortedIndexStorage sortedIndexStorage : sortedIndexes.values()) {
            sortedIndexStorage.close();
        }

        hashIndexes.clear();
        sortedIndexes.clear();
    }

    /**
     * Closing additional resources when executing {@link #close()}.
     */
    abstract void closeAdditionalResources();

    /**
     * Creates a summary info of the storage in the format "table=user, partitionId=1".
     */
    public String createStorageInfo() {
        return IgniteStringFormatter.format("table={}, partitionId={}", tableStorage.getTableName(), partitionId);
    }

    <V> V busy(Supplier<V> supplier) {
        return inBusyLock(busyLock, supplier, () -> state, this::createStorageInfo);
    }

    /**
     * Prepares the storage and its indexes for rebalancing.
     */
    public CompletableFuture<Void> startRebalance() {
        if (!STATE.compareAndSet(this, StorageState.RUNNABLE, StorageState.REBALANCE)) {
            throwExceptionDependingStorageStateOnRebalance(state, createStorageInfo());
        }

        // TODO: IGNITE-18029 реализовать и еще немного подумать об всей этой кухне

        // TODO: IGNITE-18029 возможный план:
        // TODO: IGNITE-18029 останавливаем нагрузку на хранилище и индексы
        // TODO: IGNITE-18029 пересоздаем все что нам нужно
        // TODO: IGNITE-18029 обновляем деревья и листы для хранилища и индексов
        // TODO: IGNITE-18029 обновляем lastApplied

        return completedFuture(null);
    }

    public CompletableFuture<Void> abortRebalance() {
        // TODO: IGNITE-18029 реализовать
        return completedFuture(null);
    }

    /**
     * Completes the rebalancing of the storage and its indexes.
     *
     * @param lastAppliedIndex Last applied index value.
     * @param lastAppliedTerm Last applied term value.
     */
    public void finishRebalance(long lastAppliedIndex, long lastAppliedTerm) {
        // TODO: IGNITE-18029 реализовать
    }
}
