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
import static org.apache.ignite.internal.storage.util.StorageUtils.transitionToClosedState;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datapage.DataPageReader;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.tree.BplusTree.TreeRowMapClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.util.GradualTaskExecutor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.AbortResult;
import org.apache.ignite.internal.storage.AddWriteCommittedResult;
import org.apache.ignite.internal.storage.AddWriteResult;
import org.apache.ignite.internal.storage.CommitResult;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.RowMeta;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.StorageRebalanceException;
import org.apache.ignite.internal.storage.gc.GcEntry;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.lease.LeaseInfo;
import org.apache.ignite.internal.storage.pagememory.AbstractPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMetaTree;
import org.apache.ignite.internal.storage.pagememory.mv.CommitWriteInvokeClosure.UpdateTimestampHandler;
import org.apache.ignite.internal.storage.pagememory.mv.FindRowVersion.RowVersionFilter;
import org.apache.ignite.internal.storage.pagememory.mv.RemoveWriteOnGcInvokeClosure.UpdateNextLinkHandler;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcQueue;
import org.apache.ignite.internal.storage.pagememory.mv.gc.GcRowVersion;
import org.apache.ignite.internal.storage.util.LocalLocker;
import org.apache.ignite.internal.storage.util.LockByRowId;
import org.apache.ignite.internal.storage.util.StorageState;
import org.apache.ignite.internal.storage.util.StorageUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.CursorUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract implementation of partition storage using Page Memory.
 *
 * <p>A few words about parallel operations with version chains:
 * <ul>
 *     <li>Reads and updates of version chains (or a single version) must be synchronized by the
 *     {@link RenewablePartitionStorageState#versionChainTree()}, for example for
 *     reading you can use {@link #findVersionChain(RowId, Function)} or
 *     {@link AbstractPartitionTimestampCursor#createVersionChainCursorIfMissing()}, and for updates you can use {@link InvokeClosure}
 *     for example {@link AddWriteInvokeClosure} or {@link CommitWriteInvokeClosure}.</li>
 * </ul>
 */
public abstract class AbstractPageMemoryMvPartitionStorage implements MvPartitionStorage {
    static final Predicate<HybridTimestamp> ALWAYS_LOAD_VALUE = timestamp -> true;

    static final Predicate<HybridTimestamp> DONT_LOAD_VALUE = timestamp -> false;

    /** Preserved {@link LocalLocker} instance to allow nested calls of {@link #runConsistently(WriteClosure)}. */
    static final ThreadLocal<LocalLocker> THREAD_LOCAL_LOCKER = new ThreadLocal<>();

    protected final int partitionId;

    protected final AbstractPageMemoryTableStorage tableStorage;

    final PageMemoryIndexes indexes;

    /** Current state of the storage. */
    final AtomicReference<StorageState> state = new AtomicReference<>(StorageState.RUNNABLE);

    /** Version chain update lock by row ID. */
    final LockByRowId lockByRowId = new LockByRowId();

    final GradualTaskExecutor destructionExecutor;

    final FailureProcessor failureProcessor;

    volatile RenewablePartitionStorageState renewableState;

    private final DataPageReader rowVersionDataPageReader;

    /** Busy lock. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final UpdateNextLinkHandler updateNextLinkHandler;

    private final UpdateTimestampHandler updateTimestampHandler;

    /**
     * Constructor.
     *
     * @param partitionId Partition ID.
     * @param tableStorage Table storage instance.
     */
    AbstractPageMemoryMvPartitionStorage(
            int partitionId,
            AbstractPageMemoryTableStorage tableStorage,
            RenewablePartitionStorageState renewableState,
            ExecutorService destructionExecutor,
            FailureProcessor failureProcessor
    ) {
        this.partitionId = partitionId;
        this.tableStorage = tableStorage;
        this.renewableState = renewableState;
        this.destructionExecutor = createGradualTaskExecutor(destructionExecutor);
        this.failureProcessor = failureProcessor;
        this.indexes = new PageMemoryIndexes(this.destructionExecutor, failureProcessor, this::runConsistently);

        PageMemory pageMemory = tableStorage.dataRegion().pageMemory();

        rowVersionDataPageReader = new DataPageReader(pageMemory, tableStorage.getTableId());
        updateNextLinkHandler = new UpdateNextLinkHandler();
        updateTimestampHandler = new UpdateTimestampHandler();
    }

    protected abstract GradualTaskExecutor createGradualTaskExecutor(ExecutorService threadPool);

    /**
     * Starts a partition by initializing its internal structures.
     */
    public void start() {
        busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            RenewablePartitionStorageState localState = renewableState;

            try {
                indexes.performRecovery(
                        localState.indexMetaTree(),
                        localState.indexStorageFactory(),
                        tableStorage.getIndexDescriptorSupplier()
                );
            } catch (Exception e) {
                throw new StorageException("Failed to process SQL indexes during partition start: [{}]", e, createStorageInfo());
            }

            return null;
        });
    }

    /**
     * Returns the partition ID.
     */
    public int partitionId() {
        return partitionId;
    }

    /**
     * Creates a hash index.
     *
     * @param indexDescriptor Index descriptor.
     */
    public void createHashIndex(StorageHashIndexDescriptor indexDescriptor) {
        busy(() -> indexes.createHashIndex(indexDescriptor, renewableState.indexStorageFactory()));
    }

    /**
     * Creates a sorted index.
     *
     * @param indexDescriptor Index descriptor.
     */
    public void createSortedIndex(StorageSortedIndexDescriptor indexDescriptor) {
        busy(() -> indexes.createSortedIndex(indexDescriptor, renewableState.indexStorageFactory()));
    }

    void updateRenewableState(
            VersionChainTree versionChainTree,
            FreeListImpl freeList,
            IndexMetaTree indexMetaTree,
            GcQueue gcQueue
    ) {
        var newState = new RenewablePartitionStorageState(
                tableStorage,
                partitionId,
                versionChainTree,
                freeList,
                indexMetaTree,
                gcQueue
        );

        this.renewableState = newState;

        indexes.updateDataStructures(newState.indexStorageFactory());
    }

    /**
     * Checks if current thread holds a lock on passed row ID.
     */
    static boolean rowIsLocked(RowId rowId) {
        LocalLocker locker = THREAD_LOCAL_LOCKER.get();

        return locker != null && locker.isLocked(rowId);
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

    private static boolean lookingForLatestVersion(HybridTimestamp timestamp) {
        return HybridTimestamp.MAX_VALUE.equals(timestamp);
    }

    ReadResult findLatestRowVersion(VersionChain versionChain) {
        return findLatestRowVersion(versionChain, ALWAYS_LOAD_VALUE);
    }

    private ReadResult findLatestRowVersion(VersionChain versionChain, Predicate<HybridTimestamp> loadValue) {
        RowVersion rowVersion = readRowVersion(versionChain.headLink(), loadValue);

        if (versionChain.isUncommitted()) {
            assert versionChain.transactionId() != null;

            HybridTimestamp newestCommitTs = null;

            if (versionChain.hasCommittedVersions()) {
                long newestCommitLink = versionChain.newestCommittedLink();
                newestCommitTs = readRowVersion(newestCommitLink, loadValue).timestamp();
            }

            return writeIntentToResult(versionChain, rowVersion, newestCommitTs);
        } else {
            return ReadResult.createFromCommitted(versionChain.rowId(), rowVersion.value(), rowVersion.timestamp());
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
                    row = curCommit.value();
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
        int commitTableId = chain.commitZoneId();
        int commitPartitionId = chain.commitPartitionId();

        return ReadResult.createFromWriteIntent(
                chain.rowId(),
                rowVersion.value(),
                transactionId,
                commitTableId,
                commitPartitionId,
                lastCommittedTimestamp
        );
    }

    void insertRowVersion(RowVersion rowVersion) {
        try {
            renewableState.freeList().insertDataRow(rowVersion);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot store a row version: [row={}, {}]", e, rowVersion, createStorageInfo());
        }
    }

    @Override
    public AddWriteResult addWrite(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitPartitionId
    ) throws StorageException {
        assert rowId.partitionId() == partitionId : addWriteInfo(rowId, row, txId, commitZoneId, commitPartitionId);

        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            assert rowIsLocked(rowId) : addWriteInfo(rowId, row, txId, commitZoneId, commitPartitionId);

            try {
                var addWrite = new AddWriteInvokeClosure(rowId, row, txId, commitZoneId, commitPartitionId, this);

                renewableState.versionChainTree().invoke(new VersionChainKey(rowId), null, addWrite);

                addWrite.afterCompletion();

                AddWriteResult addWriteResult = addWrite.addWriteResult();

                assert addWriteResult != null : addWriteInfo(rowId, row, txId, commitZoneId, commitPartitionId);

                return addWriteResult;
            } catch (IgniteInternalCheckedException e) {
                throwStorageExceptionIfItCause(e);

                throw new StorageException(
                        "Error while executing addWrite: [{}]",
                        e,
                        addWriteInfo(rowId, row, txId, commitZoneId, commitPartitionId)
                );
            }
        });
    }

    @Override
    public AbortResult abortWrite(RowId rowId, UUID txId) throws StorageException {
        assert rowId.partitionId() == partitionId : abortWriteInfo(rowId, txId);

        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            assert rowIsLocked(rowId) : abortWriteInfo(rowId, txId);

            try {
                var abortWrite = new AbortWriteInvokeClosure(rowId, txId, this);

                renewableState.versionChainTree().invoke(new VersionChainKey(rowId), null, abortWrite);

                abortWrite.afterCompletion();

                AbortResult abortResult = abortWrite.abortResult();

                assert abortResult != null : abortWriteInfo(rowId, txId);

                return abortResult;
            } catch (IgniteInternalCheckedException e) {
                throwStorageExceptionIfItCause(e);

                throw new StorageException("Error while executing abortWrite: [{}]", e, abortWriteInfo(rowId, txId));
            }
        });
    }

    @Override
    public CommitResult commitWrite(RowId rowId, HybridTimestamp timestamp, UUID txId) throws StorageException {
        assert rowId.partitionId() == partitionId : commitWriteInfo(rowId, timestamp, txId);

        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            assert rowIsLocked(rowId) : commitWriteInfo(rowId, timestamp, txId);

            try {
                var commitWrite = new CommitWriteInvokeClosure(
                        rowId,
                        timestamp,
                        txId,
                        updateTimestampHandler,
                        this
                );

                renewableState.versionChainTree().invoke(new VersionChainKey(rowId), null, commitWrite);

                commitWrite.afterCompletion();

                CommitResult commitResult = commitWrite.commitResult();

                assert commitResult != null : commitWriteInfo(rowId, timestamp, txId);

                return commitResult;
            } catch (IgniteInternalCheckedException e) {
                throwStorageExceptionIfItCause(e);

                throw new StorageException("Error while executing commitWrite: [{}]", e, commitWriteInfo(rowId, timestamp, txId));
            }
        });
    }

    void removeRowVersion(RowVersion rowVersion) {
        try {
            renewableState.freeList().removeDataRowByLink(rowVersion.link());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Cannot remove row version: [row={}, {}]", e, rowVersion, createStorageInfo());
        }
    }

    @Override
    public AddWriteCommittedResult addWriteCommitted(
            RowId rowId,
            @Nullable BinaryRow row,
            HybridTimestamp commitTimestamp
    ) throws StorageException {
        assert rowId.partitionId() == partitionId : addWriteCommittedInfo(rowId, row, commitTimestamp);

        return busy(() -> {
            throwExceptionIfStorageNotInRunnableOrRebalanceState(state.get(), this::createStorageInfo);

            assert rowIsLocked(rowId) : addWriteCommittedInfo(rowId, row, commitTimestamp);

            try {
                var addWriteCommitted = new AddWriteCommittedInvokeClosure(rowId, row, commitTimestamp, this);

                renewableState.versionChainTree().invoke(new VersionChainKey(rowId), null, addWriteCommitted);

                addWriteCommitted.afterCompletion();

                AddWriteCommittedResult addWriteCommittedResult = addWriteCommitted.addWriteCommittedResult();

                assert addWriteCommittedResult != null : addWriteCommittedInfo(rowId, row, commitTimestamp);

                return addWriteCommittedResult;
            } catch (IgniteInternalCheckedException e) {
                throwStorageExceptionIfItCause(e);

                throw new StorageException(
                        "Error while executing addWriteCommitted: [{}]",
                        e,
                        addWriteCommittedInfo(rowId, row, commitTimestamp)
                );
            }
        });
    }

    @Override
    public Cursor<ReadResult> scanVersions(RowId rowId) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            assert rowIsLocked(rowId);

            return findVersionChain(rowId, versionChain -> {
                if (versionChain == null) {
                    return CursorUtils.emptyCursor();
                }

                return new ScanVersionsCursor(versionChain, this);
            });
        });
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

            try (Cursor<VersionChain> cursor = renewableState.versionChainTree().find(new VersionChainKey(lowerBound), null)) {
                return cursor.hasNext() ? cursor.next().rowId() : null;
            } catch (Exception e) {
                throw new StorageException("Error occurred while trying to read a row id", e);
            }
        });
    }

    @Override
    public @Nullable RowId highestRowId() throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            try {
                VersionChain lastChain = renewableState.versionChainTree().findLast();
                return lastChain == null ? null : lastChain.rowId();
            } catch (Exception e) {
                throw new StorageException("Error occurred while trying to read a row id", e);
            }
        });
    }

    @Override
    public List<RowMeta> rowsStartingWith(RowId lowerBoundInclusive, RowId upperBoundInclusive, int limit) throws StorageException {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            VersionChainKey lowerBoundKey = new VersionChainKey(lowerBoundInclusive);
            VersionChainKey upperBoundKey = new VersionChainKey(upperBoundInclusive);
            try (Cursor<VersionChain> cursor = renewableState.versionChainTree().find(lowerBoundKey, upperBoundKey)) {
                List<RowMeta> result = new ArrayList<>();

                for (int i = 0; i < limit && cursor.hasNext(); i++) {
                    VersionChain versionChain = cursor.next();
                    RowMeta row = new RowMeta(
                            versionChain.rowId(),
                            versionChain.transactionId(),
                            versionChain.commitZoneId(),
                            versionChain.commitPartitionId()
                    );
                    result.add(row);
                }

                return result;
            } catch (Exception e) {
                throw new StorageException("Error occurred while trying to read a row id", e);
            }
        });
    }

    @Override
    public void close() {
        if (!transitionToClosedState(state, this::createStorageInfo)) {
            return;
        }

        busyLock.block();

        closeResources();
    }

    /**
     * Closes resources of this storage. Must be closed only when the busy lock is already blocked.
     */
    public void closeResources() {
        try {
            closeAll(getResourcesToClose());
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    /**
     * Returns resources that should be closed on {@link #close()}.
     */
    protected List<AutoCloseable> getResourcesToClose() {
        List<AutoCloseable> resources = new ArrayList<>();

        RenewablePartitionStorageState localState = renewableState;

        resources.add(destructionExecutor::close);
        resources.add(localState.versionChainTree()::close);
        resources.add(localState.indexMetaTree()::close);
        resources.add(localState.gcQueue()::close);

        resources.addAll(indexes.getResourcesToClose());

        return resources;
    }

    /**
     * Transitions this storage to the {@link StorageState#DESTROYED} state. Blocks the busy lock, but does not
     * close the resources (they will have to be closed by calling {@link #closeResources()}).
     *
     * @return {@code true} if this call actually made the transition and, hence, the caller must call {@link #closeResources()}.
     */
    public boolean transitionToDestroyedState() {
        if (!StorageUtils.transitionToDestroyedState(state)) {
            return false;
        }

        indexes.transitionToDestroyedState();

        busyLock.block();

        return true;
    }

    /**
     * Performs a supplier using a {@link #busyLock}.
     *
     * @param <V> Type of the returned value.
     * @param supplier Supplier.
     * @return Value.
     * @throws StorageClosedException If the storage is closed.
     */
    <V> V busy(Supplier<V> supplier) {
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
     * Executes a Runnable using a {@link #busyLock}.
     *
     * @param action Action.
     * @throws StorageClosedException If the storage is closed.
     */
    void busy(Runnable action) {
        if (!busyLock.enterBusy()) {
            throwExceptionDependingOnStorageState(state.get(), createStorageInfo());
        }

        try {
            action.run();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Performs a {@code fn} in {@code busyLock} if {@link IgniteSpinBusyLock#enterBusy()} succeed. Otherwise it just silently returns.
     *
     * @param fn Runnable to run.
     */
    void busySafe(Runnable fn) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            fn.run();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Creates a summary info of the storage in the format "table=user, partitionId=1".
     */
    public String createStorageInfo() {
        return IgniteStringFormatter.format("tableId={}, partitionId={}", tableStorage.getTableId(), partitionId);
    }

    /** Creates a string with information about the {@link #commitWrite} for logging and errors. */
    String commitWriteInfo(RowId rowId, HybridTimestamp timestamp, UUID txId) {
        return IgniteStringFormatter.format("rowId={}, timestamp={}, txId={}, {}", rowId, timestamp, txId, createStorageInfo());
    }

    /** Creates a string with information about the {@link #abortWrite} for logging and errors. */
    String abortWriteInfo(RowId rowId, UUID txId) {
        return IgniteStringFormatter.format("rowId={}, txId={}, {}", rowId, txId, createStorageInfo());
    }

    /** Creates a string with information about the {@link #addWrite} for logging and errors. */
    String addWriteInfo(
            RowId rowId,
            @Nullable BinaryRow row,
            UUID txId,
            int commitZoneId,
            int commitPartitionId
    ) {
        return IgniteStringFormatter.format(
                "rowId={}, rowIsTombstone={}, txId={}, commitZoneId={}, commitPartitionId={}, {}",
                rowId, row == null, txId, commitZoneId, commitPartitionId, createStorageInfo()
        );
    }

    /** Creates a string with information about the {@link #addWriteCommitted} for logging and errors. */
    String addWriteCommittedInfo(
            RowId rowId,
            @Nullable BinaryRow row,
            HybridTimestamp commitTimestamp
    ) {
        return IgniteStringFormatter.format(
                "rowId={}, rowIsTombstone={}, commitTimestamp={}, {}",
                rowId, row == null, commitTimestamp, createStorageInfo()
        );
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
            closeAll(getResourcesToCloseOnCleanup());

            indexes.startRebalance();
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
     * Prepares the storage and its indexes for rebalance abortion.
     *
     * <p>Stops ongoing operations on the storage and its indexes.</p>
     *
     * @throws StorageRebalanceException If there was an error when starting the rebalance abortion.
     */
    public void startAbortRebalance() {
        // Changed storage states and expect all storage operations to stop soon.
        busyLock.block();

        try {
            closeAll(getResourcesToCloseOnCleanup());
        } catch (Exception e) {
            throw new StorageRebalanceException(
                    IgniteStringFormatter.format("Error on start abort of rebalancing: [{}]", createStorageInfo()),
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

        indexes.completeRebalance();
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
     * Updates the current lease information in the storage on rebalance.
     */
    public abstract void updateLeaseOnRebalance(LeaseInfo leaseInfo);

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
            closeAll(getResourcesToCloseOnCleanup());

            indexes.startCleanup();
        } finally {
            busyLock.unblock();
        }
    }

    /**
     * Finishes cleanup up the storage and its indexes.
     */
    public void finishCleanup() {
        if (state.compareAndSet(StorageState.CLEANUP, StorageState.RUNNABLE)) {
            indexes.finishCleanup();
        }
    }

    void throwExceptionIfStorageNotInRunnableState() {
        StorageUtils.throwExceptionIfStorageNotInRunnableState(state.get(), this::createStorageInfo);
    }

    /**
     * Searches version chain by row ID and converts the found version chain to the result if found.
     *
     * @param rowId Row ID.
     * @param mapper Function for converting the version chain to a result, function is executed under the read lock of the page on
     *         which the version chain is located. If the version chain is not found, then {@code null} will be passed to the function.
     */
    <T> @Nullable T findVersionChain(RowId rowId, Function<VersionChain, T> mapper) {
        try {
            return renewableState.versionChainTree().findOne(new VersionChainKey(rowId), new TreeRowMapClosure<>() {
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

    @Override
    public List<GcEntry> peek(HybridTimestamp lowWatermark, int count) {
        return busy(() -> {
            throwExceptionIfStorageNotInRunnableState();

            if (count <= 0) {
                return List.of();
            } else if (count == 1) {
                // Use a more optimal implementation to avoid creating a cursor that can load multiple elements from the page at once.
                return peekSingleGcEntryBusy(lowWatermark);
            }

            return peekGcEntriesBusy(lowWatermark, count);
        });
    }

    @Override
    public @Nullable BinaryRow vacuum(GcEntry entry) {
        assert THREAD_LOCAL_LOCKER.get() != null;
        assert THREAD_LOCAL_LOCKER.get().isLocked(entry.getRowId());

        // Assertion above guarantees that we're in "runConsistently" closure.
        throwExceptionIfStorageNotInRunnableState();

        assert entry instanceof GcRowVersion : entry;

        GcRowVersion gcRowVersion = (GcRowVersion) entry;

        RowId rowId = entry.getRowId();
        HybridTimestamp rowTimestamp = gcRowVersion.getTimestamp();

        // Someone processed the element in parallel.
        if (!renewableState.gcQueue().remove(rowId, rowTimestamp, gcRowVersion.getLink())) {
            return null;
        }

        RowVersion removedRowVersion = removeWriteOnGc(rowId, rowTimestamp, gcRowVersion.getLink());

        return removedRowVersion.value();
    }

    private RowVersion removeWriteOnGc(RowId rowId, HybridTimestamp rowTimestamp, long rowLink) {
        RemoveWriteOnGcInvokeClosure removeWriteOnGc = new RemoveWriteOnGcInvokeClosure(
                rowId,
                rowTimestamp,
                rowLink,
                updateNextLinkHandler,
                this
        );

        try {
            renewableState.versionChainTree().invoke(new VersionChainKey(rowId), null, removeWriteOnGc);
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

    /**
     * Returns a index storage instance or {@code null} if not exists.
     *
     * @param indexId Index ID.
     */
    public @Nullable IndexStorage getIndex(int indexId) {
        return busy(() -> indexes.getIndex(indexId));
    }

    /**
     * Destroys an index storage identified by the given index ID.
     *
     * @param indexId Index ID which storage will be destroyed.
     * @return Future that will be completed as soon as the storage has been destroyed.
     */
    public CompletableFuture<Void> destroyIndex(int indexId) {
        return busy(() -> indexes.destroyIndex(indexId, renewableState.indexMetaTree()));
    }

    /**
     * Increments the estimated size of this partition.
     *
     * @see MvPartitionStorage#estimatedSize
     */
    public abstract void incrementEstimatedSize();

    /**
     * Decrements the estimated size of this partition.
     *
     * @see MvPartitionStorage#estimatedSize
     */
    public abstract void decrementEstimatedSize();

    private List<GcEntry> peekSingleGcEntryBusy(HybridTimestamp lowWatermark) {
        GcRowVersion head = renewableState.gcQueue().getFirst();

        // Garbage collection queue is empty.
        if (head == null) {
            return List.of();
        }

        HybridTimestamp rowTimestamp = head.getTimestamp();

        // There are no versions in the garbage collection queue before watermark.
        if (rowTimestamp.compareTo(lowWatermark) > 0) {
            return List.of();
        }

        preloadingForGcIfNeededBusy(head);

        return List.of(head);
    }

    private List<GcEntry> peekGcEntriesBusy(HybridTimestamp lowWatermark, int count) {
        var res = new ArrayList<GcEntry>(count);

        try (Cursor<GcRowVersion> cursor = renewableState.gcQueue().find(null, null)) {
            while (res.size() < count && cursor.hasNext()) {
                GcRowVersion next = cursor.next();

                if (next.getTimestamp().compareTo(lowWatermark) > 0) {
                    break;
                }

                res.add(next);

                preloadingForGcIfNeededBusy(next);
            }
        } catch (IgniteInternalCheckedException e) {
            throwStorageExceptionIfItCause(e);

            throw new StorageException(
                    "Peek GC entries failed: [lowWatermark={}, count={}, {}]",
                    e,
                    lowWatermark, count, createStorageInfo()
            );
        }

        return res;
    }

    protected void preloadingForGcIfNeededBusy(GcRowVersion gcRowVersion) {
        // No-op.
    }
}
