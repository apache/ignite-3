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

package org.apache.ignite.internal.metastorage.server;

import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.NOT_FOUND;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.assertCompactionRevisionLessThanCurrent;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.isLastIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.maxRevisionIndex;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.minRevisionIndex;
import static org.apache.ignite.internal.rocksdb.RocksUtils.incrementPrefix;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.CompactionRevisionUpdateListener;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.jetbrains.annotations.Nullable;

/** Abstract implementation of {@link KeyValueStorage}. */
public abstract class AbstractKeyValueStorage implements KeyValueStorage {
    protected static final Comparator<byte[]> KEY_COMPARATOR = Arrays::compareUnsigned;

    protected final IgniteLogger log = Loggers.forClass(getClass());

    protected final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    protected final FailureProcessor failureProcessor;

    protected final WatchProcessor watchProcessor;

    /**
     * Revision listener for recovery only. Notifies {@link MetaStorageManagerImpl} of current revisions update, {@code null} if recovery
     * is complete.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private @Nullable RecoveryRevisionsListener recoveryRevisionListener;

    /**
     * Revision. Will be incremented for each single-entry or multi-entry update operation.
     */
    protected volatile long rev;

    /**
     * Last compaction revision that was set or restored from a snapshot.
     *
     * <p>This field is used by metastorage read methods to determine whether {@link CompactedException} should be thrown.</p>
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     *
     * <p>Field with a volatile so as not to be blocked when reading the compaction revision by other components, for example due to long
     * write operations, including the compaction itself.</p>
     */
    protected volatile long compactionRevision = -1;

    /**
     * Planned for update compaction revision to ensure monotony without duplicates when updating it.
     *
     * <p>This is necessary to avoid a situation when changing the leader, we get two requests to update the same compaction revision.
     * Fixing the leader change problem is not at the protocol level since the update is performed asynchronously and in the background and
     * we can get into a gap when commands came from different leaders to the same compaction revision, but we simply did not have time to
     * process the update of the compaction revision from the previous leader. This is necessary to cover corner cases with a sufficiently
     * small compaction revision update interval.</p>
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    private volatile long planedUpdateCompactionRevision = -1;

    protected final AtomicBoolean stopCompaction = new AtomicBoolean();

    /** Tracks only cursors, since reading a single entry or a batch is done entirely under {@link #rwLock}. */
    protected final ReadOperationForCompactionTracker readOperationForCompactionTracker;

    /**
     * Events for notification of the {@link WatchProcessor} that were created before the {@link #startWatches start of watches}, after the
     * start of watches there will be {@code null}. Events are sorted by {@link NotifyWatchProcessorEvent#timestamp} and are expected to
     * have no duplicates.
     *
     * <p>Multi-threaded access is guarded by {@link #rwLock}.</p>
     */
    protected @Nullable TreeSet<NotifyWatchProcessorEvent> notifyWatchProcessorEventsBeforeStartingWatches = new TreeSet<>();

    /** Metastorage compaction revision update listeners. */
    private final List<CompactionRevisionUpdateListener> compactionRevisionUpdateListeners = new CopyOnWriteArrayList<>();

    /**
     * Constructor.
     *
     * @param nodeName Node name.
     * @param failureProcessor Failure processor that is used to handle critical errors.
     * @param readOperationForCompactionTracker Read operation tracker for metastorage compaction.
     */
    protected AbstractKeyValueStorage(
            String nodeName,
            FailureProcessor failureProcessor,
            ReadOperationForCompactionTracker readOperationForCompactionTracker
    ) {
        this.failureProcessor = failureProcessor;
        this.readOperationForCompactionTracker = readOperationForCompactionTracker;

        watchProcessor = new WatchProcessor(nodeName, this::get, failureProcessor);
    }

    /** Returns the key revisions for operation, an empty array if not found. */
    protected abstract long[] keyRevisionsForOperation(byte[] key);

    /** Returns key values by revision for operation. */
    protected abstract Value valueForOperation(byte[] key, long revision);

    /**
     * Returns {@code true} if the metastorage is in the recovery state.
     *
     * <p>Method is expected to be invoked under {@link #rwLock}.</p>
     */
    private boolean isInRecoveryState() {
        return recoveryRevisionListener != null;
    }

    /**
     * Returns {@code true} if the watches have {@link #startWatches started}.
     *
     * <p>Method is expected to be invoked under {@link #rwLock}.</p>
     */
    protected abstract boolean areWatchesStarted();

    @Override
    public Entry get(byte[] key) {
        rwLock.readLock().lock();

        try {
            return doGet(key, rev);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public Entry get(byte[] key, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return doGet(key, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> get(byte[] key, long revLowerBound, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return doGet(key, revLowerBound, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys) {
        rwLock.readLock().lock();

        try {
            return doGetAll(keys, rev);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        rwLock.readLock().lock();

        try {
            return doGetAll(keys, revUpperBound);
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public long revision() {
        rwLock.readLock().lock();

        try {
            return rev;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    @Override
    public void saveCompactionRevision(long revision, KeyValueUpdateContext context) {
        assert revision >= 0 : revision;

        rwLock.writeLock().lock();

        try {
            assertCompactionRevisionLessThanCurrent(revision, rev);

            saveCompactionRevision(revision, context, true);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    protected abstract void saveCompactionRevision(long compactionRevision, KeyValueUpdateContext context, boolean advanceSafeTime);

    @Override
    public void setCompactionRevision(long revision) {
        assert revision >= 0 : revision;

        rwLock.writeLock().lock();

        try {
            assertCompactionRevisionLessThanCurrent(revision, rev);

            compactionRevision = revision;

            notifyRevisionsUpdate();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public long getCompactionRevision() {
        return compactionRevision;
    }

    @Override
    public void updateCompactionRevision(long compactionRevision, KeyValueUpdateContext context) {
        assert compactionRevision >= 0 : compactionRevision;

        rwLock.writeLock().lock();

        try {
            assertCompactionRevisionLessThanCurrent(compactionRevision, rev);

            saveCompactionRevision(compactionRevision, context, false);

            if (isInRecoveryState()) {
                setCompactionRevision(compactionRevision);
            } else {
                var notifyWatchesEvent = new AdvanceSafeTimeEvent(() -> {
                    setCompactionRevision(compactionRevision);

                    compactionRevisionUpdateListeners.forEach(listener -> listener.onUpdate(compactionRevision));
                }, context.timestamp);

                if (compactionRevision > planedUpdateCompactionRevision) {
                    planedUpdateCompactionRevision = compactionRevision;

                    notifyWatchProcessor(notifyWatchesEvent);
                } else if (areWatchesStarted()) {
                    watchProcessor.advanceSafeTime(() -> {}, context.timestamp);
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void stopCompaction() {
        stopCompaction.set(true);
    }

    @Override
    public byte @Nullable [] nextKey(byte[] key) {
        return incrementPrefix(key);
    }

    @Override
    public void registerRevisionUpdateListener(RevisionUpdateListener listener) {
        watchProcessor.registerRevisionUpdateListener(listener);
    }

    @Override
    public void unregisterRevisionUpdateListener(RevisionUpdateListener listener) {
        watchProcessor.unregisterRevisionUpdateListener(listener);
    }

    @Override
    public CompletableFuture<Void> notifyRevisionUpdateListenerOnStart(long newRevision) {
        return watchProcessor.notifyUpdateRevisionListeners(newRevision);
    }

    /** Registers a metastorage compaction revision update listener. */
    @Override
    public void registerCompactionRevisionUpdateListener(CompactionRevisionUpdateListener listener) {
        compactionRevisionUpdateListeners.add(listener);
    }

    /** Unregisters a metastorage compaction revision update listener. */
    @Override
    public void unregisterCompactionRevisionUpdateListener(CompactionRevisionUpdateListener listener) {
        compactionRevisionUpdateListeners.remove(listener);
    }

    @Override
    public void setRecoveryRevisionsListener(@Nullable RecoveryRevisionsListener listener) {
        rwLock.writeLock().lock();

        try {
            this.recoveryRevisionListener = listener;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void removeWatch(WatchListener listener) {
        watchProcessor.removeWatch(listener);
    }

    @Override
    public void watchRange(byte[] keyFrom, byte @Nullable [] keyTo, long rev, WatchListener listener) {
        assert rev > 0 : rev;

        Predicate<byte[]> rangePredicate = keyTo == null
                ? k -> KEY_COMPARATOR.compare(keyFrom, k) <= 0
                : k -> KEY_COMPARATOR.compare(keyFrom, k) <= 0 && KEY_COMPARATOR.compare(keyTo, k) > 0;

        watchProcessor.addWatch(new Watch(rev, listener, rangePredicate));
    }

    @Override
    public void watchExact(Collection<byte[]> keys, long rev, WatchListener listener) {
        assert rev > 0 : rev;
        assert !keys.isEmpty();

        TreeSet<byte[]> keySet = new TreeSet<>(KEY_COMPARATOR);

        keySet.addAll(keys);

        Predicate<byte[]> inPredicate = keySet::contains;

        watchProcessor.addWatch(new Watch(rev, listener, inPredicate));
    }

    @Override
    public void watchExact(byte[] key, long rev, WatchListener listener) {
        assert rev > 0 : rev;

        Predicate<byte[]> exactPredicate = k -> KEY_COMPARATOR.compare(k, key) == 0;

        watchProcessor.addWatch(new Watch(rev, listener, exactPredicate));
    }

    /** Notifies of revision update. Must be called under the {@link #rwLock}. */
    protected void notifyRevisionsUpdate() {
        if (recoveryRevisionListener != null) {
            // Listener must be invoked only on recovery, after recovery listener must be null.
            recoveryRevisionListener.onUpdate(createCurrentRevisions());
        }
    }

    protected Entry doGet(byte[] key, long revUpperBound) {
        assert revUpperBound >= 0 : revUpperBound;

        long[] keyRevisions = keyRevisionsForOperation(key);
        int maxRevisionIndex = maxRevisionIndex(keyRevisions, revUpperBound);

        if (maxRevisionIndex == NOT_FOUND) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revUpperBound, compactionRevision);

            return EntryImpl.empty(key);
        }

        long revision = keyRevisions[maxRevisionIndex];

        Value value = valueForOperation(key, revision);

        if (revUpperBound <= compactionRevision && (!isLastIndex(keyRevisions, maxRevisionIndex) || value.tombstone())) {
            throw new CompactedException(revUpperBound, compactionRevision);
        }

        return EntryImpl.toEntry(key, revision, value);
    }

    private List<Entry> doGet(byte[] key, long revLowerBound, long revUpperBound) {
        assert revLowerBound >= 0 : revLowerBound;
        assert revUpperBound >= 0 : revUpperBound;
        assert revUpperBound >= revLowerBound : "revLowerBound=" + revLowerBound + ", revUpperBound=" + revUpperBound;

        long[] keyRevisions = keyRevisionsForOperation(key);

        int minRevisionIndex = minRevisionIndex(keyRevisions, revLowerBound);
        int maxRevisionIndex = maxRevisionIndex(keyRevisions, revUpperBound);

        if (minRevisionIndex == NOT_FOUND || maxRevisionIndex == NOT_FOUND) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revLowerBound, compactionRevision);

            return List.of();
        }

        var entries = new ArrayList<Entry>();

        for (int i = minRevisionIndex; i <= maxRevisionIndex; i++) {
            long revision = keyRevisions[i];

            Value value;

            // More complex check to read less from disk.
            // Optimization for persistent storage.
            if (revision <= compactionRevision) {
                if (!isLastIndex(keyRevisions, i)) {
                    continue;
                }

                value = valueForOperation(key, revision);

                if (value.tombstone()) {
                    continue;
                }
            } else {
                value = valueForOperation(key, revision);
            }

            entries.add(EntryImpl.toEntry(key, revision, value));
        }

        if (entries.isEmpty()) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revLowerBound, compactionRevision);
        }

        return entries;
    }

    private List<Entry> doGetAll(List<byte[]> keys, long revUpperBound) {
        assert !keys.isEmpty();
        assert revUpperBound >= 0 : revUpperBound;

        var res = new ArrayList<Entry>(keys.size());

        for (byte[] key : keys) {
            res.add(doGet(key, revUpperBound));
        }

        return res;
    }

    @Override
    public void advanceSafeTime(KeyValueUpdateContext context) {
        rwLock.writeLock().lock();

        try {
            setIndexAndTerm(context.index, context.term);

            if (areWatchesStarted()) {
                watchProcessor.advanceSafeTime(() -> {}, context.timestamp);
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public Revisions revisions() {
        rwLock.readLock().lock();

        try {
            return createCurrentRevisions();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    private Revisions createCurrentRevisions() {
        return new Revisions(rev, compactionRevision);
    }

    protected void notifyWatchProcessor(NotifyWatchProcessorEvent event) {
        if (areWatchesStarted()) {
            event.notify(watchProcessor);
        } else {
            boolean added = notifyWatchProcessorEventsBeforeStartingWatches.add(event);

            assert added : event;
        }
    }

    protected void drainNotifyWatchProcessorEventsBeforeStartingWatches() {
        assert !areWatchesStarted();

        notifyWatchProcessorEventsBeforeStartingWatches.forEach(event -> event.notify(watchProcessor));

        notifyWatchProcessorEventsBeforeStartingWatches = null;
    }
}
