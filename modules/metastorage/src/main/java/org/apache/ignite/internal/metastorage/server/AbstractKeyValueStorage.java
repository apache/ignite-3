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

    protected final FailureProcessor failureProcessor;

    protected final WatchProcessor watchProcessor;

    protected final Object watchProcessorMutex = new Object();

    /**
     * Revision listener for recovery only. Notifies {@link MetaStorageManagerImpl} of current revisions update, {@code null} if recovery
     * is complete.
     */
    private volatile @Nullable RecoveryRevisionsListener recoveryRevisionListener;

    /**
     * Revision. Will be incremented for each single-entry or multi-entry update operation.
     */
    protected volatile long rev;

    /**
     * Last compaction revision that was set or restored from a snapshot.
     *
     * <p>This field is used by metastorage read methods to determine whether {@link CompactedException} should be thrown.</p>
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
     */
    private volatile long plannedUpdateCompactionRevision = -1;

    protected final AtomicBoolean stopCompaction = new AtomicBoolean();

    /** Tracks only cursors. */
    protected final ReadOperationForCompactionTracker readOperationForCompactionTracker;

    /**
     * Events for notification of the {@link WatchProcessor} that were created before the {@link #startWatches start of watches}, after the
     * start of watches there will be {@code null}. Events are sorted by {@link NotifyWatchProcessorEvent#timestamp} and are expected to
     * have no duplicates.
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

    /**
     * Returns key values by revision for operation. Returns {@code null} if there's no value for the given revision.
     */
    protected abstract @Nullable Value valueForOperation(byte[] key, long revision);

    /**
     * Returns {@code true} if the metastorage is in the recovery state.
     */
    private boolean isInRecoveryState() {
        return recoveryRevisionListener != null;
    }

    /**
     * Returns {@code true} if the watches have {@link #startWatches started}.
     */
    protected abstract boolean areWatchesStarted();

    @Override
    public void registerNotificationEnqueuedListener(NotificationEnqueuedListener listener) {
        watchProcessor.registerNotificationEnqueuedListener(listener);
    }

    @Override
    public Entry get(byte[] key) {
        return doGet(key, rev);
    }

    @Override
    public Entry get(byte[] key, long revUpperBound) {
        return doGet(key, revUpperBound);
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys) {
        return doGetAll(keys, rev);
    }

    @Override
    public List<Entry> getAll(List<byte[]> keys, long revUpperBound) {
        return doGetAll(keys, revUpperBound);
    }

    @Override
    public long revision() {
        return rev;
    }

    @Override
    public void saveCompactionRevision(long revision, KeyValueUpdateContext context) {
        assert revision >= 0 : revision;
        assertCompactionRevisionLessThanCurrent(revision, rev);

        saveCompactionRevision(revision, context, true);
    }

    protected abstract void saveCompactionRevision(long compactionRevision, KeyValueUpdateContext context, boolean advanceSafeTime);

    @Override
    public void setCompactionRevision(long revision) {
        assert revision >= 0 : revision;

        assertCompactionRevisionLessThanCurrent(revision, rev);

        compactionRevision = revision;

        notifyRevisionsUpdate();
    }

    @Override
    public long getCompactionRevision() {
        return compactionRevision;
    }

    @Override
    public void updateCompactionRevision(long compactionRevision, KeyValueUpdateContext context) {
        assert compactionRevision >= 0 : compactionRevision;

        assertCompactionRevisionLessThanCurrent(compactionRevision, rev);

        saveCompactionRevision(compactionRevision, context, false);

        if (isInRecoveryState()) {
            setCompactionRevision(compactionRevision);
        } else if (compactionRevision > plannedUpdateCompactionRevision) {
            plannedUpdateCompactionRevision = compactionRevision;

            notifyWatchProcessor(new AdvanceSafeTimeEvent(() -> {
                setCompactionRevision(compactionRevision);

                compactionRevisionUpdateListeners.forEach(listener -> listener.onUpdate(compactionRevision));
            }, context.timestamp));
        } else if (areWatchesStarted()) {
            watchProcessor.advanceSafeTime(() -> {}, context.timestamp);
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
        this.recoveryRevisionListener = listener;
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

    /** Notifies of revision update. */
    protected void notifyRevisionsUpdate() {
        RecoveryRevisionsListener listener = recoveryRevisionListener;

        if (listener != null) {
            // The listener should be invoked only on recovery, after recovery listener will be null.
            // Currently, there is a race that allows the listener to be invoked after recovery is complete, but this race is benign
            // as it will simply lead to a second attempt to complete the recovery future, which will be ignored.
            listener.onUpdate(createCurrentRevisions());
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

        if (value == null) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revUpperBound, compactionRevision);

            return EntryImpl.empty(key);
        }

        if (!isLastIndex(keyRevisions, maxRevisionIndex) || value.tombstone()) {
            CompactedException.throwIfRequestedRevisionLessThanOrEqualToCompacted(revUpperBound, compactionRevision);
        }

        return EntryImpl.toEntry(key, revision, value);
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
        setIndexAndTerm(context.index, context.term);

        if (areWatchesStarted()) {
            watchProcessor.advanceSafeTime(() -> {}, context.timestamp);
        }
    }

    @Override
    public Revisions revisions() {
        return createCurrentRevisions();
    }

    private Revisions createCurrentRevisions() {
        return new Revisions(rev, compactionRevision);
    }

    protected void notifyWatchProcessor(NotifyWatchProcessorEvent event) {
        synchronized (watchProcessorMutex) {
            if (areWatchesStarted()) {
                event.notify(watchProcessor);
            } else {
                boolean added = notifyWatchProcessorEventsBeforeStartingWatches.add(event);

                assert added : event;
            }
        }
    }

    protected void drainNotifyWatchProcessorEventsBeforeStartingWatches() {
        synchronized (watchProcessorMutex) {
            assert !areWatchesStarted();

            notifyWatchProcessorEventsBeforeStartingWatches.forEach(event -> event.notify(watchProcessor));

            notifyWatchProcessorEventsBeforeStartingWatches = null;
        }
    }
}
