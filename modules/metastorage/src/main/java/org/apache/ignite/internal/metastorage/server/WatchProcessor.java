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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.metastorage.server.raft.MetaStorageWriteHandler.IDEMPOTENT_COMMAND_PREFIX_BYTES;
import static org.apache.ignite.internal.thread.ThreadOperation.NOTHING_ALLOWED;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.DiscardPolicy;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.RevisionUpdateListener;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.timebag.TimeBag;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Class for storing and notifying Meta Storage Watches.
 *
 * <p>Every Meta Storage update is processed by each registered Watch in parallel, however notifications for a single Watch are
 * linearised (Watches are always notified of one event at a time and in increasing order of revisions). It is also guaranteed that Watches
 * will not get notified of a new revision until all Watches have finished processing a previous revision.
 */
public class WatchProcessor implements ManuallyCloseable {
    private final boolean longHandlingLoggingEnabled = getBoolean(IgniteSystemProperties.LONG_HANDLING_LOGGING_ENABLED, false);

    /** Reads an entry from the storage using a given key and revision. */
    @FunctionalInterface
    public interface EntryReader {
        Entry get(byte[] key, long revision);
    }

    private static final IgniteLogger LOG = Loggers.forClass(WatchProcessor.class);

    /**
     * If watch event processing takes more time, than this constant, we will log warning message with some information.
     */
    private static final int WATCH_EVENT_PROCESSING_LOG_THRESHOLD_MILLIS = 100;

    /**
     * The number of keys in log message, that will be printed for long events.
     *
     * @see #WATCH_EVENT_PROCESSING_LOG_THRESHOLD_MILLIS
     */
    private static final int WATCH_EVENT_PROCESSING_LOG_KEYS = 10;

    /** Map that contains Watches and corresponding Watch notification process (represented as a CompletableFuture). */
    private final List<Watch> watches = new CopyOnWriteArrayList<>();

    /**
     * Future that represents the process of notifying registered Watches about a Meta Storage revision.
     *
     * <p>Since Watches are notified concurrently, this future is used to guarantee that no Watches get notified of a new revision,
     * until all Watches have finished processing the previous revision.
     */
    private CompletableFuture<Void> notificationFuture = nullCompletedFuture();

    private final Object notificationFutureMutex = new Object();

    private final List<NotificationEnqueuedListener> notificationEnqueuedListeners = new CopyOnWriteArrayList<>();

    private final EntryReader entryReader;

    private volatile WatchEventHandlingCallback watchEventHandlingCallback;

    // This field is used in assertions only. It was added in order to ease the debug of a tricky problem that is nearly impossible to
    // reproduce.
    private volatile long revision = -1;

    /** Executor for processing watch events. */
    private final ExecutorService watchExecutor;

    /** Meta Storage revision update listeners. */
    private final List<RevisionUpdateListener> revisionUpdateListeners = new CopyOnWriteArrayList<>();

    /** Failure processor that is used to handle critical errors. */
    private final FailureProcessor failureProcessor;

    /**
     * Whether a failure in notification chain was passed to the FailureHandler. Used to make sure that we only pass first such a failure
     * because, as any failure in the chain will stop any notifications, it only makes sense to log the first one. Subsequent ones will
     * be instances of the same original exception.
     */
    private final AtomicBoolean firedFailureOnChain = new AtomicBoolean(false);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    /**
     * Creates a new instance.
     *
     * @param entryReader Function for reading an entry from the storage using a given key and revision.
     */
    public WatchProcessor(String nodeName, EntryReader entryReader, FailureProcessor failureProcessor) {
        this.entryReader = entryReader;

        ThreadFactory threadFactory = IgniteThreadFactory.create(nodeName, "metastorage-watch-executor", LOG, NOTHING_ALLOWED);
        this.watchExecutor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                MILLISECONDS,
                new LinkedBlockingQueue<>(),
                threadFactory,
                // This executor gets shut down during node stop, so we don't care about its tasks being discarded; we would have to
                // filter those RejectedExecutionExceptions by hand anyway.
                new DiscardPolicy()
        );

        this.failureProcessor = failureProcessor;
    }

    /** Adds a watch. */
    public void addWatch(Watch watch) {
        watches.add(watch);
    }

    /** Removes a watch (identified by its listener). */
    void removeWatch(WatchListener listener) {
        watches.removeIf(watch -> watch.listener() == listener);
    }

    /**
     * Returns the minimal target revision of all registered watches.
     */
    public OptionalLong minWatchRevision() {
        return watches.stream()
                .mapToLong(Watch::startRevision)
                .min();
    }

    /** Sets the watch event handling callback. */
    public void setWatchEventHandlingCallback(WatchEventHandlingCallback callback) {
        assert this.watchEventHandlingCallback == null;

        this.watchEventHandlingCallback = callback;
    }

    /** Registers a notification enqueued listener. */
    public void registerNotificationEnqueuedListener(NotificationEnqueuedListener listener) {
        notificationEnqueuedListeners.add(listener);
    }

    private <T> CompletableFuture<T> inBusyLockAsync(Supplier<CompletableFuture<T>> fn) {
        return IgniteUtils.inBusyLockAsync(busyLock, fn);
    }

    private void inBusyLock(Runnable fn) {
        IgniteUtils.inBusyLock(busyLock, fn);
    }

    private void inBusyLockSafe(Runnable fn) {
        IgniteUtils.inBusyLockSafe(busyLock, fn);
    }

    /**
     * Queues the following set of actions that will be executed after the previous invocation of this method completes:
     *
     * <ol>
     *     <li>Notifies all registered watches about the changed entries;</li>
     *     <li>Notifies all registered revision listeners about the new revision;</li>
     *     <li>After all above notifications are processed, notifies about the Safe Time update.</li>
     * </ol>
     *
     * <p>This method is not thread-safe and must be performed under an exclusive lock in concurrent scenarios.
     *
     * @param newRevision Revision associated with an update.
     * @param updatedEntries Entries that were changed during a Meta Storage update, empty if only need to update the revision.
     * @param time Timestamp of the Meta Storage update.
     * @return Future that gets completed when all registered watches have been notified of the given event.
     */
    public CompletableFuture<Void> notifyWatches(long newRevision, List<Entry> updatedEntries, HybridTimestamp time) {
        return inBusyLockAsync(() -> notifyWatchesInternal(newRevision, updatedEntries, time));
    }

    /**
     * Composes passed action with {@link #notificationFuture} and handles any exceptions that might have occurred.
     *
     * @param asyncAction Action to compose.
     * @param additionalInfoSupplier Supplier of additional information that will be used for logging and/or invoking the FailureProcessor.
     * @return Updated value of {@link #notificationFuture}.
     */
    @VisibleForTesting
    CompletableFuture<Void> enqueue(
            Supplier<CompletableFuture<Void>> asyncAction,
            Consumer<CompletableFuture<Void>> afterEnqueuing,
            Supplier<String> additionalInfoSupplier
    ) {
        synchronized (notificationFutureMutex) {
            notificationFuture = notificationFuture
                    .thenComposeAsync(v -> inBusyLockAsync(asyncAction), watchExecutor)
                    .whenComplete((unused, e) -> {
                        if (e != null) {
                            notifyFailureHandlerOnFirstFailureInNotificationChain(e, additionalInfoSupplier);
                        }
                    });

            afterEnqueuing.accept(notificationFuture);

            return notificationFuture;
        }
    }

    private CompletableFuture<Void> notifyWatchesInternal(long newRevision, List<Entry> updatedEntries, HybridTimestamp time) {
        assert time != null;

        Set<Long> revisionsSet = updatedEntries.stream().map(Entry::revision).collect(Collectors.toUnmodifiableSet());
        assert revisionsSet.size() <= 1 : "Update entries are associated with different revisions, revisions=" + revisionsSet;

        List<Entry> filteredUpdatedEntries = updatedEntries.isEmpty() ? emptyList() : updatedEntries.stream()
                .filter(WatchProcessor::isNotIdempotentCacheCommand)
                .collect(toList());

        return enqueue(() -> {
            List<WatchAndEvents> watchAndEvents = collectWatchesAndEvents(filteredUpdatedEntries, newRevision);

            long startTimeNanos = longHandlingLoggingEnabled ? System.nanoTime() : 0;

            CompletableFuture<Void> notifyWatchesFuture = performWatchesNotifications(watchAndEvents, newRevision, time);

            // Revision update is triggered strictly after all watch listeners have been notified.
            CompletableFuture<Void> notifyUpdateRevisionFuture = notifyUpdateRevisionListeners(newRevision);

            CompletableFuture<Void> newNotificationFuture = allOf(notifyWatchesFuture, notifyUpdateRevisionFuture)
                    .thenRunAsync(() -> inBusyLock(() -> invokeOnRevisionCallback(newRevision, time)), watchExecutor);

            newNotificationFuture.whenComplete((u, e) -> maybeLogLongProcessing(filteredUpdatedEntries, watchAndEvents, startTimeNanos));

            return newNotificationFuture;
        }, newNotificationFuture -> {
            invokeNotificationFutureListeners(newNotificationFuture, filteredUpdatedEntries, time);
        }, updatedEntriesKeysInfo(newRevision, updatedEntries));
    }

    private void invokeNotificationFutureListeners(
            CompletableFuture<Void> newNotificationFuture,
            List<Entry> filteredUpdatedEntries,
            HybridTimestamp time
    ) {
        for (NotificationEnqueuedListener listener : notificationEnqueuedListeners) {
            listener.onEnqueued(newNotificationFuture, filteredUpdatedEntries, time);
        }
    }

    private Supplier<String> updatedEntriesKeysInfo(long revision, List<Entry> updatedEntries) {
        return () -> updatedEntries.stream()
                .map(entry -> new String(entry.key(), UTF_8))
                .collect(joining(", ", "Keys of revision: " + revision + " and previsions revision: " + this.revision
                        + "with updated entries: ", ""));
    }

    private static CompletableFuture<Void> performWatchesNotifications(
            List<WatchAndEvents> watchAndEventsList,
            long revision,
            HybridTimestamp time
    ) {
        if (watchAndEventsList.isEmpty()) {
            return nullCompletedFuture();
        }

        CompletableFuture<?>[] notifyWatchFutures = new CompletableFuture[watchAndEventsList.size()];

        for (int i = 0; i < watchAndEventsList.size(); i++) {
            WatchAndEvents watchAndEvents = watchAndEventsList.get(i);

            CompletableFuture<Void> notifyWatchFuture;

            try {
                var event = new WatchEvent(watchAndEvents.events, revision, time, watchAndEvents.timeBag);

                event.timeBag().start();

                notifyWatchFuture = watchAndEvents.watch.onUpdate(event);

                event.timeBag().finishGlobalStage("Sync notification");

                notifyWatchFuture = notifyWatchFuture
                        .whenComplete((unused, e) -> event.timeBag().finishGlobalStage("Async notification"));
            } catch (Throwable throwable) {
                notifyWatchFuture = failedFuture(throwable);
            }

            notifyWatchFutures[i] = notifyWatchFuture;
        }

        return allOf(notifyWatchFutures);
    }

    private void maybeLogLongProcessing(List<Entry> updatedEntries, List<WatchAndEvents> watchAndEvents, long startTimeNanos) {
        if (!longHandlingLoggingEnabled) {
            return;
        }

        long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);

        if (durationMillis > WATCH_EVENT_PROCESSING_LOG_THRESHOLD_MILLIS) {
            String keysHead = updatedEntries.stream()
                    .limit(WATCH_EVENT_PROCESSING_LOG_KEYS)
                    .map(entry -> new String(entry.key(), UTF_8))
                    .collect(joining(", "));

            String keysTail = updatedEntries.size() > WATCH_EVENT_PROCESSING_LOG_KEYS ? ", ..." : "";

            LOG.warn(
                    "Watch event processing has been too long [duration={}, keys=[{}{}]]",
                    durationMillis,
                    keysHead,
                    keysTail
            );

            String timingsHead = watchAndEvents.stream()
                    .limit(WATCH_EVENT_PROCESSING_LOG_KEYS)
                    .map(watchAndEventsItem -> {
                        String listenerName = watchAndEventsItem.watch.listener().getClass().getName();
                        String stages = watchAndEventsItem.timeBag.stagesTimings().stream().collect(joining(",", "[", "]"));

                        return "lsnr=" + listenerName + ", stages=" + stages;
                    }).collect(joining(", "));

            LOG.warn("Watch event processing timings [{}{}]", timingsHead, keysTail);
        }
    }

    private List<WatchAndEvents> collectWatchesAndEvents(List<Entry> updatedEntries, long revision) {
        if (watches.isEmpty() || updatedEntries.isEmpty()) {
            return List.of();
        }

        var watchAndEvents = new ArrayList<WatchAndEvents>();

        for (Watch watch : watches) {
            List<EntryEvent> events = List.of();

            for (Entry newEntry : updatedEntries) {
                byte[] newKey = newEntry.key();

                assert newEntry.revision() == revision;

                if (watch.matches(newKey, revision)) {
                    Entry oldEntry = entryReader.get(newKey, revision - 1);

                    if (events.isEmpty()) {
                        events = new ArrayList<>();
                    }

                    events.add(new EntryEvent(oldEntry, newEntry));
                }
            }

            if (!events.isEmpty()) {
                watchAndEvents.add(new WatchAndEvents(watch, events, TimeBag.createTimeBag(longHandlingLoggingEnabled, false)));
            }
        }

        return watchAndEvents;
    }

    private void invokeOnRevisionCallback(long revision, HybridTimestamp time) {
        watchEventHandlingCallback.onSafeTimeAdvanced(time);

        watchEventHandlingCallback.onRevisionApplied(revision);

        this.revision = revision;
    }

    /**
     * Advances safe time without notifying watches (as there is no new revision).
     *
     * <p>This method is not thread-safe and must be performed under an exclusive lock in concurrent scenarios.
     *
     * @param callback A callback that will be executed in meta-storage watch thread strictly before advancing safe time.
     * @param time Timestamp value for advancing.
     */
    public void advanceSafeTime(Runnable callback, HybridTimestamp time) {
        inBusyLockSafe(() -> advanceSafeTimeInternal(callback, time));
    }

    private void advanceSafeTimeInternal(Runnable callback, HybridTimestamp time) {
        assert time != null;

        enqueue(() -> {
            callback.run();

            watchEventHandlingCallback.onSafeTimeAdvanced(time);

            return nullCompletedFuture();
        }, newNotificationFuture -> {
            invokeNotificationFutureListeners(newNotificationFuture, List.of(), time);
        }, () -> "<nothing>");
    }

    private void notifyFailureHandlerOnFirstFailureInNotificationChain(Throwable e, Supplier<String> additionalInfoSupplier) {
        if (firedFailureOnChain.compareAndSet(false, true)) {
            boolean nodeStopping = hasCause(e, NodeStoppingException.class);

            if (!nodeStopping) {
                LOG.error("Notification chain encountered an error, so no notifications will be ever fired for subsequent revisions "
                        + "until a restart. Notifying the FailureManager. Additional info: '{}'", additionalInfoSupplier.get());

                failureProcessor.process(new FailureContext(CRITICAL_ERROR, e));
            } else {
                LOG.info("Notification chain encountered a NodeStoppingException, so no notifications will be ever fired for "
                        + "subsequent revisions until a restart.");
            }
        }
    }

    @Override
    public void close() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        synchronized (notificationFutureMutex) {
            notificationFuture.completeExceptionally(new NodeStoppingException());
        }

        IgniteUtils.shutdownAndAwaitTermination(watchExecutor, 10, SECONDS);
    }

    /** Registers a Meta Storage revision update listener. */
    void registerRevisionUpdateListener(RevisionUpdateListener listener) {
        revisionUpdateListeners.add(listener);
    }

    /** Unregisters a Meta Storage revision update listener. */
    void unregisterRevisionUpdateListener(RevisionUpdateListener listener) {
        revisionUpdateListeners.remove(listener);
    }

    /** Explicitly notifies revision update listeners. */
    CompletableFuture<Void> notifyUpdateRevisionListeners(long newRevision) {
        // Lazy set.
        List<CompletableFuture<?>> futures = List.of();

        for (RevisionUpdateListener listener : revisionUpdateListeners) {
            if (futures.isEmpty()) {
                futures = new ArrayList<>();
            }

            futures.add(listener.onUpdated(newRevision));
        }

        return futures.isEmpty() ? nullCompletedFuture() : allOf(futures.toArray(CompletableFuture[]::new));
    }

    private static boolean isNotIdempotentCacheCommand(Entry entry) {
        int prefixLength = IDEMPOTENT_COMMAND_PREFIX_BYTES.length;

        //noinspection SimplifiableIfStatement
        if (entry.key().length <= prefixLength) {
            return true;
        }

        return !Arrays.equals(
                entry.key(), 0, prefixLength,
                IDEMPOTENT_COMMAND_PREFIX_BYTES, 0, prefixLength
        );
    }
}
