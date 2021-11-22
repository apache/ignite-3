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

package org.apache.ignite.internal.metastorage.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Flow;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.metastorage.common.OperationType;
import org.apache.ignite.internal.metastorage.common.command.ConditionInfo;
import org.apache.ignite.internal.metastorage.common.command.GetAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndPutCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.GetAndRemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.GetCommand;
import org.apache.ignite.internal.metastorage.common.command.InvokeCommand;
import org.apache.ignite.internal.metastorage.common.command.MultipleEntryResponse;
import org.apache.ignite.internal.metastorage.common.command.OperationInfo;
import org.apache.ignite.internal.metastorage.common.command.PutAllCommand;
import org.apache.ignite.internal.metastorage.common.command.PutCommand;
import org.apache.ignite.internal.metastorage.common.command.RangeCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveAllCommand;
import org.apache.ignite.internal.metastorage.common.command.RemoveCommand;
import org.apache.ignite.internal.metastorage.common.command.SingleEntryResponse;
import org.apache.ignite.internal.metastorage.common.command.WatchExactKeysCommand;
import org.apache.ignite.internal.metastorage.common.command.WatchRangeKeysCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorCloseCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorHasNextCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorNextCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorsCloseCommand;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.raft.client.scan.ScanRetrieveBatchCommand;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link MetaStorageService} implementation.
 */
public class MetaStorageServiceImpl implements MetaStorageService {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(MetaStorageServiceImpl.class);

    /** IgniteUuid generator. */
    private static final IgniteUuidGenerator UUID_GENERATOR = new IgniteUuidGenerator(UUID.randomUUID(), 0);

    /** Meta storage raft group service. */
    private final RaftGroupService metaStorageRaftGrpSvc;

    /** Local node id. */
    private final String localNodeId;

    /**
     * Constructor.
     *
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     * @param localNodeId           Local node id.
     */
    public MetaStorageServiceImpl(RaftGroupService metaStorageRaftGrpSvc, String localNodeId) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGrpSvc;
        this.localNodeId = localNodeId;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key) {
        return metaStorageRaftGrpSvc.run(new GetCommand(key)).thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Entry> get(@NotNull ByteArray key, long revUpperBound) {
        return metaStorageRaftGrpSvc.run(new GetCommand(key, revUpperBound))
                .thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys) {
        return metaStorageRaftGrpSvc.run(new GetAllCommand(keys))
                .thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAll(Set<ByteArray> keys, long revUpperBound) {
        return metaStorageRaftGrpSvc.run(new GetAllCommand(keys, revUpperBound)).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> put(@NotNull ByteArray key, @NotNull byte[] value) {
        return metaStorageRaftGrpSvc.run(new PutCommand(key, value));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Entry> getAndPut(@NotNull ByteArray key, @NotNull byte[] value) {
        return metaStorageRaftGrpSvc.run(new GetAndPutCommand(key, value))
                .thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals) {
        return metaStorageRaftGrpSvc.run(new PutAllCommand(vals));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndPutAll(@NotNull Map<ByteArray, byte[]> vals) {
        return metaStorageRaftGrpSvc.run(new GetAndPutAllCommand(vals)).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> remove(@NotNull ByteArray key) {
        return metaStorageRaftGrpSvc.run(new RemoveCommand(key));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Entry> getAndRemove(@NotNull ByteArray key) {
        return metaStorageRaftGrpSvc.run(new GetAndRemoveCommand(key))
                .thenApply(MetaStorageServiceImpl::singleEntryResult);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> removeAll(@NotNull Set<ByteArray> keys) {
        return metaStorageRaftGrpSvc.run(new RemoveAllCommand(keys));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Map<ByteArray, Entry>> getAndRemoveAll(@NotNull Set<ByteArray> keys) {
        return metaStorageRaftGrpSvc.run(new GetAndRemoveAllCommand(keys)).thenApply(MetaStorageServiceImpl::multipleEntryResult);
    }

    @Override
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition condition,
            @NotNull Operation success,
            @NotNull Operation failure
    ) {
        return invoke(condition, List.of(success), List.of(failure));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Boolean> invoke(
            @NotNull Condition condition,
            @NotNull Collection<Operation> success,
            @NotNull Collection<Operation> failure
    ) {
        ConditionInfo cond = toConditionInfo(condition);

        List<OperationInfo> successOps = toOperationInfos(success);

        List<OperationInfo> failureOps = toOperationInfos(failure);

        return metaStorageRaftGrpSvc.run(new InvokeCommand(cond, successOps, failureOps));
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Publisher<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo, long revUpperBound) {
        return new RangePublisher(keyFrom, keyTo, revUpperBound);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Publisher<Entry> range(@NotNull ByteArray keyFrom, @Nullable ByteArray keyTo) {
        return new RangePublisher(keyFrom, keyTo, -1);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Flow.Publisher<WatchEvent> watch(
            @Nullable ByteArray keyFrom,
            @Nullable ByteArray keyTo,
            long revision
    ) {
        return new WatchEventsPublisher(keyFrom, keyTo, revision);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Flow.Publisher<WatchEvent> watch(
            @NotNull ByteArray key,
            long revision
    ) {
        return watch(key, null, revision);
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull Flow.Publisher<WatchEvent> watch(
            @NotNull Set<ByteArray> keys,
            long revision
    ) {
        return new WatchEventsPublisher(keys, revision);
    }

    // TODO: IGNITE-14734 Implement.

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> compact() {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> closeCursors(@NotNull String nodeId) {
        return metaStorageRaftGrpSvc.run(new CursorsCloseCommand(nodeId));
    }

    private static List<OperationInfo> toOperationInfos(Collection<Operation> ops) {
        List<OperationInfo> res = new ArrayList<>(ops.size());

        for (Operation op : ops) {
            OperationInfo info = null;

            if (op.type() == OperationType.NO_OP) {
                info = new OperationInfo(null, null, OperationType.NO_OP);
            } else if (op.type() == OperationType.REMOVE) {
                info = new OperationInfo(((Operation.RemoveOp) op.inner()).key(), null, OperationType.REMOVE);
            } else if (op.type() == OperationType.PUT) {
                Operation.PutOp inner = (Operation.PutOp) op.inner();

                info = new OperationInfo(inner.key(), inner.value(), OperationType.PUT);
            } else {
                assert false : "Unknown operation type " + op.type();
            }

            res.add(info);
        }

        return res;
    }

    private static ConditionInfo toConditionInfo(@NotNull Condition condition) {
        ConditionInfo cnd = null;

        Object obj = condition.inner();

        if (obj instanceof Condition.ExistenceCondition) {
            Condition.ExistenceCondition inner = (Condition.ExistenceCondition) obj;

            cnd = new ConditionInfo(inner.key(), inner.type(), null, 0);
        } else if (obj instanceof Condition.TombstoneCondition) {
            Condition.TombstoneCondition inner = (Condition.TombstoneCondition) obj;

            cnd = new ConditionInfo(inner.key(), inner.type(), null, 0);
        } else if (obj instanceof Condition.RevisionCondition) {
            Condition.RevisionCondition inner = (Condition.RevisionCondition) obj;

            cnd = new ConditionInfo(inner.key(), inner.type(), null, inner.revision());
        } else if (obj instanceof Condition.ValueCondition) {
            Condition.ValueCondition inner = (Condition.ValueCondition) obj;

            cnd = new ConditionInfo(inner.key(), inner.type(), inner.value(), 0);
        } else {
            assert false : "Unknown condition type: " + obj.getClass().getSimpleName();
        }

        return cnd;
    }

    private static Map<ByteArray, Entry> multipleEntryResult(Object obj) {
        MultipleEntryResponse resp = (MultipleEntryResponse) obj;

        Map<ByteArray, Entry> res = new HashMap<>();

        for (SingleEntryResponse e : resp.entries()) {
            ByteArray key = new ByteArray(e.key());

            res.put(key, new EntryImpl(key, e.value(), e.revision(), e.updateCounter()));
        }

        return res;
    }

    private static Entry singleEntryResult(Object obj) {
        SingleEntryResponse resp = (SingleEntryResponse) obj;

        return new EntryImpl(new ByteArray(resp.key()), resp.value(), resp.revision(), resp.updateCounter());
    }

    private static WatchEvent watchResponse(Object obj) {
        MultipleEntryResponse resp = (MultipleEntryResponse) obj;

        List<EntryEvent> evts = new ArrayList<>(resp.entries().size() / 2);

        Entry o = null;
        Entry n;

        for (int i = 0; i < resp.entries().size(); i++) {
            SingleEntryResponse s = resp.entries().get(i);

            EntryImpl e = new EntryImpl(new ByteArray(s.key()), s.value(), s.revision(), s.updateCounter());

            if (i % 2 == 0) {
                o = e;
            } else {
                n = e;

                evts.add(new EntryEvent(o, n));
            }
        }

        return new WatchEvent(evts);
    }
    
    /**
     * Watch events publisher.
     */
    private class WatchEventsPublisher implements Flow.Publisher<WatchEvent> {
        /** Flag that denotes that there's an active subscription. */
        private final AtomicBoolean subscribed;

        /** Start key of range (inclusive). Could be {@code null}. */
        private final ByteArray keyFrom;
        
        /** End key of range (exclusive). Could be {@code null}. */
        private final ByteArray keyTo;
        
        /** Set of target keys. Couldn't be {@code null} or empty. */
        private final Set<ByteArray> keys;

        /**
         * Start revision inclusive. {@code 0} - all revisions, {@code -1} - latest revision (accordingly to current meta
         * storage state).
         */
        private long revision;
        
        /**
         * The constructor.
         *
         * @param keyFrom  Start key of range (inclusive). Could be {@code null}.
         * @param keyTo    End key of range (exclusive). Could be {@code null}.
         * @param revision Start revision inclusive. {@code 0} - all revisions, {@code -1} - latest revision (accordingly to current meta
         *                 storage state).
         */
        WatchEventsPublisher(
                @Nullable ByteArray keyFrom,
                @Nullable ByteArray keyTo,
                long revision
        ) {
            this.subscribed = new AtomicBoolean(false);
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.keys = null;
            this.revision = revision;
        }

        /**
         * The constructor.
         *
         * @param keys     Set of target keys. Couldn't be {@code null} or empty.
         * @param revision Start revision inclusive. {@code 0} - all revisions, {@code -1} - latest revision (accordingly to current meta
         *                 storage state).
         */
        WatchEventsPublisher(
                @NotNull Set<ByteArray> keys,
                long revision
        ) {
            this.subscribed = new AtomicBoolean(false);
            this.keyFrom = null;
            this.keyTo = null;
            this.keys = Collections.unmodifiableSet(keys);
            this.revision = revision;
        }
        
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void subscribe(Flow.Subscriber<? super WatchEvent> subscriber) {
            if (subscriber == null) {
                throw new NullPointerException("Subscriber is null");
            }
            
            if (!subscribed.compareAndSet(false, true)) {
                subscriber.onError(new IllegalStateException("Watch events publisher does not support multiple subscriptions."));
            }
    
            WatchEventsSubscription subscription = keys == null
                    ?
                    new WatchEventsSubscription(subscriber, keyFrom, keyTo, revision) :
                    new WatchEventsSubscription(subscriber, keys, revision);

            subscriber.onSubscribe(subscription);
        }
        
        /**
         * Watch events subscription.
         */
        private class WatchEventsSubscription implements Flow.Subscription {
            /** Subscriber. */
            private final Flow.Subscriber<? super WatchEvent> subscriber;
            
            /** Flag that denotes that subscription was canceled. */
            private final AtomicBoolean canceled;

            /** Watch id to uniquely identify it on server side. */
            private final IgniteUuid watchId;

            /** Watch initial operation that created server cursor. */
            private final CompletableFuture<Void> watchInitOp;
    
            /**
             * The constructor.
             *
             * @param subscriber The subscriber.
             * @param keyFrom    Start key of range (inclusive).
             * @param keyTo      End key of range (exclusive).
             * @param revision   Start revision inclusive. {@code 0} - all revisions.
             */
            private WatchEventsSubscription(
                    Flow.Subscriber<? super WatchEvent> subscriber,
                    @Nullable ByteArray keyFrom,
                    @Nullable ByteArray keyTo,
                    long revision
            ) {
                this.subscriber = subscriber;
                this.canceled = new AtomicBoolean(false);
                this.watchId = UUID_GENERATOR.randomUuid();
                this.watchInitOp = metaStorageRaftGrpSvc.run(new WatchRangeKeysCommand(keyFrom, keyTo, revision, localNodeId, watchId));
            }
    
            /**
             * The constructor.
             *
             * @param subscriber The subscriber.
             * @param keys       The keys collection. Couldn't be {@code null}.
             * @param revision   Start revision inclusive. {@code 0} - all revisions.
             */
            private WatchEventsSubscription(
                    Flow.Subscriber<? super WatchEvent> subscriber,
                    @NotNull Set<ByteArray> keys,
                    long revision
            ) {
                this.subscriber = subscriber;
                this.canceled = new AtomicBoolean(false);
                this.watchId = UUID_GENERATOR.randomUuid();
                this.watchInitOp = metaStorageRaftGrpSvc.run(new WatchExactKeysCommand(keys, revision, localNodeId, watchId));
            }
            
            /**
             * {@inheritDoc}
             */
            @Override
            public void request(long n) {
                if (n <= 0) {
                    cancel();
                    
                    subscriber.onError(
                            new IllegalArgumentException(
                                    LoggerMessageHelper.format("Invalid requested amount of items [requested={}, minValue=1]", n))
                    );
                }
                
                // TODO: IGNITE-14691 Back pressure logic should be adjusted after implementing reactive server-side watches.
                assert n == 1 : LoggerMessageHelper.format("Invalid requested amount of watch items [requested={}, expected=1]", n);
                
                watchInitOp.thenRun(this::retrieveNextWatchEvent);
            }
            
            /**
             * {@inheritDoc}
             */
            @Override
            public void cancel() {
                cancel(true);
            }
            
            /**
             * Cancels given subscription and closes cursor if necessary.
             *
             * @param closeCursor If {@code true} closes inner storage scan.
             */
            private void cancel(boolean closeCursor) {
                if (!canceled.compareAndSet(false, true)) {
                    return;
                }
                
                if (closeCursor) {
                    watchInitOp.thenRun(() -> metaStorageRaftGrpSvc.run(new CursorCloseCommand(watchId))).exceptionally(closeT -> {
                        LOG.warn("Unable to close watch.", closeT);
                        
                        return null;
                    });
                }
            }
            
            /**
             * Retrieves next watch event.
             */
            // TODO: IGNITE-14691 Temporary solution, that'll be reworked after implementing proper server-side reactive watches.
            private void retrieveNextWatchEvent() {
                if (canceled.get()) {
                    return;
                }
                
                metaStorageRaftGrpSvc.<Boolean>run(new CursorHasNextCommand(watchId))
                        .thenAccept((hasNext) -> {
                            if (hasNext) {
                                metaStorageRaftGrpSvc.run(new CursorNextCommand(watchId))
                                        .thenAccept((res) -> {
                                                    WatchEvent item = watchResponse(res);
                                                    
                                                    subscriber.onNext(item);
                                                    
                                                }
                                        )
                                        .exceptionally(
                                                t -> {
                                                    cancel(!watchInitOp.isCompletedExceptionally());
    
                                                    if (t instanceof CompletionException) {
                                                        subscriber.onError(t.getCause());
                                                    } else {
                                                        subscriber.onError(t);
                                                    }
                                                    
                                                    return null;
                                                }
                                        );
                            } else {
                                try {
                                    Thread.sleep(10);
                                } catch (InterruptedException e) {
                                    subscriber.onError(e);
                                }
                                retrieveNextWatchEvent();
                            }
                        });
            }
        }
    }
    
    /**
     * Range publisher.
     */
    private class RangePublisher implements Publisher<Entry> {
        /** Start key of range (inclusive). Couldn't be {@code null}. */
        @NotNull
        private final ByteArray keyFrom;
        
        /** End key of range (exclusive). Could be {@code null}. */
        @Nullable
        private final ByteArray keyTo;
        
        /** The upper bound for entry revision. {@code -1} means latest revision. */
        private final long revUpperBound;
    
        /** Flag that denotes that there's an active subscription. */
        private final AtomicBoolean subscribed;
    
        /**
         * The constructor.
         *
         * @param keyFrom Start key of range (inclusive). Couldn't be {@code null}.
         * @param keyTo End key of range (exclusive). Could be {@code null}.
         * @param revUpperBound The upper bound for entry revision. {@code -1} means latest revision.
         */
        RangePublisher(
                @NotNull ByteArray keyFrom,
                @Nullable ByteArray keyTo,
                long revUpperBound
        ) {
            this.keyFrom = keyFrom;
            this.keyTo = keyTo;
            this.revUpperBound = revUpperBound;
            this.subscribed = new AtomicBoolean(false);
        }
        
        /** {@inheritDoc} */
        @Override
        public void subscribe(Flow.Subscriber<? super Entry> subscriber) {
            if (subscriber == null) {
                throw new NullPointerException("Subscriber is null");
            }
            
            if (!subscribed.compareAndSet(false, true)) {
                subscriber.onError(new IllegalStateException("Range publisher does not support multiple subscriptions."));
            }
            
            RangeSubscription subscription = new RangeSubscription(subscriber);
            
            subscriber.onSubscribe(subscription);
        }
        
        /**
         * Partition Scan Subscription.
         */
        private class RangeSubscription implements Subscription {
            /** Subscriber. */
            private final Subscriber<? super Entry> subscriber;
    
            /** Flag that denotes that subscription was canceled. */
            private final AtomicBoolean canceled;
            
            /** Scan id to uniquely identify it on server side. */
            private final IgniteUuid scanId;

            /** Scan initial operation that created server cursor. */
            private final CompletableFuture<Void> scanInitOp;
            
            /**
             * The constructor.
             *
             * @param subscriber The subscriber.
             */
            private RangeSubscription(Subscriber<? super Entry> subscriber) {
                this.subscriber = subscriber;
                this.canceled = new AtomicBoolean(false);
                this.scanId = UUID_GENERATOR.randomUuid();
                this.scanInitOp = revUpperBound == -1
                        ?
                        metaStorageRaftGrpSvc.run(new RangeCommand(keyFrom, keyTo, localNodeId, scanId)) :
                        metaStorageRaftGrpSvc.run(new RangeCommand(keyFrom, keyTo, revUpperBound, localNodeId, scanId));
            }
            
            /** {@inheritDoc} */
            @Override
            public void request(long n) {
                if (n <= 0) {
                    cancel();
                    
                    subscriber.onError(new IllegalArgumentException(LoggerMessageHelper
                            .format("Invalid requested amount of items [requested={}, minValue=1]", n))
                    );
                }
                
                if (canceled.get()) {
                    return;
                }
                
                final int internalBatchSize = Integer.MAX_VALUE;
                
                for (int intBatchCnr = 0; intBatchCnr < (n / internalBatchSize); intBatchCnr++) {
                    if (canceled.get()) {
                        return;
                    }
                    
                    scanBatch(internalBatchSize);
                }
                
                scanBatch((int) (n % internalBatchSize));
            }
    
            /** {@inheritDoc} */
            @Override
            public void cancel() {
                cancel(true);
            }
            
            /**
             * Cancels given subscription and closes cursor if necessary.
             *
             * @param closeCursor If {@code true} closes inner storage scan.
             */
            private void cancel(boolean closeCursor) {
                if (!canceled.compareAndSet(false, true)) {
                    return;
                }
                
                if (closeCursor) {
                    scanInitOp.thenRun(() -> metaStorageRaftGrpSvc.run(new CursorCloseCommand(scanId))).exceptionally(closeT -> {
                        LOG.warn("Unable to close scan.", closeT);
                        
                        return null;
                    });
                }
            }
            
            /**
             * Requests and processes n requested elements where n is an integer.
             *
             * @param n Requested amount of items.
             */
            private void scanBatch(int n) {
                if (canceled.get()) {
                    return;
                }

                scanInitOp.thenRun(() -> {
                    metaStorageRaftGrpSvc.<MultipleEntryResponse>run(new ScanRetrieveBatchCommand(n, scanId))
                            .thenAccept(
                                    res -> {
                                        if (res.entries() == null) {
                                            cancel();

                                            subscriber.onComplete();

                                            return;
                                        } else {
                                            res.entries().forEach(entry -> subscriber.onNext(singleEntryResult(entry)));
                                        }

                                        if (res.entries().size() < n) {
                                            cancel();

                                            subscriber.onComplete();
                                        }
                                    })
                            .exceptionally(
                                    t -> {
                                        cancel(!scanInitOp.isCompletedExceptionally());
    
                                        if (t instanceof CompletionException) {
                                            subscriber.onError(t.getCause());
                                        } else {
                                            subscriber.onError(t);
                                        }

                                        return null;
                                    });
                });
            }
        }
    }
}
