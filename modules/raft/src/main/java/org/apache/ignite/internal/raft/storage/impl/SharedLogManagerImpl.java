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
package org.apache.ignite.internal.raft.storage.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteStripedReadWriteLock;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.disruptor.DisruptorEventSourceType;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.LogManagerOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl;

/**
 * LogManager implementation.
 */
public class SharedLogManagerImpl extends LogManagerImpl {
    private static final IgniteLogger LOG = Loggers.forClass(SharedLogManagerImpl.class);

    private SharedAppendQueue queue;

    /** Log storage instance. */
    private RocksDbSharedLogStorage logStorage;

    @Override
    public boolean init(LogManagerOptions opts) {
        boolean state = super.init(opts);
        LogStorage logStorage = opts.getLogStorage();

        assert logStorage instanceof RocksDbSharedLogStorage;
        this.logStorage = (RocksDbSharedLogStorage) logStorage;
        queue = opts.getAppendQueue();

        return state;
    }

    public static class SharedAppendQueue extends AbstractSharedAppendQueue<IgniteBiTuple<RocksDbSharedLogStorage, StableClosure>> {
        @Override
        protected final void doFlush(Queue<IgniteBiTuple<RocksDbSharedLogStorage, StableClosure>> queue0, CompletableFuture<Void> fut) {
            boolean valid = true;

            while (true) {
                IgniteBiTuple<RocksDbSharedLogStorage, StableClosure> tuple = queue0.poll();
                RocksDbSharedLogStorage storage = tuple.get1();
                StableClosure clo = tuple.get2();

                // It doesn't throw. TODO fragile.
                if (valid) {
                    valid = storage.appendEntriesToBatch(clo.getEntries());
                }

                if (queue0.isEmpty()) {
                    if (!valid) {
                        fut.completeExceptionally(new Exception("Flush failed"));
                    } else {
                        valid = storage.commitWriteBatch2();
                        fut.complete(null);
                    }
                    return;
                }
            }
        }
    }

    public abstract static class AbstractSharedAppendQueue<T> implements IAppendQueue<T> {
        // TODO we can use another structure to avoid contention on queue tail.
        private volatile ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();

        private final AtomicReference<FlushInfo> flushFut =
                new AtomicReference<>(new FlushInfo(0, CompletableFutures.nullCompletedFuture(), CompletableFutures.nullCompletedFuture()));

        // TODO striped lock.
        private final IgniteStripedReadWriteLock lock = new IgniteStripedReadWriteLock();

        @Override
        public void append(T entry) {
            // Append is called from concurrent threads.
            lock.readLock().lock();

            try {
                queue.add(entry);
            } finally {
                lock.readLock().unlock();
            }
        }

        static class FlushInfo {
            private final CompletableFuture<Void> waitFut;
            private final long generation;
            private final CompletableFuture<Void> flushFut;

            public FlushInfo(long generation, CompletableFuture<Void> parentFut, CompletableFuture<Void> flushFut) {
                this.generation = generation;
                this.flushFut = flushFut;
                this.waitFut = CompletableFuture.allOf(parentFut, flushFut);
            }
        }

        @Override
        public CompletableFuture<Void> flushAsync() {
            // Flush is called only if a called has something to flush.
            FlushInfo info0 = flushFut.get();
            FlushInfo info1 = new FlushInfo(info0.generation + 1, info0.waitFut, new CompletableFuture<>());

            boolean res = flushFut.compareAndSet(info0, info1);

            if (res) {
                assert info0.generation + 1 == info1.generation : "Failed ordering";

                ConcurrentLinkedQueue<T> queue0;

                // Atomically replace queue if choosen as flush coordinator.
                lock.writeLock().lock();

                try {
                    FlushInfo tmp = flushFut.get();
                    // Another thread may increment generation.
                    if (tmp.generation > info1.generation) { // TODO FIXME if many times ?
                        return tmp.waitFut; // It's ok to wait for later generation.
                    } else {
                        assert tmp.generation == info1.generation : "Failed ordering";
                    }

                    queue0 = queue;

                    queue = new ConcurrentLinkedQueue<>(); // TODO FIXME GC pressure ?
                } finally {
                    lock.writeLock().unlock();
                }

                ForkJoinPool.commonPool().execute(() -> doFlush(queue0, info1.flushFut));
                //doFlush(queue0, info1.flushFut);

                return info1.waitFut;
            } else {
                // Re-read
                FlushInfo info2 = flushFut.get();
                assert info2.generation >= info0.generation : "Failed ordering"; // TODO

                // Can wait on current generation because it implies older generations.
                return info2.waitFut;
            }
        }

        protected abstract void doFlush(Queue<T> queue0, CompletableFuture<Void> fut);
    }

    class SharedAppender implements IAppendBatcher {
        private final List<StableClosure> closures = new ArrayList<>();

        public SharedAppender(List<StableClosure> storages, int cap, LogId lastId) {
            // Ignoring params.
        }

        @Override
        public void append(StableClosure done) {
            // Add to shared queue.
            queue.append(new IgniteBiTuple<>(logStorage, done));
            closures.add(done);
        }

        @Override
        public LogId flush() {
            if (closures.isEmpty()) {
                return null;
            }

            queue.flushAsync().join(); // TODO handle errors

            for (StableClosure closure : closures) {
                closure.run(Status.OK()); // TODO handle errors.
                closure.getEntries().clear();
            }

            StableClosure clo = closures.get(closures.size() - 1);
            closures.clear();
            return clo.getEntries().get(clo.getEntries().size() - 1).getId();
        }

        @Override
        public void flushAsync() {
            assert !closures.isEmpty(); // Async flush never called with empty queue.

            var clos = new ArrayList<>(closures);
            closures.clear(); // New closures will be accumulated while async flush is in progress.

            var fut = queue.flushAsync();
            fut.whenComplete((r, e) -> {
                if (e != null) {
                    reportError(RaftError.EIO.getNumber(), "Fail to append log entries");
                }

                for (StableClosure clo : clos) {
                    diskQueue.publishEvent((event, sequence) -> {
                        event.reset();

                        event.setSrcType(DisruptorEventSourceType.LOG);
                        event.setNodeId(SharedLogManagerImpl.this.nodeId);
                        event.setEventType(EventType.OTHER);
                        event.setFlush(Boolean.TRUE);
                        event.setDone(clo);
                    });
                }
            });
        }
    }

    @Override
    protected IAppendBatcher newAppendBatcher(List<StableClosure> storages, int cap, LogId diskId) {
        return new SharedAppender(storages, cap, diskId);
    }

    public interface IAppendQueue<T> {
        void append(T entry);

        CompletableFuture<Void> flushAsync();
    }
}
