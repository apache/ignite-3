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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.LogManagerOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.LogManagerImpl;

/**
 * Log manager that enables batch processing of log entries from different partitions within a stripe.
 * <br>
 * Upon each flush of the log, it will also trigger flush on all the other log storages, that belong to the same stripe in
 * corresponding {@link LogManagerOptions#getLogManagerDisruptor()}.
 */
public class StripeAwareLogManager extends LogManagerImpl {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(StripeAwareLogManager.class);

    /** Log storage instance. */
    private LogStorage logStorage;

    /** Stripe, that corresponds to the current log storage instance. */
    private Stripe stripe;

    /** Size threshold of log entries list, that will trigger the flush upon the excess. */
    private int maxAppendBufferSize;

    /**
     * Whether the log storage is a {@link RocksDbSharedLogStorage} or not.
     * It requires special treatment in order to better optimize writes.
     */
    private boolean sharedLogStorage;

    @Override
    public boolean init(LogManagerOptions opts) {
        LogStorage logStorage = opts.getLogStorage();

        this.sharedLogStorage = logStorage instanceof RocksDbSharedLogStorage;
        this.logStorage = logStorage;
        this.maxAppendBufferSize = opts.getRaftOptions().getMaxAppendBufferSize();

        boolean isInitSuccessfully = super.init(opts);

        int stripe = opts.getLogManagerDisruptor().getStripe(opts.getNode().getNodeId());

        assert stripe != -1;

        this.stripe = opts.getLogStripes().get(stripe);

        return isInitSuccessfully;
    }

    /**
     * Regular append to shared storage has been replaced with appending data into a batch. Data will later be "committed" by calling
     * {@link StripeAwareAppendBatcher#commitWriteBatch()} on any of the log instances.
     * The reason why is given in {@link Stripe}'s comments.
     */
    @Override
    protected int appendToLogStorage(List<LogEntry> toAppend) {
        if (sharedLogStorage) {
            return ((RocksDbSharedLogStorage) logStorage).appendEntriesToBatch(toAppend) ? toAppend.size() : 0;
        } else {
            return logStorage.appendEntries(toAppend);
        }
    }

    @Override
    protected AppendBatcher newAppendBatcher(List<StableClosure> storages, int cap, LogId diskId) {
        return new StripeAwareAppendBatcher(storages, cap, diskId);
    }

    /**
     * Append batcher implementation that triggers flush on all log storages, that belong to the same stripe.
     */
    private class StripeAwareAppendBatcher extends AppendBatcher {
        StripeAwareAppendBatcher(List<StableClosure> storage, int cap, LogId lastId) {
            super(storage, cap, new ArrayList<>(), lastId);
        }

        private LogId lastIdCandidate;

        /**
         * Flush is simply delegated to the {@link Stripe}.
         */
        @Override
        protected LogId flush() {
            stripe.flush();

            // Last ID is already updated at this point.
            return lastId;
        }

        /**
         * Delegates to {@link LogManagerImpl#appendToStorage(List)}.
         */
        void appendToStorage() {
            assert size > 0;

            lastIdCandidate = StripeAwareLogManager.super.appendToStorage(toAppend);
        }

        /**
         * Delegates to {@link RocksDbSharedLogStorage#commitWriteBatch()} if it can. No-op otherwise.
         */
        void commitWriteBatch() {
            if (sharedLogStorage) {
                ((RocksDbSharedLogStorage) logStorage).commitWriteBatch();
            }
        }

        /**
         * Delegates to {@link LogManagerImpl#reportError(int, String, Object...)}.
         */
        void reportError(int code, String fmt, Object... args) {
            StripeAwareLogManager.super.reportError(code, fmt, args);
        }

        /**
         * Notifies storage stable closures and clears the batch. Based on the code from {@link AppendBatcher#flush()}.
         */
        void notifyClosures() {
            lastId = lastIdCandidate;

            for (int i = 0; i < this.size; i++) {
                this.storage.get(i).getEntries().clear();
                Status st = null;
                try {
                    if (StripeAwareLogManager.super.hasError) {
                        st = new Status(RaftError.EIO, "Corrupted LogStorage");
                    } else {
                        st = Status.OK();
                    }
                    this.storage.get(i).run(st);
                } catch (Throwable t) {
                    LOG.error("Fail to run closure with status: {}.", t, st);
                }
            }

            toAppend.clear();
            storage.clear();
            size = 0;

            setDiskId(lastId);
        }

        @Override
        protected void append(StableClosure done) {
            if (stripe.size >= cap || stripe.bufferSize >= maxAppendBufferSize) {
                flush();
            }

            // "super.append(done);" will calculate the size of update entries and put that value into "bufferSize".
            // We use it later to add to "stripe.bufferSize".
            bufferSize = 0;
            super.append(done);

            stripe.addBatcher(this, bufferSize);
        }
    }

    /**
     * Special instance, shared between different instances of {@link StripeAwareLogManager}, that correspond to the same stripe in the
     * {@link LogManagerOptions#getLogManagerDisruptor()}.
     * <br>
     * It accumulates data from different {@link AppendBatcher} instances, allowing to flush data from several log storages all at once.
     * <br>
     * Also supports batch log updates for {@link RocksDbSharedLogStorage}.
     */
    public static class Stripe {
        /** Cumulative data size of all data entries, not yet flushed in this stripe. */
        private int bufferSize;

        /** The number of all entry lists added to all {@link AppendBatcher}s, not yet flushed in this stripe. */
        private int size;

        /** This list of all append batchers, that contain data not yet flushed by this stripe. */
        private final Set<StripeAwareAppendBatcher> appendBatchers = new HashSet<>();

        /**
         * Notifies the stripe that there's a new append to one of the append batchers.
         *
         * @param appendBatcher Append batcher that had an append.
         * @param bufferSize The buffer size of that append.
         */
        void addBatcher(StripeAwareAppendBatcher appendBatcher, int bufferSize) {
            appendBatchers.add(appendBatcher);

            size++;
            this.bufferSize += bufferSize;
        }

        /**
         * Performs an composite flush for all log storages that belong to the stripe.
         */
        void flush() {
            if (size == 0) {
                return;
            }

            // At first, all log storages should prepare the data by adding it to the write batch in the log storage factory.
            for (StripeAwareAppendBatcher appendBatcher : appendBatchers) {
                appendBatcher.appendToStorage();
            }

            // Calling "commitWriteBatch" on StripeAwareAppendBatcher is confusing and hacky, but it doesn't require explicit access
            // to the log storage factory, which makes it far easier to use in current jraft code.
            // The reason why we don't call this method on log factory, for example, is because the factory doesn't have proper access
            // to the RAFT configuration, and can't say, whether it should use "fsync" or not, for example.
            try {
                for (StripeAwareAppendBatcher appendBatcher : appendBatchers) {
                    appendBatcher.commitWriteBatch();
                }
            } catch (Exception e) {
                LOG.error("**Critical error**, failed to appendEntries.", e);

                for (StripeAwareAppendBatcher appendBatcher : appendBatchers) {
                    appendBatcher.reportError(RaftError.EIO.getNumber(), "Fail to append log entries");
                }

                return;
            }

            // When data is committed, we can notify all stable closures and send response messages.
            for (StripeAwareAppendBatcher appendBatcher : appendBatchers) {
                appendBatcher.notifyClosures();
            }

            appendBatchers.clear();
            size = 0;
            bufferSize = 0;
        }
    }
}
