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
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.Status;
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

    private IAppendQueue queue;

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

    public static class SharedAppendQueue implements IAppendQueue {
        private final ConcurrentLinkedQueue<IgniteBiTuple<RocksDbSharedLogStorage, StableClosure>> queue = new ConcurrentLinkedQueue<>();

        private volatile boolean broken = false;

        @Override
        public void append(RocksDbSharedLogStorage logStorage, StableClosure done) {
            queue.add(new IgniteBiTuple<>(logStorage, done));
        }

        @Override
        public synchronized boolean flush() {
            if (queue.isEmpty() || broken) {
                // Queue will be empty if called not from flushing stripe.
                return broken;
            }

            while (true) {
                IgniteBiTuple<RocksDbSharedLogStorage, StableClosure> tuple = queue.poll();
                RocksDbSharedLogStorage storage = tuple.get1();
                StableClosure clo = tuple.get2();

                // It doesn't throw. TODO fragile.
                broken = storage.appendEntriesToBatch(clo.getEntries());
                clo.getEntries().clear();

                // Commit batch on last entry.
                if (queue.isEmpty()) {
                    // TODO errors.
                    storage.commitWriteBatch();
                    return broken;
                }
            }
        }
    }

    class SharedAppender implements IAppendBatcher {
        private LogId logId;
        private final List<StableClosure> closures = new ArrayList<>();

        public SharedAppender(List<StableClosure> storages, int cap, LogId lastId) {
            // Ignoring params.
        }

        @Override
        public void append(StableClosure done) {
            // Add to shared queue.
            queue.append(logStorage, done);
            // Update local state.
            logId = done.getEntries().get(done.getEntries().size() - 1).getId();
            closures.add(done);
        }

        @Override
        public LogId flush() {
            if (SharedLogManagerImpl.this.hasError) {
                Status st = new Status(RaftError.EIO, "Corrupted LogStorage");
                for (StableClosure closure : closures) {
                    closure.run(st);
                }

                closures.clear();
                return logId;
            }

            if (!queue.flush()) {
                reportError(RaftError.EIO.getNumber(), "Fail to append log entries");
            }

            Status st;
            if (SharedLogManagerImpl.this.hasError) {
                // TODO dump corrupted info.
                st = new Status(RaftError.EIO, "Corrupted LogStorage");
            }
            else {
                st = Status.OK();
            }

            for (StableClosure closure : closures) {
                closure.run(st);
            }

            closures.clear();

            // In case of error do not adjust disk id.
            return st.isOk() ? logId : null;
        }
    }

    @Override
    protected IAppendBatcher newAppendBatcher(List<StableClosure> storages, int cap, LogId diskId) {
        return new SharedAppender(storages, cap, diskId);
    }

    public interface IAppendQueue {
        void append(RocksDbSharedLogStorage sharedLogManager, StableClosure done);

        boolean flush();
    }
}
