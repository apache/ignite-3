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

package org.apache.ignite.raft.jraft.storage.impl;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryEncoder;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.util.BytesUtil;
import org.apache.ignite.raft.jraft.util.Requires;
import org.apache.ignite.raft.jraft.util.Utils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Slice;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * {@link Logs} implementation that stores logs spilt out to disk in RocksDB. It shares rocksdb instance with other similar instances.
 *
 * <p>Stores key with groupId prefix to distinguish them from keys that belongs to other storages.
 *
 * <p>The data stored by this class is treated as volatile. No flush is done; when the Ignite instance is restarted,
 * the contents of the corresponding RocksDB database should be erased.
 */
public class RocksDbSpillout implements Logs {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(RocksDbSpillout.class);

    static {
        RocksDB.loadLibrary();
    }

    /**
     * VarHandle that gives the access to the elements of a {@code byte[]} array viewed as if it was a {@code long[]}
     * array.
     */
    private static final VarHandle LONG_ARRAY_HANDLE = MethodHandles.byteArrayViewVarHandle(
            long[].class,
            ByteOrder.BIG_ENDIAN
    );

    /** Shared db instance. */
    private final RocksDB db;

    /** Shared data column family handle. */
    private final ColumnFamilyHandle columnFamily;

    /** Write options. */
    private final WriteOptions writeOptions;

    /** Start prefix. */
    private final byte[] groupStartPrefix;

    /** End prefix. */
    private final byte[] groupEndPrefix;

    /** Raft group start bound. */
    private final Slice groupStartBound;

    /** Raft group end bound. */
    private final Slice groupEndBound;

    /** RW lock. */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /** Storage use lock. Non-exclusive. */
    private final Lock useLock = this.readWriteLock.readLock();

    /** Storage manage lock. Exclusive. */
    private final Lock manageLock = this.readWriteLock.writeLock();

    /** Flag indicating whether storage is stopped. Guarded by readWriteLock. */
    private boolean stopped = false;

    /** Executor that handles prefix truncation. */
    private final Executor executor;

    /** Log entry encoder. */
    private LogEntryEncoder logEntryEncoder;

    /** Log entry decoder. */
    private LogEntryDecoder logEntryDecoder;

    /** First log index. */
    private volatile long firstLogIndex = 1;

    private volatile long lastLogIndex = 0;

    /** Constructor. */
    public RocksDbSpillout(
            RocksDB db,
            ColumnFamilyHandle columnFamily,
            String groupId,
            Executor executor
    ) {
        Requires.requireNonNull(db);
        Requires.requireNonNull(columnFamily);
        Requires.requireNonNull(executor);

        Requires.requireTrue(
                groupId.indexOf(0) == -1,
                "Raft group id " + groupId + " must not contain char(0)"
        );
        Requires.requireTrue(
                groupId.indexOf(1) == -1,
                "Raft group id " + groupId + " must not contain char(1)"
        );

        this.db = db;
        this.columnFamily = columnFamily;
        this.executor = executor;
        this.groupStartPrefix = (groupId + (char) 0).getBytes(StandardCharsets.UTF_8);
        this.groupEndPrefix = (groupId + (char) 1).getBytes(StandardCharsets.UTF_8);
        this.groupStartBound = new Slice(groupStartPrefix);
        this.groupEndBound = new Slice(groupEndPrefix);

        this.writeOptions = new WriteOptions();
        this.writeOptions.setDisableWAL(true);
        this.writeOptions.setSync(false);
    }

    @Override
    public boolean init(LogStorageOptions opts) {
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");

        this.manageLock.lock();
        try {
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();

            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");

            doInit();

            return true;
        } finally {
            this.manageLock.unlock();
        }
    }

    private void doInit() {
        this.firstLogIndex = 1;
        this.lastLogIndex = 0;
    }

    private void setFirstLogIndex(long index) {
        this.firstLogIndex = index;
    }

    @Override
    public void shutdown() {
        this.manageLock.lock();

        try {
            if (stopped) {
                return;
            }

            stopped = true;

            deleteWholeGroupRange();

            closeResources();
        } catch (RocksDBException e) {
            throw new LogStorageException("Cannot remove group keys", e);
        } finally {
            this.manageLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(long index) {
        this.useLock.lock();
        try {
            if (index < this.firstLogIndex || index > this.lastLogIndex) {
                return null;
            }

            byte[] keyBytes = createKey(index);
            byte[] bs = getValueFromRocksDb(keyBytes);

            if (bs != null) {
                LogEntry entry = this.logEntryDecoder.decode(bs);
                if (entry != null) {
                    return entry;
                } else {
                    LOG.error("Bad log entry format for index={}, the log data is: {}.", index, BytesUtil.toHex(bs));
                    // invalid data remove? TODO https://issues.apache.org/jira/browse/IGNITE-14832
                    return null;
                }
            }
        } catch (RocksDBException e) {
            LOG.error("Fail to get log entry at index {}.", e, index);
        } finally {
            this.useLock.unlock();
        }
        return null;
    }

    protected byte[] getValueFromRocksDb(byte[] keyBytes) throws RocksDBException {
        return this.db.get(this.columnFamily, keyBytes);
    }

    @Override
    public void appendEntry(LogEntry entry) {
        this.useLock.lock();
        try {
            if (stopped) {
                LOG.warn("Storage stopped.");
                return;
            }

            long logIndex = entry.getId().getIndex();
            byte[] valueBytes = this.logEntryEncoder.encode(entry);
            this.db.put(this.columnFamily, this.writeOptions, createKey(logIndex), valueBytes);
        } catch (RocksDBException e) {
            LOG.error("Fail to append entry.", e);
            throw new LogStorageException("Fail to append entry", e);
        } finally {
            this.useLock.unlock();
        }
    }

    @Override
    public void appendEntries(List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return;
        }

        executeBatch(batch -> {
            for (LogEntry entry : entries) {
                addDataBatch(entry, batch);
            }
        });
    }

    @Override
    public void truncateSuffix(long lastIndexKept) {
        this.useLock.lock();
        try {
            this.db.deleteRange(this.columnFamily, this.writeOptions, createKey(lastIndexKept + 1),
                    createKey(this.lastLogIndex + 1));
        } catch (RocksDBException e) {
            LOG.error("Fail to truncateSuffix {}.", e, lastIndexKept);
            throw new LogStorageException("Fail to truncateSuffix " + lastLogIndex, e);
        } finally {
            this.useLock.unlock();
        }
    }

    @Override
    public void reset() {
        this.manageLock.lock();

        try {
            deleteWholeGroupRange();
        } catch (RocksDBException e) {
            LOG.error("Fail to reset next log index.", e);
            throw new LogStorageException("Fail to reset next log index.", e);
        } finally {
            this.manageLock.unlock();
        }

        doInit();
    }

    private void deleteWholeGroupRange() throws RocksDBException {
        db.deleteRange(columnFamily, groupStartPrefix, groupEndPrefix);
    }

    @Override
    public void truncatePrefix(long firstIndexKept) {
        this.useLock.lock();
        try {
            long startIndex = this.firstLogIndex;
            setFirstLogIndex(firstIndexKept);

            truncatePrefixInBackground(startIndex, firstIndexKept);
        } finally {
            this.useLock.unlock();
        }
    }

    /**
     * Execute write batch template.
     *
     * @param template write batch template
     */
    private void executeBatch(WriteBatchTemplate template) {
        this.useLock.lock();

        try (WriteBatch batch = new WriteBatch()) {
            if (stopped) {
                LOG.warn("Storage stopped.");
                return;
            }

            template.execute(batch);
            this.db.write(this.writeOptions, batch);
        } catch (RocksDBException e) {
            LOG.error("Execute batch failed with rocksdb exception.", e);
            throw new LogStorageException("Execute batch failed with rocksdb exception.", e);
        } catch (IOException e) {
            LOG.error("Execute batch failed with io exception.", e);
            throw new LogStorageException("Execute batch failed with io exception.", e);
        } finally {
            this.useLock.unlock();
        }
    }

    private void addDataBatch(LogEntry entry, WriteBatch batch) throws RocksDBException {
        long logIndex = entry.getId().getIndex();
        byte[] content = this.logEntryEncoder.encode(entry);
        batch.put(this.columnFamily, createKey(logIndex), content);
    }

    private void truncatePrefixInBackground(long startIndex, long firstIndexKept) {
        // delete logs in background.
        Utils.runInThread(executor, () -> {
            this.useLock.lock();
            try {
                if (stopped) {
                    return;
                }

                byte[] startKey = createKey(startIndex);
                byte[] endKey = createKey(firstIndexKept);
                this.db.deleteRange(this.columnFamily, startKey, endKey);
            } catch (RocksDBException e) {
                LOG.error("Fail to truncatePrefix {}.", e, firstIndexKept);
            } finally {
                this.useLock.unlock();
            }
        });
    }

    /**
     * Called upon closing the storage.
     */
    private void closeResources() {
        writeOptions.close();
        groupEndBound.close();
        groupStartBound.close();
    }

    private byte[] createKey(long index) {
        byte[] ks = new byte[groupStartPrefix.length + Long.BYTES];
        System.arraycopy(groupStartPrefix, 0, ks, 0, groupStartPrefix.length);
        LONG_ARRAY_HANDLE.set(ks, groupStartPrefix.length, index);
        return ks;
    }

    /**
     * Write batch template.
     */
    private interface WriteBatchTemplate {
        void execute(WriteBatch batch) throws RocksDBException, IOException;
    }
}
