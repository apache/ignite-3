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

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.VolatileStorage;
import org.apache.ignite.raft.jraft.util.Describer;
import org.apache.ignite.raft.jraft.util.Requires;

/**
 * Stores RAFT log in memory.
 *
 * <p>Also, supports spilling out to disk if the in-memory storage is overflowed.
 *
 * <p>Current implementation always spills out a prefix of all the logs currently stored by the storage.
 */
public class VolatileLogStorage implements LogStorage, Describer, VolatileStorage {
    private static final IgniteLogger LOG = Loggers.forClass(VolatileLogStorage.class);

    private final LogStorageBudget inMemoryBudget;

    private final Logs inMemoryLogs;
    private final Logs spiltOnDisk;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.readWriteLock.readLock();
    private final Lock writeLock = this.readWriteLock.writeLock();

    private volatile long firstLogIndex = -1;
    private volatile long lastLogIndex = -1;

    private volatile long lastSpiltLogIndex = -1;

    private volatile boolean initialized = false;

    public VolatileLogStorage(LogStorageBudget inMemoryBudget, Logs inMemoryLogs, Logs spiltOnDisk) {
        this.inMemoryBudget = inMemoryBudget;

        this.inMemoryLogs = inMemoryLogs;
        this.spiltOnDisk = spiltOnDisk;
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");

        this.writeLock.lock();

        try {
            if (initialized) {
                LOG.warn("VolatileLogStorage init() was already called.");
                return true;
            }

            inMemoryLogs.init(opts);
            spiltOnDisk.init(opts);

            this.initialized = true;

            return true;
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        this.writeLock.lock();

        try {
            this.initialized = false;

            this.inMemoryLogs.shutdown();
            this.spiltOnDisk.shutdown();
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();

        try {
            return hasAnyEntries() ? this.firstLogIndex : 1;
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean hasAnyEntries() {
        return firstLogIndex != -1;
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();

        try {
            return hasAnyEntries() ? this.lastLogIndex : 0;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();

        try {
            if (!hasAnyEntries() || index < firstLogIndex) {
                return null;
            }

            if (isSomethingSpilt() && index <= lastSpiltLogIndex) {
                return spiltOnDisk.getEntry(index);
            } else {
                return inMemoryLogs.getEntry(index);
            }
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public long getTerm(final long index) {
        final LogEntry entry = getEntry(index);

        if (entry != null) {
            return entry.getId().getTerm();
        }

        return 0;
    }

    @Override
    public boolean appendEntry(final LogEntry entry) {
        this.readLock.lock();

        try {
            if (!initialized) {
                LOG.warn("DB not initialized or destroyed.");
                return false;
            }

            appendEntryInternal(entry);

            return true;
        } finally {
            this.readLock.unlock();
        }
    }

    private void appendEntryInternal(LogEntry entry) {
        final boolean hadAnyEntries = hasAnyEntries();

        spillToDiskUntilEnoughInMemorySpaceIsAvailable(entry);

        long entryIndex = entry.getId().getIndex();

        if (!inMemoryBudget.hasRoomFor(entry)) {
            // We spilt everything we could but the new entry is alone so big that we cannot put it to memory.
            // So spill it immediately.
            spiltOnDisk.appendEntry(entry);

            lastSpiltLogIndex = entryIndex;
        } else {
            inMemoryLogs.appendEntry(entry);
        }

        lastLogIndex = entryIndex;

        if (!hadAnyEntries) {
            firstLogIndex = entryIndex;
        }

        inMemoryBudget.onAppended(entry);
    }

    private void spillToDiskUntilEnoughInMemorySpaceIsAvailable(LogEntry entry) {
        while (!inMemoryBudget.hasRoomFor(entry)) {
            long indexToSpill = isSomethingSpilt() ? lastSpiltLogIndex + 1 : getFirstLogIndex();
            assert indexToSpill >= getFirstLogIndex() : indexToSpill + " must be after " + getFirstLogIndex();

            if (indexToSpill > lastLogIndex) {
                // We spilt everything we could but the new entry is alone so big that we cannot put it to memory.
                break;
            }

            LogEntry entryToSpill = inMemoryLogs.getEntry(indexToSpill);
            assert entryToSpill != null;

            spiltOnDisk.appendEntry(entryToSpill);
            inMemoryLogs.truncatePrefix(indexToSpill + 1);

            inMemoryBudget.onTruncatedPrefix(indexToSpill + 1);

            lastSpiltLogIndex = indexToSpill;
        }
    }

    @Override
    public int appendEntries(final List<LogEntry> entries) {
        if (entries == null || entries.isEmpty()) {
            return 0;
        }

        final int entriesCount = entries.size();

        this.readLock.lock();

        try {
            if (!initialized) {
                LOG.warn("DB not initialized or destroyed.");
                return 0;
            }

            for (LogEntry entry : entries) {
                appendEntryInternal(entry);
            }

            return entriesCount;
        } catch (Exception e) {
            LOG.error("Fail to append entry.", e);
            return 0;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public boolean truncatePrefix(final long firstIndexKept) {
        this.readLock.lock();

        try {
            if (!hasAnyEntries() || firstIndexKept <= firstLogIndex) {
                return true;
            }

            if (isSomethingSpilt()) {
                spiltOnDisk.truncatePrefix(Math.min(firstIndexKept, lastSpiltLogIndex + 1));
            }
            inMemoryLogs.truncatePrefix(firstIndexKept);

            boolean stillNotEmpty = firstIndexKept <= lastLogIndex;
            firstLogIndex = stillNotEmpty ? firstIndexKept : -1;
            lastLogIndex = stillNotEmpty ? lastLogIndex : -1;

            if (isSomethingSpilt() && stillNotEmpty) {
                if (lastSpiltLogIndex < firstIndexKept) {
                    lastSpiltLogIndex = -1;
                }
            } else {
                lastSpiltLogIndex = -1;
            }

            inMemoryBudget.onTruncatedPrefix(firstIndexKept);

            return true;
        } finally {
            this.readLock.unlock();
        }
    }

    private boolean isSomethingSpilt() {
        return lastSpiltLogIndex > 0;
    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.readLock.lock();

        try {
            if (!hasAnyEntries() || lastIndexKept >= lastLogIndex) {
                return true;
            }

            if (isSomethingSpilt() && lastIndexKept < lastSpiltLogIndex) {
                spiltOnDisk.truncateSuffix(lastIndexKept);
                lastSpiltLogIndex = lastIndexKept < firstLogIndex ? -1 : lastIndexKept;
            }
            inMemoryLogs.truncateSuffix(lastIndexKept);

            boolean stillNotEmpty = lastIndexKept >= firstLogIndex;
            firstLogIndex = stillNotEmpty ? firstLogIndex : -1;
            lastLogIndex = stillNotEmpty ? lastIndexKept : -1;

            inMemoryBudget.onTruncatedSuffix(lastIndexKept);

            return true;
        } catch (Exception e) {
            LOG.error("Fail to truncateSuffix {}.", e, lastIndexKept);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    @Override
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }

        this.writeLock.lock();

        try {
            LogEntry entry = getEntry(nextLogIndex);

            inMemoryLogs.reset();
            spiltOnDisk.reset();

            firstLogIndex = -1;
            lastLogIndex = -1;

            if (entry == null) {
                entry = new LogEntry();
                entry.setType(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(nextLogIndex, 0));
                LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
            }

            inMemoryBudget.onReset();

            return appendEntry(entry);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        this.readLock.lock();

        try {
            out.println("firstLogIndex=" + firstLogIndex);
            out.println("lastLogIndex=" + lastLogIndex);
        } catch (final Exception e) {
            out.println(e);
        } finally {
            this.readLock.unlock();
        }
    }
}
