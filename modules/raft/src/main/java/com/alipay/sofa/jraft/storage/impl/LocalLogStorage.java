package com.alipay.sofa.jraft.storage.impl;

import com.alipay.sofa.jraft.entity.EnumOutter;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.entity.LogId;
import com.alipay.sofa.jraft.entity.codec.LogEntryDecoder;
import com.alipay.sofa.jraft.entity.codec.LogEntryEncoder;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.LogStorage;
import com.alipay.sofa.jraft.util.Describer;
import com.alipay.sofa.jraft.util.Requires;
import java.util.List;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores log in heap.
 * <p>
 * TODO asch can use SegmentList.
 */
public class LocalLogStorage implements LogStorage, Describer {
    private static final Logger LOG = LoggerFactory.getLogger(LocalLogStorage.class);

    private final String path;
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readLock = this.readWriteLock.readLock();
    private final Lock writeLock = this.readWriteLock.writeLock();

    private final ConcurrentSkipListMap<Long, LogEntry> log = new ConcurrentSkipListMap<>();

    private LogEntryEncoder logEntryEncoder;
    private LogEntryDecoder logEntryDecoder;

    private volatile long firstLogIndex = 1;
    private volatile long lastLogIndex = 0;

    private volatile boolean initialized = false;

    public LocalLogStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
    }

    @Override
    public boolean init(final LogStorageOptions opts) {
        Requires.requireNonNull(opts.getConfigurationManager(), "Null conf manager");
        Requires.requireNonNull(opts.getLogEntryCodecFactory(), "Null log entry codec factory");
        this.writeLock.lock();
        try {
            if (initialized) {
                LOG.warn("RocksDBLogStorage init() already.");
                return true;
            }
            this.initialized = true;
            this.logEntryDecoder = opts.getLogEntryCodecFactory().decoder();
            this.logEntryEncoder = opts.getLogEntryCodecFactory().encoder();
            Requires.requireNonNull(this.logEntryDecoder, "Null log entry decoder");
            Requires.requireNonNull(this.logEntryEncoder, "Null log entry encoder");

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
            this.log.clear();
            LOG.info("DB destroyed, the db path is: {}.", this.path);
        } finally {
            this.writeLock.unlock();
        }
    }

//    private void closeDB() {
//        this.confHandle.close();
//        this.defaultHandle.close();
//        this.db.close();
//    }

    @Override
    public long getFirstLogIndex() {
        this.readLock.lock();
        try {
//            if (this.hasLoadFirstLogIndex) {
//                return this.firstLogIndex;
//            }
//            checkState();
//            it = this.db.newIterator(this.defaultHandle, this.totalOrderReadOptions);
//            it.seekToFirst();
//            if (it.isValid()) {
//                final long ret = Bits.getLong(it.key(), 0);
//                saveFirstLogIndex(ret);
//                setFirstLogIndex(ret);
//                return ret;
//            }
            return this.firstLogIndex;
        } finally {
//            if (it != null) {
//                it.close();
//            }
            this.readLock.unlock();
        }
    }

    @Override
    public long getLastLogIndex() {
        this.readLock.lock();
        //checkState();
        try {
//            it.seekToLast();
//            if (it.isValid()) {
//                return Bits.getLong(it.key(), 0);
//            }


            return this.lastLogIndex;
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (index < getFirstLogIndex()) {
                return null;
            }

            return log.get(index);
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

            this.log.put(entry.getId().getIndex(), entry);

            lastLogIndex = log.lastKey();
            firstLogIndex = log.firstKey();

            return true;
        } finally {
            this.readLock.unlock();
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

            for (LogEntry logEntry : entries) {
                log.put(logEntry.getId().getIndex(), logEntry);
            }

            lastLogIndex = log.lastKey();
            firstLogIndex = log.firstKey();

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
            ConcurrentNavigableMap<Long, LogEntry> map = log.headMap(firstIndexKept);

            map.clear();

            firstLogIndex = log.isEmpty() ? 1 : log.firstKey();

            return true;
        } finally {
            this.readLock.unlock();
        }

    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.readLock.lock();
        try {
            ConcurrentNavigableMap<Long, LogEntry> suffix = log.tailMap(lastIndexKept, false);

            suffix.clear();

            lastLogIndex = log.isEmpty() ? 0 : log.lastKey();

            return true;
        } catch (Exception e) {
            LOG.error("Fail to truncateSuffix {}.", lastIndexKept, e);
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

            log.clear();
            firstLogIndex = 1;
            lastLogIndex = 0;

            if (entry == null) {
                entry = new LogEntry();
                entry.setType(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(nextLogIndex, 0));
                LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
            }

            return appendEntry(entry);
        } finally {
            this.writeLock.unlock();
        }
    }

    @Override
    public void describe(final Printer out) {
        this.readLock.lock();
        try {
            // TODO
        } catch (final Exception e) {
            out.println(e);
        } finally {
            this.readLock.unlock();
        }
    }
}
