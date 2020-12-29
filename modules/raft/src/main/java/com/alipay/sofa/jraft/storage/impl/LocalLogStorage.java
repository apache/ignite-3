package com.alipay.sofa.jraft.storage.impl;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.conf.ConfigurationEntry;
import com.alipay.sofa.jraft.conf.ConfigurationManager;
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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stores log in heap.
 *
 * TODO can use SegmentList.
 */
public class LocalLogStorage implements LogStorage, Describer {
    private static final Logger LOG = LoggerFactory.getLogger(LocalLogStorage.class);

    private final String                    path;
    private final boolean                   sync;
    private final boolean                   openStatistics;
    private final ReadWriteLock             readWriteLock = new ReentrantReadWriteLock();
    private final Lock                      readLock      = this.readWriteLock.readLock();
    private final Lock                      writeLock     = this.readWriteLock.writeLock();

    private volatile long                   firstLogIndex = 1;

    private final LinkedList<LogEntry> log = new LinkedList<>();

    private LogEntryEncoder logEntryEncoder;
    private LogEntryDecoder logEntryDecoder;

    private volatile boolean initialized = false;

    public LocalLogStorage(final String path, final RaftOptions raftOptions) {
        super();
        this.path = path;
        this.sync = raftOptions.isSync();
        this.openStatistics = raftOptions.isOpenStatistics();
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
        } catch (final Exception e) {
            LOG.error("Fail to init RocksDBLogStorage, path={}.", this.path, e);
            return false;
        } finally {
            this.writeLock.unlock();
        }
    }

    /**
     * Save the first log index into conf column family.
     */
    private boolean saveFirstLogIndex(final long firstLogIndex) {
        this.readLock.lock();
        try {
//            final byte[] vs = new byte[8];
//            Bits.putLong(vs, 0, firstLogIndex);
//            checkState();
//            this.db.put(this.confHandle, this.writeOptions, FIRST_LOG_IDX_KEY, vs);

            this.firstLogIndex = firstLogIndex;

            return true;
        } catch (final Exception e) {
            LOG.error("Fail to save first log index {}.", firstLogIndex, e);
            return false;
        } finally {
            this.readLock.unlock();
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
        try  {
//            it.seekToLast();
//            if (it.isValid()) {
//                return Bits.getLong(it.key(), 0);
//            }



            return this.firstLogIndex - 1 + this.log.size();
        } finally {
            this.readLock.unlock();
        }
    }

    @Override
    public LogEntry getEntry(final long index) {
        this.readLock.lock();
        try {
            if (index < this.firstLogIndex) {
                return null;
            }

            return log.get((int) (this.firstLogIndex - 1 + this.log.size()));
        } catch (Exception e) {
            LOG.error("Fail to get log entry at index {}.", index, e);
        } finally {
            this.readLock.unlock();
        }
        return null;
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

            this.log.add(entry);

            return true;
        } catch (Exception e) {
            LOG.error("Fail to append entry.", e);
            return false;
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
        try {
            if (!initialized) {
                LOG.warn("DB not initialized or destroyed.");
                return 0;
            }

            this.log.addAll(entries);

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
            final long startIndex = getFirstLogIndex();

            this.firstLogIndex = firstIndexKept;

            for (long i = startIndex; i < firstIndexKept; i++)
                log.pollFirst();

            return true;
        } finally {
            this.readLock.unlock();
        }

    }

    @Override
    public boolean truncateSuffix(final long lastIndexKept) {
        this.readLock.lock();
        try {
            long lastLogIndex = getLastLogIndex();

            while(lastLogIndex-- > lastIndexKept)
                log.pollLast();

            return true;
        } catch (Exception e) {
            LOG.error("Fail to truncateSuffix {}.", lastIndexKept, e);
        } finally {
            this.readLock.unlock();
        }
        return false;
    }

    @Override
    // TOOD it doesn't work.
    public boolean reset(final long nextLogIndex) {
        if (nextLogIndex <= 0) {
            throw new IllegalArgumentException("Invalid next log index.");
        }
        this.writeLock.lock();
        try {
            LogEntry entry = getEntry(nextLogIndex);

            try {
                if (false) { // TODO should read snapshot.
                    if (entry == null) {
                        entry = new LogEntry();
                        entry.setType(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                        entry.setId(new LogId(nextLogIndex, 0));
                        LOG.warn("Entry not found for nextLogIndex {} when reset.", nextLogIndex);
                    }
                    return appendEntry(entry);
                } else {
                    return false;
                }
            } catch (final Exception e) {
                LOG.error("Fail to reset next log index.", e);
                return false;
            }
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
