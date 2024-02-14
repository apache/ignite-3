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

package org.apache.ignite.raft.jraft.storage.logit.storage.db;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.Lifecycle;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryDecoder;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.factory.LogStoreFactory;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.AbstractFile;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.AbstractFile.RecoverResult;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.FileHeader;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.FileManager;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.FileType;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.assit.AbortFile;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.assit.FlushStatusCheckpoint;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.segment.SegmentFile;
import org.apache.ignite.raft.jraft.storage.logit.storage.service.ServiceManager;
import org.apache.ignite.raft.jraft.storage.logit.util.Pair;

/**
 * DB parent class that invokes fileManager and anager
 * and wrappers uniform functions such as recover() etc..
 */
public abstract class AbstractDB implements Lifecycle<LogStoreFactory> {
    private static final IgniteLogger LOG = Loggers.forClass(AbstractDB.class);

    private static final String      FLUSH_STATUS_CHECKPOINT = "FlushStatusCheckpoint";
    private static final String      ABORT_FILE              = "Abort";

    protected final String           storePath;
    protected FileManager            fileManager;
    protected ServiceManager         serviceManager;
    protected LogStoreFactory        logStoreFactory;
    protected StoreOptions           storeOptions;
    protected AbortFile              abortFile;
    protected FlushStatusCheckpoint  flushStatusCheckpoint;

    private final ScheduledExecutorService checkpointExecutor;
    private ScheduledFuture<?> checkpointScheduledFuture;

    protected AbstractDB(String storePath, ScheduledExecutorService checkpointExecutor) {
        this.storePath = storePath;
        this.checkpointExecutor = checkpointExecutor;
    }

    @Override
    public boolean init(final LogStoreFactory logStoreFactory) {
        this.logStoreFactory = logStoreFactory;
        this.storeOptions = logStoreFactory.getStoreOptions();
        final String flushStatusCheckpointPath = Paths.get(this.storePath, FLUSH_STATUS_CHECKPOINT).toString();
        final String abortFilePath = Paths.get(this.storePath, ABORT_FILE).toString();
        this.flushStatusCheckpoint = new FlushStatusCheckpoint(flushStatusCheckpointPath, logStoreFactory.getRaftOptions());
        this.abortFile = new AbortFile(abortFilePath);
        this.serviceManager = logStoreFactory.newServiceManager(this);
        if (!this.serviceManager.init(logStoreFactory)) {
            return false;
        }
        this.fileManager = logStoreFactory.newFileManager(getDBFileType(), this.storePath,
            this.serviceManager.getAllocateService());
        final int interval = this.storeOptions.getCheckpointFlushStatusInterval();

        checkpointScheduledFuture =
                this.checkpointExecutor.scheduleAtFixedRate(this::doCheckpoint, interval, interval, TimeUnit.MILLISECONDS);

        return true;
    }

    @Override
    public void shutdown() {
        checkpointScheduledFuture.cancel(false);

        doCheckpoint();
        if (this.serviceManager != null) {
            this.serviceManager.shutdown();
        }
        if (this.fileManager != null) {
            this.fileManager.shutdown();
        }
        if (this.abortFile != null) {
            this.abortFile.destroy();
        }
    }

    /**
     * @return this db's name
     */
    public String getDBName() {
        return getClass().getSimpleName();
    }

    /**
     * @return this db's file type (index or segmentLog or conf)
     */
    public abstract FileType getDBFileType();

    /**
     * @return this db's file size
     */
    public abstract int getDBFileSize();

    /**
     * Log Entry iterator
     */
    public static class LogEntryIterator implements Iterator<LogEntry> {
        private final AbstractFile[]  files;
        private int                   currentReadPos;
        private int                   preReadPos;
        private int                   currentFileId;
        private final LogEntryDecoder logEntryDecoder;

        /**
         *
         * @param files target files
         * @param logEntryDecoder decoder
         * @param currentReadPos the beginning read position in the first file
         */
        public LogEntryIterator(final AbstractFile[] files, final LogEntryDecoder logEntryDecoder,
                                final int currentReadPos) {
            this.files = files;
            this.logEntryDecoder = logEntryDecoder;
            if (files.length > 0) {
                this.currentFileId = 0;
                this.currentReadPos = Math.max(currentReadPos, FileHeader.HEADER_SIZE);
            } else {
                this.currentFileId = -1;
                this.currentReadPos = -1;
            }
        }

        @Override
        public boolean hasNext() {
            return this.currentFileId >= 0 && this.currentFileId < this.files.length;
        }

        @Override
        public LogEntry next() {
            if (this.currentFileId == -1)
                { return null; }
            byte[] data;
            while (true) {
                if (currentFileId >= this.files.length)
                    { return null; }
                final SegmentFile segmentFile = (SegmentFile) this.files[currentFileId];
                if (segmentFile == null) {
                    return null;
                }

                data = segmentFile.lookupData(this.currentReadPos);
                if (data == null) {
                    // Reach file end
                    this.currentFileId += 1;
                    this.currentReadPos = FileHeader.HEADER_SIZE;
                } else {
                    this.preReadPos = this.currentReadPos;
                    this.currentReadPos += SegmentFile.getWriteBytes(data);
                    return this.logEntryDecoder.decode(data);
                }
            }
        }

        public int getReadPosition() {
            return this.preReadPos;
        }
    }

    public LogEntryIterator iterator(final LogEntryDecoder logEntryDecoder, long beginIndex, int beginPosition) {
        final AbstractFile[] files = this.fileManager.findFileFromLogIndex(beginIndex);
        return new LogEntryIterator(files, logEntryDecoder, beginPosition);
    }

    public LogEntryIterator iterator(final LogEntryDecoder logEntryDecoder) {
        final AbstractFile[] files = this.fileManager.copyFiles();
        return new LogEntryIterator(files, logEntryDecoder, 0);
    }

    /**
     * Recover when startUp
     */
    public synchronized void recover() {
        final List<AbstractFile> files = this.fileManager.loadExistedFiles();
        try {
            if (files.isEmpty()) {
                this.fileManager.setFlushedPosition(0);
                this.abortFile.create();
                return;
            }
            this.flushStatusCheckpoint.load();
            final boolean normalExit = !this.abortFile.exists();
            long recoverOffset;
            int startRecoverIndex;
            if (!normalExit) {
                // Abnormal exit, should recover from lastCheckpointFile
                startRecoverIndex = findLastCheckpointFile(files, this.flushStatusCheckpoint);
                LOG.info("{} {} did not exit normally, will try to recover files from fileIndex:{}.", getDBName(),
                    this.storePath, startRecoverIndex);
            } else {
                // Normal exit , just recover last file
                startRecoverIndex = files.size() - 1;
            }
            recoverOffset = (long) startRecoverIndex * (long) getDBFileSize();
            recoverOffset = recoverFiles(startRecoverIndex, files, recoverOffset);
            this.fileManager.setFlushedPosition(recoverOffset);

            if (normalExit) {
                this.abortFile.create();
            } else {
                this.abortFile.touch();
            }
        } catch (final Exception e) {
            LOG.error("Error on recover {} files , store path: {} , {}", getDBName(), this.storePath, e);
            throw new RuntimeException(e);
        } finally {
            startServiceManager();
        }
    }

    /**
     * Recover files
     * @return last recover offset
     */
    protected long recoverFiles(final int startRecoverIndex, final List<AbstractFile> files, long processOffset) {
        AbstractFile preFile = null;
        boolean needTruncate = false;
        for (int index = 0; index < files.size(); index++) {
            final AbstractFile file = files.get(index);
            final boolean isLastFile = index == files.size() - 1;

            if (index < startRecoverIndex) {
                // Update files' s position when don't need to recover
                file.updateAllPosition(getDBFileSize());
            } else {
                final RecoverResult result = file.recover();
                if (result.recoverSuccess()) {
                    if (result.recoverTotal()) {
                        processOffset += isLastFile ? result.getLastOffset() : getDBFileSize();
                    } else {
                        processOffset += result.getLastOffset();
                        needTruncate = true;
                    }
                } else {
                    needTruncate = true;
                }
            }

            if (preFile != null) {
                preFile.setLastLogIndex(file.getFirstLogIndex() - 1);
            }
            preFile = file;

            if (needTruncate) {
                // Error on recover files , truncate to processOffset
                LOG.warn("Try to truncate files to processOffset:{} when recover files", processOffset);
                this.fileManager.truncateSuffixByOffset(processOffset);
                break;
            }
        }
        return processOffset;
    }

    private int findLastCheckpointFile(final List<AbstractFile> files, final FlushStatusCheckpoint checkpoint) {
        if (checkpoint == null || checkpoint.fileName == null) {
            return 0;
        }
        for (int fileIndex = 0; fileIndex < files.size(); fileIndex++) {
            final AbstractFile file = files.get(fileIndex);
            if (getFileName(file).equalsIgnoreCase(checkpoint.fileName)) {
                return fileIndex;
            }
        }
        return 0;
    }

    private static String getFileName(AbstractFile file) {
        return Path.of(file.getFilePath()).getFileName().toString();
    }

    private void doCheckpoint() {
        long flushedPosition = getFlushedPosition();
        if (flushedPosition % getDBFileSize() == 0) {
            flushedPosition -= 1;
        }
        final AbstractFile file = this.fileManager.findFileByOffset(flushedPosition, false);
        try {
            if (file != null) {
                this.flushStatusCheckpoint.setFileName(getFileName(file));
                this.flushStatusCheckpoint.setFlushPosition(flushedPosition);
                this.flushStatusCheckpoint.setLastLogIndex(getLastLogIndex());
                this.flushStatusCheckpoint.save();
            }
        } catch (final IOException e) {
            LOG.error("Error when do checkpoint in db:{}", e, getDBName());
        }
    }

    /**
     * Write the data and return it's wrote position.
     * @param data logEntry data
     * @return (wrotePosition, expectFlushPosition)
     */
    public Pair<Integer, Long> appendLogAsync(final long logIndex, final byte[] data) {
        final int waitToWroteSize = SegmentFile.getWriteBytes(data);
        final SegmentFile segmentFile = (SegmentFile) this.fileManager.getLastFile(logIndex, waitToWroteSize, true);
        if (segmentFile != null) {
            final int pos = segmentFile.appendData(logIndex, data);
            final long expectFlushPosition = segmentFile.getFileFromOffset() + pos + waitToWroteSize;
            return Pair.of(pos, expectFlushPosition);
        }
        return Pair.of(-1, (long) -1);
    }

    /**
     * Read log from the segmentFile.
     *
     * @param logIndex the log index
     * @param pos      the position to read
     * @return read data
     */
    public byte[] lookupLog(final long logIndex, final int pos) {
        final SegmentFile segmentFile = (SegmentFile) this.fileManager.findFileByLogIndex(logIndex, false);
        if (segmentFile != null) {
            final long targetFlushPosition = segmentFile.getFileFromOffset() + pos;
            if (targetFlushPosition <= getFlushedPosition()) {
                return segmentFile.lookupData(logIndex, pos);
            }
        }
        return null;
    }

    /**
     * Flush db files and wait for flushPosition >= maxExpectedFlushPosition
     * @return true if flushPosition >= maxExpectedFlushPosition
     */
    public boolean waitForFlush(final long maxExpectedFlushPosition, final int maxFlushTimes) {
        int cnt = 0;
        while (getFlushedPosition() < maxExpectedFlushPosition) {
            flush();
            cnt++;
            if (cnt > maxFlushTimes) {
                LOG.error("Try flush db {} times, but the flushPosition {} can't exceed expectedFlushPosition {}",
                    maxFlushTimes, getFlushedPosition(), maxExpectedFlushPosition);
                return false;
            }
        }
        return true;
    }

    public void startServiceManager() {
        this.serviceManager.start();
    }

    public boolean flush() {
        return this.fileManager.flush();
    }

    public boolean truncatePrefix(final long firstIndexKept) {
        return this.fileManager.truncatePrefix(firstIndexKept);

    }

    public boolean truncateSuffix(final long lastIndexKept, final int pos) {
        if (this.fileManager.truncateSuffix(lastIndexKept, pos)) {
            doCheckpoint();

            return true; // This fix is missing in "jraft".
        }
        return false;
    }

    public boolean reset(final long nextLogIndex) {
        this.flushStatusCheckpoint.destroy();
        this.fileManager.reset(nextLogIndex);
        doCheckpoint();
        return true;
    }

    public long getFlushedPosition() {
        return this.fileManager.getFlushedPosition();
    }

    public StoreOptions getStoreOptions() {
        return this.storeOptions;
    }

    public String getStorePath() {
        return this.storePath;
    }

    public long getFirstLogIndex() {
        return this.fileManager.getFirstLogIndex();
    }

    public long getLastLogIndex() {
        return this.fileManager.getLastLogIndex();
    }
}
