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

package org.apache.ignite.internal.pagememory.persistence.store;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * FilePageStore is a PageStore implementation that uses regular files to store pages.
 * <p>
 * Actual read and write operations are performed with {@link FileIo} abstract interface, list of its implementations is a good source of
 * information about functionality in Ignite Native Persistence.
 * </p>
 * <p>
 * On a physical level each instance of FilePageStore corresponds to a partition file assigned to the local node or to index file of a
 * particular cache if any secondary indexes were created.
 * </p>
 * <p>
 * Instances of FilePageStore are managed by {@link FilePageStoreManager} for regular cache operations like assignment of new partition to
 * the local node or checkpoint event and by {@link IgniteSnapshotManager} during snapshot creation.
 * </p>
 */
public class FilePageStore implements PageStore {
    /** Page store file signature. */
    private static final long SIGNATURE = 0xF19AC4FE60C530B8L;

    /** File version. */
    public static final int VERSION = 1;

    /** Allocated field offset. */
    public static final int HEADER_SIZE = 8/*SIGNATURE*/ + 4/*VERSION*/ + 1/*type*/ + 4/*page size*/;

    /**
     * The type of stored pages into given page storage :{@link PageStore#TYPE_DATA} for regular affinity partition files or {@link
     * PageStore#TYPE_IDX} for index pages.
     */
    private final byte type;

    /** File page store path. */
    private final Path filePath;

    /** {@link FileIo} factory. */
    private final FileIoFactory ioFactory;

    /** Page size in bytes. */
    private final int pageSize;

    /**
     * Caches the existence state of storage file. After it is initialized, it will be not set to null during FilePageStore lifecycle.
     */
    private volatile Boolean fileExists;

    /** {@link FileIo} for read/write operations with file. */
    protected volatile FileIo fileIo;

    /** Number of allocated bytes. */
    private final AtomicLong allocatedBytes = new AtomicLong();

    /** List of listeners for current page store to handle. */
    private final List<PageWriteListener> listeners = new CopyOnWriteArrayList<>();

    /**
     *
     */
    private volatile boolean inited;

    /** Partition file version, 1-based incrementing counter. For outdated pages tag has low value, and write does nothing. */
    private volatile int tag;

    /**
     *
     */
    private final boolean skipCrc = IgniteSystemProperties.getBoolean(IGNITE_PDS_SKIP_CRC);

    /**
     *
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Constructor.
     *
     * @param type Data type, can be {@link PageStore#TYPE_IDX} or {@link PageStore#TYPE_DATA}.
     * @param filePath File page store path.
     * @param ioFactory {@link FileIo} factory.
     * @param pageSize Page size in bytes.
     */
    public FilePageStore(
            byte type,
            Path filePath,
            FileIoFactory ioFactory,
            int pageSize
    ) {
        assert type == PageStore.TYPE_DATA || type == PageStore.TYPE_IDX : type;

        this.type = type;
        this.filePath = filePath;
        this.ioFactory = ioFactory;
        this.pageSize = pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public void addWriteListener(PageWriteListener lsnr) {
        listeners.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override
    public void removeWriteListener(PageWriteListener lsnr) {
        listeners.remove(lsnr);
    }

    /** {@inheritDoc} */
    @Override
    public int getPageSize() {
        return pageSize;
    }

    /** {@inheritDoc} */
    @Override
    public long size() {
        try {
            FileIo io = fileIo;

            return io == null ? 0 : io.size();
        } catch (IOException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean exists() {
        if (fileExists == null) {
            readWriteLock.writeLock().lock();

            try {
                if (fileExists == null) {
                    File file = filePath.apply().toFile();

                    fileExists = file.exists() && file.length() > headerSize();
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }

        return fileExists;
    }

    /**
     * Size of page store header.
     */
    public int headerSize() {
        return HEADER_SIZE;
    }

    /**
     * Page store version.
     */
    @Override
    public int version() {
        return VERSION;
    }

    /**
     * Creates header for current version file store. Doesn't init the store.
     *
     * @param type Type.
     * @param pageSize Page size.
     * @return Byte buffer instance.
     */
    public ByteBuffer header(byte type, int pageSize) {
        ByteBuffer hdr = ByteBuffer.allocate(headerSize()).order(ByteOrder.nativeOrder());

        hdr.putLong(SIGNATURE);

        hdr.putInt(version());

        hdr.put(type);

        hdr.putInt(pageSize);

        hdr.rewind();

        return hdr;
    }

    /**
     * Initializes header and writes it into the file store.
     *
     * @return Next available position in the file to store a data.
     * @throws IOException If initialization is failed.
     */
    private long initFile(FileIo fileIO) throws IOException {
        try {
            ByteBuffer hdr = header(type, pageSize);

            fileIO.writeFully(hdr);

            //there is 'super' page in every file
            return headerSize() + pageSize;
        } catch (ClosedByInterruptException e) {
            // If thread was interrupted written header can be inconsistent.
            readWriteLock.writeLock().lock();

            try {
                Files.delete(filePath.apply());

                fileExists = false;
            } finally {
                readWriteLock.writeLock().unlock();
            }

            throw e;
        }
    }

    /**
     * Checks that file store has correct header and size.
     *
     * @return Next available position in the file to store a data.
     * @throws IOException If check has failed.
     */
    private long checkFile(FileIo fileIO, File cfgFile) throws IOException {
        ByteBuffer hdr = ByteBuffer.allocate(headerSize()).order(ByteOrder.nativeOrder());

        fileIO.readFully(hdr);

        hdr.rewind();

        long signature = hdr.getLong();

        String prefix = "Failed to verify, file=" + cfgFile.getAbsolutePath() + "\" ";

        if (SIGNATURE != signature) {
            throw new IOException(prefix + "(invalid file signature)" +
                    " [expectedSignature=" + U.hexLong(SIGNATURE) +
                    ", actualSignature=" + U.hexLong(signature) + ']');
        }

        int ver = hdr.getInt();

        if (version() != ver) {
            throw new IOException(prefix + "(invalid file version)" +
                    " [expectedVersion=" + version() +
                    ", fileVersion=" + ver + "]");
        }

        byte type = hdr.get();

        if (this.type != type) {
            throw new IOException(prefix + "(invalid file type)" +
                    " [expectedFileType=" + this.type +
                    ", actualFileType=" + type + "]");
        }

        int pageSize = hdr.getInt();

        if (this.pageSize != pageSize) {
            throw new IOException(prefix + "(invalid page size)" +
                    " [expectedPageSize=" + this.pageSize +
                    ", filePageSize=" + pageSize + "]");
        }

        long fileSize = cfgFile.length();

        if (fileSize == headerSize()) // Every file has a special meta page.
        {
            fileSize = pageSize + headerSize();
        }

        if (fileSize % pageSize != 0) // In the case of compressed pages we can miss the tail of the page.
        {
            fileSize = (fileSize / pageSize + 1) * pageSize;
        }

        return fileSize;
    }

    /**
     * @param delete {@code True} to delete file.
     * @throws IOException If fails.
     */
    private void stop0(boolean delete) throws IOException {
        readWriteLock.writeLock().lock();

        try {
            if (!inited) {
                if (fileIo != null) // Ensure the file is closed even if not initialized yet.
                {
                    fileIo.close();
                }

                if (delete && exists()) {
                    Files.delete(filePath.apply().toAbsolutePath());
                }

                return;
            }

            fileIo.force();

            fileIo.close();

            fileIo = null;

            if (delete) {
                Files.delete(filePath.apply());

                fileExists = false;
            }
        } finally {
            inited = false;

            readWriteLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop(boolean delete) throws StorageException {
        try {
            stop0(delete);
        } catch (IOException e) {
            throw new StorageException("Failed to stop serving partition file [file=" + getFileAbsolutePath()
                    + ", delete=" + delete + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        stop0(false);
    }

    /** {@inheritDoc} */
    @Override
    public void truncate(int tag) throws StorageException {
        init();

        Path filePath = this.filePath.apply();

        readWriteLock.writeLock().lock();

        try {
            this.tag = tag;

            fileIo.clear();

            fileIo.close();

            fileIo = null;

            Files.delete(filePath);

            fileExists = false;
        } catch (IOException e) {
            throw new StorageException("Failed to truncate partition file [file=" + filePath.toAbsolutePath() + "]", e);
        } finally {
            inited = false;

            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * @param pageId Page ID.
     * @param pageBuf Page buffer.
     * @return Number of bytes to calculate CRC on.
     */
    private int getCrcSize(long pageId, ByteBuffer pageBuf) throws IOException {
        int compressedSize = PageIO.getCompressedSize(pageBuf);

        if (compressedSize == 0) {
            return pageSize; // Page is not compressed.
        }

        if (compressedSize < 0 || compressedSize > pageSize) {
            throw new IgniteDataIntegrityViolationException("Failed to read page (CRC validation failed) " +
                    "[id=" + U.hexLong(pageId) + ", file=" + getFileAbsolutePath() + ", fileSize=" + fileIo.size() +
                    ", page=" + U.toHexString(pageBuf) + "]");
        }

        return compressedSize;
    }

    /** {@inheritDoc} */
    @Override
    public boolean read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        return read(pageId, pageBuf, !skipCrc, keepCrc);
    }

    /**
     * @param pageId Page ID.
     * @param pageBuf Page buffer to read into.
     * @param checkCrc Check CRC on page.
     * @param keepCrc By default reading zeroes CRC which was on file, but you can keep it in pageBuf if set keepCrc
     * @return {@code true} if page has been read successfully, {@code false} if page hasn't been written yet.
     * @throws IgniteInternalCheckedException If reading failed (IO error occurred).
     */
    public boolean read(long pageId, ByteBuffer pageBuf, boolean checkCrc, boolean keepCrc) throws IgniteInternalCheckedException {
        init();

        try {
            long off = pageOffset(pageId);

            assert pageBuf.capacity() == pageSize;
            assert pageBuf.remaining() == pageSize;
            assert pageBuf.position() == 0;
            assert pageBuf.order() == ByteOrder.nativeOrder();
            assert off <= allocatedBytes.get() : "calculatedOffset=" + off +
                    ", allocated=" + allocatedBytes.get() + ", headerSize=" + headerSize() + ", cfgFile=" + filePath;

            int n = readWithFailover(pageBuf, off);

            // If page was not written yet, nothing to read.
            if (n < 0) {
                pageBuf.put(new byte[pageBuf.remaining()]);

                return false;
            }

            int savedCrc32 = PageIO.getCrc(pageBuf);

            PageIO.setCrc(pageBuf, 0);

            pageBuf.position(0);

            if (checkCrc) {
                int curCrc32 = FastCrc.calcCrc(pageBuf, getCrcSize(pageId, pageBuf));

                if ((savedCrc32 ^ curCrc32) != 0) {
                    throw new IgniteDataIntegrityViolationException("Failed to read page (CRC validation failed) " +
                            "[id=" + U.hexLong(pageId) + ", off=" + (off - pageSize) +
                            ", file=" + getFileAbsolutePath() + ", fileSize=" + fileIo.size() +
                            ", savedCrc=" + U.hexInt(savedCrc32) + ", curCrc=" + U.hexInt(curCrc32) +
                            ", page=" + U.toHexString(pageBuf) +
                            "]");
                }
            }

            assert PageIO.getCrc(pageBuf) == 0;

            if (keepCrc) {
                PageIO.setCrc(pageBuf, savedCrc32);
            }

            return true;
        } catch (IOException e) {
            throw new StorageException("Failed to read page [file=" + getFileAbsolutePath() + ", pageId=" + pageId + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void readHeader(ByteBuffer buf) throws IgniteInternalCheckedException {
        init();

        try {
            assert buf.remaining() == headerSize();

            readWithFailover(buf, 0);
        } catch (IOException e) {
            throw new StorageException("Failed to read header [file=" + getFileAbsolutePath() + "]", e);
        }
    }

    /**
     * @throws StorageException If failed to initialize store file.
     */
    public void init() throws StorageException {
        if (!inited) {
            readWriteLock.writeLock().lock();

            try {
                if (!inited) {
                    FileIO fileIO = null;

                    StorageException err = null;

                    long newSize;

                    try {
                        boolean interrupted = false;

                        while (true) {
                            try {
                                File cfgFile = filePath.apply().toFile();

                                this.fileIo = fileIO = ioFactory.create(cfgFile, CREATE, READ, WRITE);

                                fileExists = true;

                                newSize = (cfgFile.length() == 0 ? initFile(fileIO) : checkFile(fileIO, cfgFile)) - headerSize();

                                if (interrupted) {
                                    Thread.currentThread().interrupt();
                                }

                                break;
                            } catch (ClosedByInterruptException e) {
                                interrupted = true;

                                Thread.interrupted();
                            }
                        }

                        assert allocatedBytes.get() == 0;

                        allocatedBytes.set(newSize);

                        inited = true;
                    } catch (IOException e) {
                        err = new StorageException("Failed to initialize partition file: " + getFileAbsolutePath(), e);

                        throw err;
                    } finally {
                        if (err != null && fileIO != null) {
                            try {
                                fileIO.close();
                            } catch (IOException e) {
                                err.addSuppressed(e);
                            }
                        }
                    }
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }
    }

    /**
     * Reinit page store after file channel was closed by thread interruption.
     *
     * @param fileIO Old fileIO.
     */
    private void reinit(FileIo fileIO) throws IOException {
        if (!inited) {
            return;
        }

        if (fileIO != this.fileIo) {
            return;
        }

        readWriteLock.writeLock().lock();

        try {
            if (fileIO != this.fileIo) {
                return;
            }

            try {
                boolean interrupted = false;

                while (true) {
                    try {
                        fileIO = null;

                        File cfgFile = filePath.apply().toFile();

                        fileIO = ioFactory.create(cfgFile, CREATE, READ, WRITE);

                        fileExists = true;

                        checkFile(fileIO, cfgFile);

                        this.fileIo = fileIO;

                        if (interrupted) {
                            Thread.currentThread().interrupt();
                        }

                        break;
                    } catch (ClosedByInterruptException e) {
                        interrupted = true;

                        Thread.interrupted();
                    }
                }
            } catch (IOException e) {
                try {
                    if (fileIO != null) {
                        fileIO.close();
                    }
                } catch (IOException e0) {
                    e.addSuppressed(e0);
                }

                throw e;
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void write(long pageId, ByteBuffer pageBuf, int tag, boolean calculateCrc) throws IgniteInternalCheckedException {
        init();

        boolean interrupted = false;

        while (true) {
            FileIO fileIO = this.fileIo;

            try {
                readWriteLock.readLock().lock();

                try {
                    if (tag < this.tag) {
                        return;
                    }

                    long off = pageOffset(pageId);

                    assert (off >= 0 && off <= allocatedBytes.get()) || recover :
                            "off=" + U.hexLong(off) + ", allocated=" + U.hexLong(allocatedBytes.get()) +
                                    ", pageId=" + U.hexLong(pageId) + ", file=" + getFileAbsolutePath();

                    assert pageBuf.position() == 0;
                    assert pageBuf.order() == ByteOrder.nativeOrder() : "Page buffer order " + pageBuf.order()
                            + " should be same with " + ByteOrder.nativeOrder();
                    assert PageIO.getType(pageBuf) != 0 : "Invalid state. Type is 0! pageId = " + U.hexLong(pageId);
                    assert PageIO.getVersion(pageBuf) != 0 : "Invalid state. Version is 0! pageId = " + U.hexLong(pageId);

                    if (calculateCrc && !skipCrc) {
                        assert PageIO.getCrc(pageBuf) == 0 : U.hexLong(pageId);

                        PageIO.setCrc(pageBuf, calcCrc32(pageBuf, getCrcSize(pageId, pageBuf)));
                    }

                    // Check whether crc was calculated somewhere above the stack if it is forcibly skipped.
                    assert skipCrc || PageIO.getCrc(pageBuf) != 0 || calcCrc32(pageBuf, pageSize) == 0 :
                            "CRC hasn't been calculated, crc=0";

                    assert pageBuf.position() == 0 : pageBuf.position();

                    for (PageWriteListener lsnr : listeners) {
                        lsnr.accept(pageId, pageBuf);

                        pageBuf.rewind();
                    }

                    fileIO.writeFully(pageBuf, off);

                    PageIO.setCrc(pageBuf, 0);

                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }

                    return;
                } finally {
                    readWriteLock.readLock().unlock();
                }
            } catch (IOException e) {
                if (e instanceof ClosedChannelException) {
                    try {
                        if (e instanceof ClosedByInterruptException) {
                            interrupted = true;

                            Thread.interrupted();
                        }

                        reinit(fileIO);

                        pageBuf.position(0);

                        PageIO.setCrc(pageBuf, 0);

                        continue;
                    } catch (IOException e0) {
                        e0.addSuppressed(e);

                        e = e0;
                    }
                }

                throw new StorageException("Failed to write page [file=" + getFileAbsolutePath()
                        + ", pageId=" + pageId + ", tag=" + tag + "]", e);
            }
        }
    }

    /**
     * @param pageBuf Page buffer.
     * @param pageSize Page size.
     */
    private static int calcCrc32(ByteBuffer pageBuf, int pageSize) {
        try {
            pageBuf.position(0);

            return FastCrc.calcCrc(pageBuf, pageSize);
        } finally {
            pageBuf.position(0);
        }
    }

    /** {@inheritDoc} */
    @Override
    public long pageOffset(long pageId) {
        return (long) PageIdUtils.pageIndex(pageId) * pageSize + headerSize();
    }

    /** {@inheritDoc} */
    @Override
    public void sync() throws StorageException {
        readWriteLock.writeLock().lock();

        try {
            init();

            FileIO fileIO = this.fileIo;

            if (fileIO != null) {
                fileIO.force();
            }
        } catch (IOException e) {
            throw new StorageException("Failed to fsync partition file [file=" + getFileAbsolutePath() + ']', e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void ensure() throws IgniteInternalCheckedException {
        init();
    }

    /** {@inheritDoc} */
    @Override
    public long allocatePage() throws IgniteInternalCheckedException {
        init();

        return allocPage() / pageSize;
    }

    /**
     * @return File absolute path.
     */
    public String getFileAbsolutePath() {
        return filePath.apply().toAbsolutePath().toString();
    }

    private long allocPage() {
        long off;

        do {
            off = allocatedBytes.get();

            if (allocatedBytes.compareAndSet(off, off + pageSize)) {
                break;
            }
        }
        while (true);

        return off;
    }

    /** {@inheritDoc} */
    @Override
    public long pages() {
        if (!inited) {
            return 0;
        }

        return (int) (allocatedBytes.get() / pageSize);
    }

    /**
     * @param destBuf Destination buffer.
     * @param position Position.
     * @return Number of read bytes.
     */
    private int readWithFailover(ByteBuffer destBuf, long position) throws IOException {
        boolean interrupted = false;

        int bufPos = destBuf.position();

        while (true) {
            FileIO fileIO = this.fileIo;

            if (fileIO == null) {
                throw new IOException("FileIO has stopped");
            }

            try {
                assert destBuf.remaining() > 0;

                int bytesRead = fileIO.readFully(destBuf, position);

                if (interrupted) {
                    Thread.currentThread().interrupt();
                }

                return bytesRead;
            } catch (ClosedChannelException e) {
                destBuf.position(bufPos);

                if (e instanceof ClosedByInterruptException) {
                    interrupted = true;

                    Thread.interrupted();
                }

                reinit(fileIO);
            }
        }
    }
}
