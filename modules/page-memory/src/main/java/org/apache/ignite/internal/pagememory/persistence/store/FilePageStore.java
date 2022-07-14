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

import static java.nio.ByteOrder.nativeOrder;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.util.IgniteUtils.hexInt;
import static org.apache.ignite.internal.util.IgniteUtils.hexLong;
import static org.apache.ignite.internal.util.IgniteUtils.toHexString;
import static org.apache.ignite.lang.IgniteSystemProperties.getBoolean;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.persistence.FastCrc;
import org.apache.ignite.internal.pagememory.persistence.IgniteInternalDataIntegrityViolationException;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * FilePageStore is a {@link PageStore} implementation that uses regular files to store pages.
 *
 * <p>Actual read and write operations are performed with {@link FileIo} abstract interface, list of its implementations is a good source
 * of information about functionality in Ignite Native Persistence.
 *
 * <p>On a physical level each instance of {@code FilePageStore} corresponds to a partition file assigned to the local node.
 *
 * <p>Consists of:
 * <ul>
 *     <li>Header - {@link FilePageStoreHeader}. </li>
 *     <li>Body - data pages are multiples of {@link FilePageStoreHeader#pageSize() pageSize}.</li>
 * </ul>
 */
public class FilePageStore implements PageStore {
    /** File version. */
    static final int VERSION_1 = 1;

    /** Skip CRC calculation flag. */
    // TODO: IGNITE-17011 Move to config
    private final boolean skipCrc = getBoolean("IGNITE_PDS_SKIP_CRC");

    /** File page store version. */
    private final int version;

    /** Page size in bytes. */
    private final int pageSize;

    /** Header size in bytes. Should be aligned to the {@link #pageSize}. */
    private final int headerSize;

    /** File page store path. */
    private final Path filePath;

    /** {@link FileIo} factory. */
    private final FileIoFactory ioFactory;

    /** Page count. */
    private final AtomicInteger pageCount;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /** Caches the existence state of storage file. After it is initialized, it will be not {@code null} during lifecycle. */
    private volatile Boolean fileExists;

    /** {@link FileIo} for read/write operations with file. */
    private volatile FileIo fileIo;

    /** Initialized file page store. */
    private volatile boolean initialized;

    /**
     * Constructor.
     *
     * @param version File page store version.
     * @param pageSize Page size in bytes.
     * @param headerSize File page store header in bytes, should be {@code pageSize} aligned.
     * @param pageCount Page count.
     * @param filePath File page store path.
     * @param ioFactory {@link FileIo} factory.
     */
    public FilePageStore(
            int version,
            int pageSize,
            int headerSize,
            int pageCount,
            Path filePath,
            FileIoFactory ioFactory
    ) {
        assert pageCount >= 0 : pageCount;
        assert headerSize % pageSize == 0 : "Not aligned [headerSiz=" + headerSize + ", pageSize=" + pageSize + "]";

        this.version = version;
        this.pageSize = pageSize;
        this.headerSize = headerSize;
        this.filePath = filePath;
        this.ioFactory = ioFactory;

        this.pageCount = new AtomicInteger(pageCount);
    }

    /** {@inheritDoc} */
    @Override
    public void stop(boolean clean) throws IgniteInternalCheckedException {
        try {
            stop0(clean);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException(
                    "Failed to stop serving partition file [file=" + filePath + ", delete=" + clean + "]",
                    e
            );
        }
    }

    /** {@inheritDoc} */
    @Override
    public long allocatePage() throws IgniteInternalCheckedException {
        ensure();

        return pageCount.getAndIncrement();
    }

    /** {@inheritDoc} */
    @Override
    public int pages() {
        return pageCount.get();
    }

    /** {@inheritDoc} */
    @Override
    public boolean read(long pageId, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        return read(pageId, pageBuf, !skipCrc, keepCrc);
    }

    /**
     * Reads a page from the page store.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to read into.
     * @param checkCrc Check CRC on page.
     * @param keepCrc By default reading zeroes CRC which was on file, but you can keep it in pageBuf if set keepCrc.
     * @return {@code true} if page has been read successfully, {@code false} if page hasn't been written yet.
     * @throws IgniteInternalCheckedException If reading failed (IO error occurred).
     */
    private boolean read(long pageId, ByteBuffer pageBuf, boolean checkCrc, boolean keepCrc) throws IgniteInternalCheckedException {
        ensure();

        try {
            long off = pageOffset(pageId);

            assert pageBuf.capacity() == pageSize : pageBuf.capacity();
            assert pageBuf.remaining() == pageSize : pageBuf.remaining();
            assert pageBuf.position() == 0 : pageBuf.position();
            assert pageBuf.order() == nativeOrder() : pageBuf.order();
            assert off <= allocatedBytes() : "calculatedOffset=" + off
                    + ", allocated=" + allocatedBytes() + ", headerSize=" + headerSize + ", filePath=" + filePath;

            int n = readWithFailover(pageBuf, off);

            // If page was not written yet, nothing to read.
            if (n < 0) {
                pageBuf.put(new byte[pageBuf.remaining()]);

                return false;
            }

            int savedCrc32 = PageIo.getCrc(pageBuf);

            PageIo.setCrc(pageBuf, 0);

            pageBuf.position(0);

            if (checkCrc) {
                int curCrc32 = FastCrc.calcCrc(pageBuf, pageSize);

                if ((savedCrc32 ^ curCrc32) != 0) {
                    throw new IgniteInternalDataIntegrityViolationException("Failed to read page (CRC validation failed) "
                            + "[id=" + hexLong(pageId) + ", off=" + (off - pageSize)
                            + ", filePath=" + filePath + ", fileSize=" + fileIo.size()
                            + ", savedCrc=" + hexInt(savedCrc32) + ", curCrc=" + hexInt(curCrc32)
                            + ", page=" + toHexString(pageBuf) + "]");
                }
            }

            assert PageIo.getCrc(pageBuf) == 0;

            if (keepCrc) {
                PageIo.setCrc(pageBuf, savedCrc32);
            }

            return true;
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Failed to read page [file=" + filePath + ", pageId=" + pageId + "]", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void write(long pageId, ByteBuffer pageBuf, boolean calculateCrc) throws IgniteInternalCheckedException {
        ensure();

        boolean interrupted = false;

        while (true) {
            FileIo fileIo = this.fileIo;

            try {
                readWriteLock.readLock().lock();

                try {
                    long off = pageOffset(pageId);

                    assert (off >= 0 && off <= allocatedBytes()) : "off=" + hexLong(off) + ", allocated="
                            + hexLong(allocatedBytes()) + ", pageId=" + hexLong(pageId) + ", filePath=" + filePath;

                    assert pageBuf.position() == 0 : pageBuf.position();
                    assert pageBuf.order() == nativeOrder() : "Page buffer order " + pageBuf.order()
                            + " should be same with " + nativeOrder();
                    assert PageIo.getType(pageBuf) != 0 : "Invalid state. Type is 0! pageId = " + hexLong(pageId);
                    assert PageIo.getVersion(pageBuf) != 0 : "Invalid state. Version is 0! pageId = " + hexLong(pageId);

                    if (calculateCrc && !skipCrc) {
                        assert PageIo.getCrc(pageBuf) == 0 : hexLong(pageId);

                        PageIo.setCrc(pageBuf, calcCrc32(pageBuf, pageSize));
                    }

                    // Check whether crc was calculated somewhere above the stack if it is forcibly skipped.
                    assert skipCrc || PageIo.getCrc(pageBuf) != 0
                            || calcCrc32(pageBuf, pageSize) == 0 : "CRC hasn't been calculated, crc=0";

                    assert pageBuf.position() == 0 : pageBuf.position();

                    fileIo.writeFully(pageBuf, off);

                    PageIo.setCrc(pageBuf, 0);

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

                        reinit(fileIo);

                        pageBuf.position(0);

                        PageIo.setCrc(pageBuf, 0);

                        continue;
                    } catch (IOException e0) {
                        e0.addSuppressed(e);

                        e = e0;
                    }
                }

                throw new IgniteInternalCheckedException(
                        "Failed to write page [filePath=" + filePath + ", pageId=" + pageId + "]",
                        e
                );
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void sync() throws IgniteInternalCheckedException {
        readWriteLock.writeLock().lock();

        try {
            ensure();

            FileIo fileIo = this.fileIo;

            if (fileIo != null) {
                fileIo.force();
            }
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Failed to fsync partition file [filePath=" + filePath + ']', e);
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean exists() {
        if (fileExists == null) {
            readWriteLock.writeLock().lock();

            try {
                if (fileExists == null) {
                    fileExists = Files.exists(filePath) && filePath.toFile().length() >= headerSize;
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        }

        return fileExists;
    }

    /** {@inheritDoc} */
    @Override
    public void ensure() throws IgniteInternalCheckedException {
        if (!initialized) {
            readWriteLock.writeLock().lock();

            try {
                if (!initialized) {
                    FileIo fileIo = null;

                    IgniteInternalCheckedException err = null;

                    try {
                        boolean interrupted = false;

                        while (true) {
                            try {
                                this.fileIo = fileIo = ioFactory.create(filePath, CREATE, READ, WRITE);

                                fileExists = true;

                                if (fileIo.size() < headerSize) {
                                    fileIo.writeFully(new FilePageStoreHeader(version, pageSize).toByteBuffer(), 0);
                                } else {
                                    checkHeader(fileIo);
                                }

                                if (interrupted) {
                                    Thread.currentThread().interrupt();
                                }

                                break;
                            } catch (ClosedByInterruptException e) {
                                interrupted = true;

                                Thread.interrupted();
                            }
                        }

                        initialized = true;
                    } catch (IOException e) {
                        err = new IgniteInternalCheckedException("Failed to initialize partition file: " + filePath, e);

                        throw err;
                    } finally {
                        if (err != null && fileIo != null) {
                            try {
                                fileIo.close();
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

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        stop0(false);
    }

    /**
     * Returns size of the page store in bytes.
     *
     * <p>May differ from {@link #pages} * {@link #pageSize} due to delayed writes or due to other implementation specific details.
     *
     * @throws IgniteInternalCheckedException If an I/O error occurs.
     */
    public long size() throws IgniteInternalCheckedException {
        try {
            FileIo io = fileIo;

            return io == null ? 0 : io.size();
        } catch (IOException e) {
            throw new IgniteInternalCheckedException(e);
        }
    }

    /**
     * Stops file page store.
     *
     * @param delete {@code True} to delete file.
     * @throws IOException If fails.
     */
    private void stop0(boolean delete) throws IOException {
        readWriteLock.writeLock().lock();

        try {
            if (!initialized) {
                // Ensure the file is closed even if not initialized yet.
                if (fileIo != null) {
                    fileIo.close();
                }

                if (delete && exists()) {
                    Files.delete(filePath);
                }

                return;
            }

            fileIo.force();

            fileIo.close();

            fileIo = null;

            if (delete) {
                Files.delete(filePath);

                fileExists = false;
            }
        } finally {
            initialized = false;

            readWriteLock.writeLock().unlock();
        }
    }

    /**
     * Gets page offset within the store file.
     *
     * @param pageId Page ID.
     * @return Page offset.
     */
    long pageOffset(long pageId) {
        return (long) PageIdUtils.pageIndex(pageId) * pageSize + headerSize;
    }

    /**
     * Reads from page storage with failover.
     *
     * @param destBuf Destination buffer.
     * @param position Position.
     * @return Number of read bytes, or {@code -1} if the given position is greater than or equal to the file's current size.
     */
    private int readWithFailover(ByteBuffer destBuf, long position) throws IOException {
        boolean interrupted = false;

        int bufPos = destBuf.position();

        while (true) {
            FileIo fileIo = this.fileIo;

            if (fileIo == null) {
                throw new IOException("FileIo has stopped");
            }

            try {
                assert destBuf.remaining() > 0;

                int bytesRead = fileIo.readFully(destBuf, position);

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

                reinit(fileIo);
            }
        }
    }

    /**
     * Reinit page store after file channel was closed by thread interruption.
     *
     * @param fileIo Old fileIo.
     */
    private void reinit(FileIo fileIo) throws IOException {
        if (!initialized) {
            return;
        }

        if (fileIo != this.fileIo) {
            return;
        }

        readWriteLock.writeLock().lock();

        try {
            if (fileIo != this.fileIo) {
                return;
            }

            try {
                boolean interrupted = false;

                while (true) {
                    try {
                        fileIo = null;

                        fileIo = ioFactory.create(filePath, CREATE, READ, WRITE);

                        fileExists = true;

                        checkHeader(fileIo);

                        this.fileIo = fileIo;

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
                    if (fileIo != null) {
                        fileIo.close();
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

    private static int calcCrc32(ByteBuffer pageBuf, int pageSize) {
        try {
            pageBuf.position(0);

            return FastCrc.calcCrc(pageBuf, pageSize);
        } finally {
            pageBuf.position(0);
        }
    }

    /**
     * Returns file page store path.
     */
    Path filePath() {
        return filePath;
    }

    private void checkHeader(FileIo fileIo) throws IOException {
        FilePageStoreHeader header = FilePageStoreHeader.readHeader(fileIo);

        if (header == null) {
            throw new IOException("Missing file header");
        }

        FilePageStoreHeader.checkHeaderVersion(header, version);
        FilePageStoreHeader.checkHeaderPageSize(header, pageSize);
    }

    private long allocatedBytes() {
        return (long) pageCount.get() * pageSize;
    }

    /**
     * Returns file page store header size.
     */
    public int headerSize() {
        return headerSize;
    }
}
