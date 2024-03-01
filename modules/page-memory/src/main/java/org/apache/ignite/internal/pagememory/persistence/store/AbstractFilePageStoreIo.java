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

package org.apache.ignite.internal.pagememory.persistence.store;

import static java.nio.ByteOrder.nativeOrder;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.util.IgniteUtils.atomicMoveFile;
import static org.apache.ignite.internal.util.StringUtils.hexInt;
import static org.apache.ignite.internal.util.StringUtils.hexLong;
import static org.apache.ignite.internal.util.StringUtils.toHexString;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.fileio.FileIo;
import org.apache.ignite.internal.fileio.FileIoFactory;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.persistence.FastCrc;
import org.apache.ignite.internal.pagememory.persistence.IgniteInternalDataIntegrityViolationException;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract class for performing IO operations on file page storage.
 */
public abstract class AbstractFilePageStoreIo implements Closeable {
    /** {@link FileIo} factory. */
    protected final FileIoFactory ioFactory;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /** Skip CRC calculation flag. */
    // TODO: IGNITE-16350 Move to config
    private final boolean skipCrc = getBoolean("IGNITE_PDS_SKIP_CRC");

    private volatile Path filePath;

    private volatile @Nullable FileIo fileIo;

    /** Initialized file page store IO. */
    private volatile boolean initialized;

    /**
     * Caches the existence state of file. After it is initialized, it will be not {@code null} during lifecycle.
     *
     * <p>Guarded by {@link #readWriteLock}.
     */
    private @Nullable Boolean fileExists;

    /**
     * Constructor.
     *
     * @param ioFactory {@link FileIo} factory.
     * @param filePath File page store path.
     */
    AbstractFilePageStoreIo(FileIoFactory ioFactory, Path filePath) {
        this.ioFactory = ioFactory;
        this.filePath = filePath;
    }

    /**
     * Returns the page size in bytes.
     */
    public abstract int pageSize();

    /**
     * Returns the size of the header in bytes.
     */
    public abstract int headerSize();

    /**
     * Returns a buffer with the contents of the file page store header, for writing to a file.
     */
    public abstract ByteBuffer headerBuffer();

    /**
     * Checks the file page storage header.
     *
     * @param fileIo File page store IO to read the header.
     * @throws IOException If there is an error reading the header or the header did not pass the check.
     */
    public abstract void checkHeader(FileIo fileIo) throws IOException;

    /**
     * Returns page offset within the store file.
     *
     * @param pageId Page ID.
     */
    public abstract long pageOffset(long pageId);

    /**
     * Stops the file page store IO.
     *
     * @param clean {@code True} to clean file page store.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void stop(boolean clean) throws IgniteInternalCheckedException {
        try {
            stop0(clean);
        } catch (IOException e) {
            throw new IgniteInternalCheckedException(
                    "Failed to stop serving file [file=" + filePath + ", delete=" + clean + "]",
                    e
            );
        }
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        stop0(false);
    }

    /**
     * Reads a page.
     *
     * @param pageId Page ID.
     * @param pageOff Page offset in the file.
     * @param pageBuf Page buffer to read into.
     * @param keepCrc By default, reading zeroes CRC which was on page store, but you can keep it in {@code pageBuf} if set {@code true}.
     * @throws IgniteInternalCheckedException If reading failed (IO error occurred).
     */
    protected void read(long pageId, long pageOff, ByteBuffer pageBuf, boolean keepCrc) throws IgniteInternalCheckedException {
        read0(pageId, pageOff, pageBuf, !skipCrc, keepCrc);
    }

    /**
     * Writes a page.
     *
     * @param pageId Page ID.
     * @param pageBuf Page buffer to write from.
     * @param calculateCrc If {@code false} crc calculation will be forcibly skipped.
     * @throws IgniteInternalCheckedException If page writing failed (IO error occurred).
     */
    public void write(long pageId, ByteBuffer pageBuf, boolean calculateCrc) throws IgniteInternalCheckedException {
        ensure();

        boolean interrupted = false;

        while (true) {
            FileIo fileIo = this.fileIo;

            try {
                readWriteLock.readLock().lock();

                try {
                    assert pageBuf.position() == 0 : pageBuf.position();
                    assert pageBuf.order() == nativeOrder() : "Page buffer order " + pageBuf.order()
                            + " should be same with " + nativeOrder();
                    assert PageIo.getType(pageBuf) != 0 : "Invalid state. Type is 0! pageId = " + hexLong(pageId);
                    assert PageIo.getVersion(pageBuf) != 0 : "Invalid state. Version is 0! pageId = " + hexLong(pageId);

                    if (calculateCrc && !skipCrc) {
                        assert PageIo.getCrc(pageBuf) == 0 : hexLong(pageId);

                        PageIo.setCrc(pageBuf, calcCrc32(pageBuf, pageSize()));
                    }

                    // Check whether crc was calculated somewhere above the stack if it is forcibly skipped.
                    assert skipCrc || PageIo.getCrc(pageBuf) != 0
                            || calcCrc32(pageBuf, pageSize()) == 0 : "CRC hasn't been calculated, crc=0";

                    assert pageBuf.position() == 0 : pageBuf.position();

                    long pageOff = pageOffset(pageId);

                    fileIo.writeFully(pageBuf, pageOff);

                    PageIo.setCrc(pageBuf, 0);

                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }

                    return;
                } finally {
                    readWriteLock.readLock().unlock();
                }
            } catch (IOException e) {
                IOException cause = e;
                if (cause instanceof ClosedChannelException) {
                    try {
                        if (cause instanceof ClosedByInterruptException) {
                            interrupted = true;

                            Thread.interrupted();
                        }

                        reinit(fileIo);

                        pageBuf.position(0);

                        PageIo.setCrc(pageBuf, 0);

                        continue;
                    } catch (IOException e0) {
                        e0.addSuppressed(cause);

                        cause = e0;
                    }
                }

                throw new IgniteInternalCheckedException(
                        "Failed to write page [filePath=" + filePath + ", pageId=" + pageId + "]",
                        cause
                );
            }
        }
    }

    /**
     * Sync method used to ensure that the given pages are guaranteed to be written to the file page store.
     *
     * @throws IgniteInternalCheckedException If sync failed (IO error occurred).
     */
    public void sync() throws IgniteInternalCheckedException {
        ensure();

        readWriteLock.readLock().lock();

        try {
            FileIo fileIo = this.fileIo;

            if (fileIo != null) {
                fileIo.force();
            }
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Failed to fsync file [filePath=" + filePath + ']', e);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Returns {@code true} if the file page store exists.
     */
    public boolean exists() {
        readWriteLock.readLock().lock();

        try {
            if (fileExists == null) {
                fileExists = Files.exists(filePath) && filePath.toFile().length() >= headerSize();
            }

            return fileExists;
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Initializes the file page store if it hasn't already.
     *
     * @throws IgniteInternalCheckedException If initialization failed (IO error occurred).
     */
    void ensure() throws IgniteInternalCheckedException {
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

                                if (fileIo.size() < headerSize()) {
                                    fileIo.writeFully(headerBuffer().rewind(), 0);
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

    /**
     * Returns size of the file page store in bytes.
     *
     * @throws IgniteInternalCheckedException If an I/O error occurs.
     */
    public long size() throws IgniteInternalCheckedException {
        readWriteLock.readLock().lock();

        try {
            FileIo io = fileIo;

            return io == null ? 0 : io.size();
        } catch (IOException e) {
            throw new IgniteInternalCheckedException(e);
        } finally {
            readWriteLock.readLock().unlock();
        }
    }

    private void stop0(boolean clean) throws IOException {
        readWriteLock.writeLock().lock();

        try {
            if (!initialized) {
                // Ensure the file is closed even if not initialized yet.
                if (fileIo != null) {
                    fileIo.close();
                }

                if (clean && exists()) {
                    Files.delete(filePath);
                }

                return;
            }

            fileIo.force();

            fileIo.close();

            fileIo = null;

            if (clean) {
                Files.delete(filePath);

                fileExists = false;
            }
        } finally {
            initialized = false;

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
     * Reinit page store after file channel was closed by thread interruption.
     *
     * @param fileIo Old fileIo.
     */
    private void reinit(FileIo fileIo) throws IOException {
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

    /**
     * Reads a page from the page store.
     *
     * @param pageId Page ID.
     * @param pageOff Page offset in the file.
     * @param pageBuf Page buffer to read into.
     * @param checkCrc Check CRC on page.
     * @param keepCrc By default reading zeroes CRC which was on file, but you can keep it in pageBuf if set keepCrc.
     * @throws IgniteInternalCheckedException If reading failed (IO error occurred).
     */
    private void read0(
            long pageId,
            long pageOff,
            ByteBuffer pageBuf,
            boolean checkCrc,
            boolean keepCrc
    ) throws IgniteInternalCheckedException {
        assert pageOff >= headerSize() : "pageOff=" + pageOff + ", headerSize=" + headerSize();

        ensure();

        try {
            assert pageBuf.capacity() == pageSize() : pageBuf.capacity();
            assert pageBuf.remaining() == pageSize() : pageBuf.remaining();
            assert pageBuf.position() == 0 : pageBuf.position();
            assert pageBuf.order() == nativeOrder() : pageBuf.order();

            int n = readWithFailover(pageBuf, pageOff);

            // TODO: IGNITE-17397 Investigate the ability to read an empty page
            // If page was not written yet, nothing to read.
            if (n < 0) {
                pageBuf.put(new byte[pageBuf.remaining()]);

                return;
            }

            int savedCrc32 = PageIo.getCrc(pageBuf);

            PageIo.setCrc(pageBuf, 0);

            pageBuf.position(0);

            if (checkCrc) {
                int curCrc32 = FastCrc.calcCrc(pageBuf, pageSize());

                if ((savedCrc32 ^ curCrc32) != 0) {
                    throw new IgniteInternalDataIntegrityViolationException("Failed to read page (CRC validation failed) "
                            + "[id=" + hexLong(pageId) + ", off=" + pageOff
                            + ", filePath=" + filePath + ", fileSize=" + fileIo.size()
                            + ", savedCrc=" + hexInt(savedCrc32) + ", curCrc=" + hexInt(curCrc32)
                            + ", page=" + toHexString(pageBuf) + "]");
                }
            }

            assert PageIo.getCrc(pageBuf) == 0;

            if (keepCrc) {
                PageIo.setCrc(pageBuf, savedCrc32);
            }
        } catch (IOException e) {
            throw new IgniteInternalCheckedException("Failed to read page [file=" + filePath + ", pageId=" + pageId + "]", e);
        }
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
     * Returns file page store path.
     */
    public Path filePath() {
        return filePath;
    }

    /**
     * Renames the current file page store path to a new one.
     *
     * @param newFilePath New file page store path.
     * @throws IOException If failed.
     */
    public void renameFilePath(Path newFilePath) throws IOException {
        readWriteLock.writeLock().lock();

        try {
            initialized = false;

            Path filePath = this.filePath;

            if (!filePath.equals(newFilePath)) {
                FileIo fileIo = this.fileIo;

                assert fileIo != null : "Tried to rename right after creation: " + filePath;

                fileIo.force();

                fileIo.close();

                atomicMoveFile(filePath, newFilePath, null);

                this.filePath = newFilePath;

                reinit(fileIo);

                initialized = true;
            }
        } finally {
            readWriteLock.writeLock().unlock();
        }
    }
}
