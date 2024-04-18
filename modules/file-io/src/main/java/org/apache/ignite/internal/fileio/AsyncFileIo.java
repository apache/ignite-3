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

package org.apache.ignite.internal.fileio;

import static org.apache.ignite.internal.util.IgniteUtils.getUninterruptibly;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * {@link FileIo} implementation based on {@link AsynchronousFileChannel}.
 */
public class AsyncFileIo extends AbstractFileIo {
    /** File channel associated with {@code file}. */
    private final AsynchronousFileChannel ch;

    /** Channel's position. */
    private volatile long position;

    private final Set<ChannelOpFuture> asyncFutures = ConcurrentHashMap.newKeySet();

    /**
     * Creates I/O implementation for specified file.
     *
     * @param filePath File path.
     * @param modes Open modes.
     * @throws IOException If some I/O error occurs.
     */
    public AsyncFileIo(Path filePath, OpenOption... modes) throws IOException {
        ch = AsynchronousFileChannel.open(filePath, modes);
    }

    /** {@inheritDoc} */
    @Override
    public long position() {
        return position;
    }

    /** {@inheritDoc} */
    @Override
    public void position(long newPosition) {
        this.position = newPosition;
    }

    /** {@inheritDoc} */
    @Override
    public int read(ByteBuffer destBuf) throws IOException {
        ChannelOpFuture future = new ChannelOpFuture();

        ch.read(destBuf, position, this, future);

        return future.waitUninterruptibly();
    }

    /** {@inheritDoc} */
    @Override
    public int read(ByteBuffer destBuf, long position) throws IOException {
        ChannelOpFuture future = new ChannelOpFuture();

        ch.read(destBuf, position, null, future);

        try {
            return future.waitUninterruptibly();
        } finally {
            asyncFutures.remove(future);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int read(byte[] buf, int off, int length) throws IOException {
        ChannelOpFuture future = new ChannelOpFuture();

        ch.read(ByteBuffer.wrap(buf, off, length), position, this, future);

        return future.waitUninterruptibly();
    }

    /** {@inheritDoc} */
    @Override
    public int write(ByteBuffer srcBuf) throws IOException {
        ChannelOpFuture future = new ChannelOpFuture();

        ch.write(srcBuf, position, this, future);

        return future.waitUninterruptibly();
    }

    /** {@inheritDoc} */
    @Override
    public int write(ByteBuffer srcBuf, long position) throws IOException {
        ChannelOpFuture future = new ChannelOpFuture();

        asyncFutures.add(future);

        ch.write(srcBuf, position, null, future);

        try {
            return future.waitUninterruptibly();
        } finally {
            asyncFutures.remove(future);
        }
    }

    /** {@inheritDoc} */
    @Override
    public int write(byte[] buf, int off, int len) throws IOException {
        ChannelOpFuture future = new ChannelOpFuture();

        ch.write(ByteBuffer.wrap(buf, off, len), position, this, future);

        return future.waitUninterruptibly();
    }

    /** {@inheritDoc} */
    @Override
    public MappedByteBuffer map(int sizeBytes) {
        throw new UnsupportedOperationException("AsynchronousFileChannel doesn't support mmap.");
    }

    /** {@inheritDoc} */
    @Override
    public void force() throws IOException {
        force(false);
    }

    /** {@inheritDoc} */
    @Override
    public void force(boolean withMetadata) throws IOException {
        ch.force(withMetadata);
    }

    /** {@inheritDoc} */
    @Override
    public long size() throws IOException {
        return ch.size();
    }

    /** {@inheritDoc} */
    @Override
    public void clear() throws IOException {
        ch.truncate(0);

        this.position = 0;
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        for (ChannelOpFuture future : asyncFutures) {
            future.waitUninterruptibly();
        }

        ch.close();
    }

    /**
     * {@link CompletableFuture} for working with the {@link AsynchronousFileChannel}.
     */
    static class ChannelOpFuture extends CompletableFuture<Integer> implements CompletionHandler<Integer, AsyncFileIo> {
        /** {@inheritDoc} */
        @Override
        public void completed(Integer res, AsyncFileIo attach) {
            if (attach != null) {
                if (res != -1) {
                    attach.position += res;
                }
            }

            // Release waiter and allow next operation to begin.
            super.complete(res);
        }

        /** {@inheritDoc} */
        @Override
        public void failed(Throwable exc, AsyncFileIo attach) {
            super.completeExceptionally(exc);
        }

        int waitUninterruptibly() throws IOException {
            try {
                return getUninterruptibly(this);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }

                throw new IOException(cause);
            } catch (CancellationException e) {
                throw new IOException(e);
            }
        }
    }
}
