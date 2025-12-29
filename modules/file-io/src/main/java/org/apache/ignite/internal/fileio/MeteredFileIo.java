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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * File IO that records metrics for read and write operations under underlying File IO.
 */
public class MeteredFileIo implements FileIo {
    private final FileIo delegate;
    private final FileIoMetrics metrics;

    public MeteredFileIo(FileIo delegate, FileIoMetrics metrics) {
        this.delegate = delegate;
        this.metrics = metrics;
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public void position(long newPosition) throws IOException {
        delegate.position(newPosition);
    }

    @Override
    public int read(ByteBuffer destBuf) throws IOException {
        long startNanos = System.nanoTime();

        int bytesRead = delegate.read(destBuf);

        if (bytesRead > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordRead(bytesRead, durationNanos);
        }
        return bytesRead;
    }

    @Override
    public int read(ByteBuffer destBuf, long position) throws IOException {
        long startNanos = System.nanoTime();

        int bytesRead = delegate.read(destBuf, position);

        if (bytesRead > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordRead(bytesRead, durationNanos);
        }
        return bytesRead;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        long startNanos = System.nanoTime();

        int bytesRead = delegate.read(buf, off, len);

        if (bytesRead > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordRead(bytesRead, durationNanos);
        }
        return bytesRead;
    }

    @Override
    public int readFully(ByteBuffer destBuf) throws IOException {
        long startNanos = System.nanoTime();

        int bytesRead = delegate.read(destBuf);

        if (bytesRead > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordRead(bytesRead, durationNanos);
        }
        return bytesRead;
    }

    @Override
    public int readFully(ByteBuffer destBuf, long position) throws IOException {
        long startNanos = System.nanoTime();

        int bytesRead = delegate.read(destBuf, position);

        if (bytesRead > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordRead(bytesRead, durationNanos);
        }
        return bytesRead;
    }

    @Override
    public int readFully(byte[] buf, int off, int len) throws IOException {
        long startNanos = System.nanoTime();

        int bytesRead = delegate.read(buf, off, len);

        if (bytesRead > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordRead(bytesRead, durationNanos);
        }
        return bytesRead;
    }

    @Override
    public int write(ByteBuffer srcBuf) throws IOException {
        long startNanos = System.nanoTime();

        int bytesWritten = delegate.write(srcBuf);

        if (bytesWritten > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordWrite(bytesWritten, durationNanos);
        }
        return bytesWritten;
    }

    @Override
    public int write(ByteBuffer srcBuf, long position) throws IOException {
        long startNanos = System.nanoTime();

        int bytesWritten = delegate.write(srcBuf, position);

        if (bytesWritten > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordWrite(bytesWritten, durationNanos);
        }
        return bytesWritten;
    }

    @Override
    public int write(byte[] buf, int off, int len) throws IOException {
        long startNanos = System.nanoTime();

        int bytesWritten = delegate.write(buf, off, len);

        if (bytesWritten > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordWrite(bytesWritten, durationNanos);
        }
        return bytesWritten;
    }

    @Override
    public int writeFully(ByteBuffer srcBuf) throws IOException {
        long startNanos = System.nanoTime();

        int bytesWritten = delegate.writeFully(srcBuf);

        if (bytesWritten > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordWrite(bytesWritten, durationNanos);
        }
        return bytesWritten;
    }

    @Override
    public int writeFully(ByteBuffer srcBuf, long position) throws IOException {
        long startNanos = System.nanoTime();

        int bytesWritten = delegate.writeFully(srcBuf, position);

        if (bytesWritten > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordWrite(bytesWritten, durationNanos);
        }
        return bytesWritten;
    }

    @Override
    public int writeFully(byte[] buf, int off, int len) throws IOException {
        long startNanos = System.nanoTime();

        int bytesWritten = delegate.writeFully(buf, off, len);

        if (bytesWritten > 0) {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordWrite(bytesWritten, durationNanos);
        }
        return bytesWritten;
    }

    @Override
    public MappedByteBuffer map(int sizeBytes) throws IOException {
        return delegate.map(sizeBytes);
    }

    @Override
    public void force() throws IOException {
        delegate.force();
    }

    @Override
    public void force(boolean withMetadata) throws IOException {
        delegate.force(withMetadata);
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public void clear() throws IOException {
        delegate.clear();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
