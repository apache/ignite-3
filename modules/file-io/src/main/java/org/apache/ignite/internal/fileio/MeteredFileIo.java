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
 * FileIo decorator that records metrics for read and write operations.
 */
public class MeteredFileIo extends FileIoDecorator {
    private final FileIoMetrics metrics;

    public MeteredFileIo(FileIo delegate, FileIoMetrics metrics) {
        super(delegate);
        this.metrics = metrics;
    }

    @FunctionalInterface
    private interface IoIntSupplier {
        int get() throws IOException;
    }

    private int measureRead(IoIntSupplier operation) throws IOException {
        long startNanos = System.nanoTime();
        int bytesRead = -1;
        try {
            bytesRead = operation.get();
            return bytesRead;
        } finally {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordRead(bytesRead, durationNanos);
        }
    }

    private int measureWrite(IoIntSupplier operation) throws IOException {
        long startNanos = System.nanoTime();
        int bytesWritten = -1;
        try {
            bytesWritten = operation.get();
            return bytesWritten;
        } finally {
            long durationNanos = System.nanoTime() - startNanos;
            metrics.recordWrite(bytesWritten, durationNanos);
        }
    }

    @Override
    public int read(ByteBuffer destBuf) throws IOException {
        return measureRead(() -> super.read(destBuf));
    }

    @Override
    public int read(ByteBuffer destBuf, long position) throws IOException {
        return measureRead(() -> super.read(destBuf, position));
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        return measureRead(() -> super.read(buf, off, len));
    }

    @Override
    public int write(ByteBuffer srcBuf) throws IOException {
        return measureWrite(() -> super.write(srcBuf));
    }

    @Override
    public int write(ByteBuffer srcBuf, long position) throws IOException {
        return measureWrite(() -> super.write(srcBuf, position));
    }

    @Override
    public int write(byte[] buf, int off, int len) throws IOException {
        return measureWrite(() -> super.write(buf, off, len));
    }

    @Override
    public MappedByteBuffer map(int sizeBytes) throws IOException {
        return super.map(sizeBytes);
    }
}
