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
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

/**
 * {@link FileIo} implementation based on {@link FileChannel}.
 */
public class RandomAccessFileIo extends AbstractFileIo {
    /** File channel. */
    private final FileChannel ch;

    /**
     * Creates I/O implementation for specified file.
     *
     * @param filePath File path.
     * @param modes Open modes.
     */
    public RandomAccessFileIo(Path filePath, OpenOption... modes) throws IOException {
        ch = FileChannel.open(filePath, modes);
    }

    /** {@inheritDoc} */
    @Override
    public long position() throws IOException {
        return ch.position();
    }

    /** {@inheritDoc} */
    @Override
    public void position(long newPosition) throws IOException {
        ch.position(newPosition);
    }

    /** {@inheritDoc} */
    @Override
    public int read(ByteBuffer destBuf) throws IOException {
        return ch.read(destBuf);
    }

    /** {@inheritDoc} */
    @Override
    public int read(ByteBuffer destBuf, long position) throws IOException {
        return ch.read(destBuf, position);
    }

    /** {@inheritDoc} */
    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        return ch.read(ByteBuffer.wrap(buf, off, len));
    }

    /** {@inheritDoc} */
    @Override
    public int write(ByteBuffer srcBuf) throws IOException {
        return ch.write(srcBuf);
    }

    /** {@inheritDoc} */
    @Override
    public int write(ByteBuffer srcBuf, long position) throws IOException {
        return ch.write(srcBuf, position);
    }

    /** {@inheritDoc} */
    @Override
    public int write(byte[] buf, int off, int len) throws IOException {
        return ch.write(ByteBuffer.wrap(buf, off, len));
    }

    /** {@inheritDoc} */
    @Override
    public void force(boolean withMetadata) throws IOException {
        ch.force(withMetadata);
    }

    /** {@inheritDoc} */
    @Override
    public void force() throws IOException {
        force(false);
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
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        ch.close();
    }

    /** {@inheritDoc} */
    @Override
    public MappedByteBuffer map(int sizeBytes) throws IOException {
        return ch.map(FileChannel.MapMode.READ_WRITE, 0, sizeBytes);
    }
}
