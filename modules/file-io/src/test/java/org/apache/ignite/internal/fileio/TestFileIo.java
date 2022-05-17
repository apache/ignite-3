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

package org.apache.ignite.internal.fileio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * {@link FileIo} implementation based on byte array.
 */
class TestFileIo extends AbstractFileIo {
    /** Data. */
    private final byte[] data;

    /** Position. */
    private int position;

    /**
     * Constructor.
     *
     * @param data Initial data.
     */
    public TestFileIo(byte[] data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override
    public long position() {
        return position;
    }

    /** {@inheritDoc} */
    @Override
    public void position(long newPosition) throws IOException {
        checkPosition(newPosition);

        this.position = (int) newPosition;
    }

    /** {@inheritDoc} */
    @Override
    public int read(ByteBuffer destBuf) throws IOException {
        final int len = Math.min(destBuf.remaining(), data.length - position);

        destBuf.put(data, position, len);

        position += len;

        return len;
    }

    /** {@inheritDoc} */
    @Override
    public int read(ByteBuffer destBuf, long position) throws IOException {
        checkPosition(position);

        final int len = Math.min(destBuf.remaining(), data.length - (int) position);

        destBuf.put(data, (int) position, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override
    public int read(byte[] buf, int off, int maxLen) throws IOException {
        final int len = Math.min(maxLen, data.length - position);

        System.arraycopy(data, position, buf, off, len);

        position += len;

        return len;
    }

    /** {@inheritDoc} */
    @Override
    public int write(ByteBuffer srcBuf) throws IOException {
        final int len = Math.min(srcBuf.remaining(), data.length - position);

        srcBuf.get(data, position, len);

        position += len;

        return len;
    }

    /** {@inheritDoc} */
    @Override
    public int write(ByteBuffer srcBuf, long position) throws IOException {
        checkPosition(position);

        final int len = Math.min(srcBuf.remaining(), data.length - (int) position);

        srcBuf.get(data, (int) position, len);

        return len;
    }

    /** {@inheritDoc} */
    @Override
    public int write(byte[] buf, int off, int maxLen) throws IOException {
        final int len = Math.min(maxLen, data.length - position);

        System.arraycopy(buf, off, data, position, len);

        position += len;

        return len;
    }

    /** {@inheritDoc} */
    @Override
    public MappedByteBuffer map(int sizeBytes) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    public void force() {
    }

    /** {@inheritDoc} */
    @Override
    public void force(boolean withMetadata) {
    }

    /** {@inheritDoc} */
    @Override
    public long size() {
        return data.length;
    }

    /** {@inheritDoc} */
    @Override
    public void clear() {
        position = 0;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
    }

    private void checkPosition(long position) throws IOException {
        if (position < 0 || position >= data.length) {
            throw new IOException("Invalid position: " + position);
        }
    }
}
