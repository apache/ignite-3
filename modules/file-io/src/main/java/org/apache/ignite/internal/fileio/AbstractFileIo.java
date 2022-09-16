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
import java.util.concurrent.TimeUnit;

/**
 * Abstract {@link FileIo}.
 */
public abstract class AbstractFileIo implements FileIo {
    /** Max io timeout milliseconds. */
    private static final int MAX_IO_TIMEOUT_MS = 2000;

    /** {@inheritDoc} */
    @Override
    public int readFully(final ByteBuffer destBuf) throws IOException {
        return fully(offs -> read(destBuf), position(), destBuf.remaining(), false);
    }

    /** {@inheritDoc} */
    @Override
    public int readFully(final ByteBuffer destBuf, final long position) throws IOException {
        return fully(offs -> read(destBuf, position + offs), position, destBuf.remaining(), false);
    }

    /** {@inheritDoc} */
    @Override
    public int readFully(final byte[] buf, final int off, final int len) throws IOException {
        return fully(offs -> read(buf, off + offs, len - offs), position(), len, false);
    }

    /** {@inheritDoc} */
    @Override
    public int writeFully(final ByteBuffer srcBuf) throws IOException {
        return fully(offs -> write(srcBuf), position(), srcBuf.remaining(), true);
    }

    /** {@inheritDoc} */
    @Override
    public int writeFully(final ByteBuffer srcBuf, final long position) throws IOException {
        return fully(offs -> write(srcBuf, position + offs), position, srcBuf.remaining(), true);
    }

    /** {@inheritDoc} */
    @Override
    public int writeFully(final byte[] buf, final int off, final int len) throws IOException {
        return fully(offs -> write(buf, off + offs, len - offs), position(), len, true);
    }

    /**
     * I/O operation.
     */
    private interface IoOperation {
        /**
         * Returns number of bytes operated.
         *
         * @param offs Offset.
         * @throws IOException If some I/O error occurs.
         */
        int run(int offs) throws IOException;
    }

    /**
     * Returns available bytes.
     *
     * @param requested Requested bytes.
     * @param position File position.
     * @throws IOException If some I/O error occurs.
     */
    private int available(int requested, long position) throws IOException {
        long available = size() - position;

        return requested > available ? (int) available : requested;
    }

    /**
     * Returns number of written/read bytes.
     *
     * @param operation IO operation.
     * @param position File position.
     * @param num Number of bytes to operate.
     * @param write {@code True} if writing to file.
     * @throws IOException If some I/O error occurs.
     */
    private int fully(IoOperation operation, long position, int num, boolean write) throws IOException {
        if (num > 0) {
            long time = 0;

            for (int i = 0; i < num; ) {
                int n = operation.run(i);

                if (n > 0) {
                    i += n;
                    time = 0;
                } else if (n == 0 || i > 0) {
                    if (!write && available(num - i, position + i) == 0) {
                        return i;
                    }

                    if (time == 0) {
                        time = System.nanoTime();
                    } else if ((System.nanoTime() - time) >= TimeUnit.MILLISECONDS.toNanos(MAX_IO_TIMEOUT_MS)) {
                        throw new IOException(write && (position + i) == size() ? "Failed to extend file." :
                                "Probably disk is too busy, please check your device.");
                    }
                } else {
                    return -1;
                }
            }
        }

        return num;
    }
}
