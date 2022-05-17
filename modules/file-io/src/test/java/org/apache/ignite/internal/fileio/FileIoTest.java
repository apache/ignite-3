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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

/**
 * For {@link FileIo} testing.
 */
public class FileIoTest {
    /** Test data size. */
    private static final int TEST_DATA_SIZE = 16 * 1024 * 1024;

    @Test
    public void testReadFully() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        fillRandomArray(arr);

        ByteBuffer buf = ByteBuffer.allocate(TEST_DATA_SIZE);

        TestFileIo fileIo = new TestFileIo(arr) {
            /** {@inheritDoc} */
            @Override
            public int read(ByteBuffer destBuf) throws IOException {
                if (destBuf.remaining() < 2) {
                    return super.read(destBuf);
                }

                int oldLimit = destBuf.limit();

                destBuf.limit(destBuf.position() + (destBuf.remaining() >> 1));

                try {
                    return super.read(destBuf);
                } finally {
                    destBuf.limit(oldLimit);
                }
            }
        };

        fileIo.readFully(buf);

        assertEquals(0, buf.remaining());

        assertArrayEquals(arr, buf.array());
    }

    @Test
    public void testReadFullyArray() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        byte[] arrDst = new byte[TEST_DATA_SIZE];

        fillRandomArray(arr);

        TestFileIo fileIo = new TestFileIo(arr) {
            /** {@inheritDoc} */
            @Override
            public int read(byte[] buf, int off, int len) throws IOException {
                return super.read(buf, off, len < 2 ? len : (len >> 1));
            }
        };

        fileIo.readFully(arrDst, 0, arrDst.length);

        assertArrayEquals(arr, arrDst);
    }

    @Test
    public void testWriteFully() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        ByteBuffer buf = ByteBuffer.allocate(TEST_DATA_SIZE);

        fillRandomArray(buf.array());

        TestFileIo fileIo = new TestFileIo(arr) {
            /** {@inheritDoc} */
            @Override
            public int write(ByteBuffer destBuf) throws IOException {
                if (destBuf.remaining() < 2) {
                    return super.write(destBuf);
                }

                int oldLimit = destBuf.limit();

                destBuf.limit(destBuf.position() + (destBuf.remaining() >> 1));

                try {
                    return super.write(destBuf);
                } finally {
                    destBuf.limit(oldLimit);
                }
            }
        };

        fileIo.writeFully(buf);

        assertEquals(0, buf.remaining());

        assertArrayEquals(arr, buf.array());
    }

    @Test
    public void testWriteFullyArray() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        byte[] arrSrc = new byte[TEST_DATA_SIZE];

        fillRandomArray(arrSrc);

        TestFileIo fileIo = new TestFileIo(arr) {
            /** {@inheritDoc} */
            @Override
            public int write(byte[] buf, int off, int len) throws IOException {
                return super.write(buf, off, len < 2 ? len : (len >> 1));
            }
        };

        fileIo.writeFully(arrSrc, 0, arrSrc.length);

        assertArrayEquals(arr, arrSrc);
    }

    private static void fillRandomArray(final byte[] arr) {
        ThreadLocalRandom.current().nextBytes(arr);
    }
}
