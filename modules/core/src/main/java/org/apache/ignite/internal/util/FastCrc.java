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

package org.apache.ignite.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;

/**
 * Fast calculation of the CRC.
 */
public class FastCrc {
    /** CRC algo. */
    private static final ThreadLocal<CRC32> CRC = ThreadLocal.withInitial(CRC32::new);

    private final CRC32 crc = new CRC32();

    /** Current value. */
    private int val;

    /**
     * Constructor.
     */
    public FastCrc() {
        reset();
    }

    /**
     * Preparation for further calculations.
     */
    public void reset() {
        val = 0xffffffff;

        crc.reset();
    }

    /**
     * Returns crc value.
     */
    public int getValue() {
        return val;
    }

    /**
     * Updates crc value.
     *
     * @param buf Input buffer.
     * @param len Data length.
     */
    public void update(final ByteBuffer buf, final int len) {
        val = calcCrc(crc, buf, len);
    }

    /**
     * Calculates the crc checksum.
     *
     * @param buf Input buffer.
     * @param len Data length.
     * @return Crc checksum.
     */
    public static int calcCrc(ByteBuffer buf, int len) {
        CRC32 crcAlgo = CRC.get();

        int res = calcCrc(crcAlgo, buf, len);

        crcAlgo.reset();

        return res;
    }

    /**
     * Calculates the crc checksum.
     *
     * @param file A file to calculate checksum over it.
     * @return CRC32 checksum.
     * @throws IOException If fails.
     */
    @SuppressWarnings("PMD.EmptyControlStatement")
    public static int calcCrc(Path file) throws IOException {
        assert !Files.isDirectory(file) : "CRC32 can't be calculated over directories";

        CRC32 algo = new CRC32();

        try (InputStream in = new CheckedInputStream(Files.newInputStream(file), algo)) {
            byte[] buf = new byte[1024];

            while (in.read(buf) != -1) {
                // No-op.
            }
        }

        return ~(int) algo.getValue();
    }

    /**
     * Calculates the crc checksum.
     *
     * @param crcAlgo CRC algorithm.
     * @param buf Input buffer.
     * @param len Buffer length.
     * @return Crc checksum.
     */
    private static int calcCrc(CRC32 crcAlgo, ByteBuffer buf, int len) {
        int initLimit = buf.limit();

        buf.limit(buf.position() + len);

        crcAlgo.update(buf);

        buf.limit(initLimit);

        return ~(int) crcAlgo.getValue();
    }
}
