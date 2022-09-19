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

package org.apache.ignite.internal.pagememory.persistence;

import static org.apache.ignite.internal.pagememory.persistence.FastCrc.calcCrc;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link FastCrc} testing.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class FastCrcTest {
    @Test
    void testCrcValue() {
        FastCrc fastCrc = new FastCrc();

        assertEquals(-1, fastCrc.getValue());

        byte[] bytes = createByteArray();

        fastCrc.update(ByteBuffer.wrap(bytes), bytes.length);

        assertEquals(getCrc(bytes), fastCrc.getValue());

        fastCrc.reset();

        assertEquals(-1, fastCrc.getValue());
    }

    @Test
    void testCalcCrcByteBuffer(@WorkDirectory Path workDir) throws Exception {
        byte[] bytes = createByteArray();

        Path testFilePath = workDir.resolve("test");

        Files.write(testFilePath, bytes);

        assertEquals(getCrc(bytes), calcCrc(testFilePath.toFile()));
    }

    @Test
    void testCalcCrcFile() {
        byte[] bytes = createByteArray();

        assertEquals(getCrc(bytes), calcCrc(ByteBuffer.wrap(bytes), bytes.length));
    }

    private byte[] createByteArray() {
        byte[] bytes = new byte[128];

        Arrays.fill(bytes, 0, 32, (byte) 1);
        Arrays.fill(bytes, 32, 64, (byte) 2);
        Arrays.fill(bytes, 64, 128, (byte) 3);

        return bytes;
    }

    private int getCrc(byte[] bytes) {
        CRC32 crc32 = new CRC32();

        crc32.update(bytes, 0, bytes.length);

        return ~(int) crc32.getValue();
    }
}
