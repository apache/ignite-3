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

import static org.apache.ignite.internal.pagememory.persistence.WriteSpeedFormatter.formatWriteSpeed;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.util.Constants;
import org.junit.jupiter.api.Test;

/** For {@link WriteSpeedFormatter} testing. */
public class WriteSpeedFormatterTest {
    @Test
    void testZeroDuration() {
        assertEquals("0", formatWriteSpeed(1, 0));
    }

    @Test
    void testZeroBytes() {
        assertEquals("0", formatWriteSpeed(0, 1));
    }

    @Test
    void testZeroBytesAndDuration() {
        assertEquals("0", formatWriteSpeed(0, 0));
    }

    @Test
    void testHighSpeed() {
        long bytes = 2L * Constants.GiB;
        long secInNanos = TimeUnit.SECONDS.toNanos(1);

        assertEquals("2048", formatWriteSpeed(bytes, secInNanos));
        assertEquals("1024", formatWriteSpeed(bytes, secInNanos * 2));
        assertEquals("4096", formatWriteSpeed(bytes, secInNanos / 2));

        assertEquals("205", formatWriteSpeed(bytes, secInNanos * 10));
        assertEquals("20480", formatWriteSpeed(bytes, secInNanos / 10));
    }

    @Test
    void testMediumSpeed() {
        long bytes = 2L * Constants.MiB;
        long secInNanos = TimeUnit.SECONDS.toNanos(1);

        assertEquals("2.00", formatWriteSpeed(bytes, secInNanos));
        assertEquals("1.00", formatWriteSpeed(bytes, secInNanos * 2));
        assertEquals("4.00", formatWriteSpeed(bytes, secInNanos / 2));

        assertEquals("0.20", formatWriteSpeed(bytes, secInNanos * 10));
        assertEquals("8.00", formatWriteSpeed(bytes, secInNanos / 4));
    }

    @Test
    void testLowSpeed() {
        long bytes = 2L * Constants.KiB;
        long secInNanos = TimeUnit.SECONDS.toNanos(1);

        assertEquals("0.0020", formatWriteSpeed(bytes, secInNanos));
        assertEquals("0.0010", formatWriteSpeed(bytes, secInNanos * 2));
        assertEquals("0.0039", formatWriteSpeed(bytes, secInNanos / 2));

        assertEquals("0.0002", formatWriteSpeed(bytes, secInNanos * 10));
        assertEquals("0.0078", formatWriteSpeed(bytes, secInNanos / 4));
    }
}
