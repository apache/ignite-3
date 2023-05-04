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

package org.apache.ignite.internal.metastorage.server;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.Entry;
import org.junit.jupiter.api.Test;

/** Compaction tests. */
public abstract class CompactionKeyValueStorageTest extends BaseKeyValueStorageTest {

    private final HybridClock clock = new HybridClockImpl();

    @Test
    public void test1() {
        byte[] key = key(0);
        byte[] value1 = keyValue(0, 0);
        byte[] value2 = keyValue(0, 1);

        storage.put(key, value1, clock.now());
        storage.put(key, value2, clock.now());

        long lastRevision = storage.revision();

        storage.compact(clock.now());

        // Latest value, must exist.
        Entry entry2 = storage.get(key, lastRevision);
        assertEquals(lastRevision, entry2.revision());
        assertArrayEquals(value2, entry2.value());

        // Previous value, must be removed due to compaction.
        Entry entry1 = storage.get(key, lastRevision - 1);
        assertTrue(entry1.empty());
    }

    @Test
    public void test2() {
        byte[] key = key(0);
        byte[] value1 = keyValue(0, 0);

        storage.put(key, value1, clock.now());
        storage.remove(key, clock.now());

        long lastRevision = storage.revision();

        storage.compact(clock.now());

        // Previous value, must be removed due to compaction.
        Entry entry2 = storage.get(key, lastRevision);
        assertTrue(entry2.empty());

        // Previous value, must be removed due to compaction.
        Entry entry1 = storage.get(key, lastRevision - 1);
        assertTrue(entry1.empty());
    }

    @Test
    public void test3() {
        byte[] key = key(0);
        byte[] value1 = keyValue(0, 0);
        byte[] value2 = keyValue(0, 1);
        byte[] value3 = keyValue(0, 2);
        byte[] value4 = keyValue(0, 3);

        storage.put(key, value1, clock.now());
        storage.put(key, value2, clock.now());
        HybridTimestamp ts = clock.now();
        storage.put(key, value3, clock.now());
        storage.put(key, value4, clock.now());

        long lastRevision = storage.revision();

        storage.compact(ts);

        // Previous value, must be removed due to compaction.
        Entry entry4 = storage.get(key, lastRevision);
        assertArrayEquals(value4, entry4.value());
    }

    @Test
    public void test4() {
        byte[] key = key(0);
        byte[] value1 = keyValue(0, 0);
        byte[] value2 = keyValue(0, 1);
        byte[] value3 = keyValue(0, 2);

        HybridTimestamp now1 = clock.now();
        storage.put(key, value1, now1);
        HybridTimestamp now2 = clock.now();
        storage.put(key, value2, now2);
        HybridTimestamp ts = clock.now();
        HybridTimestamp now3 = clock.now();
        storage.remove(key, now3);
        HybridTimestamp now4 = clock.now();
        storage.put(key, value3, now4);
        storage.remove(key, clock.now());

        long lastRevision = storage.revision();

        storage.compact(ts);

        // Previous value, must be removed due to compaction.
        Entry entry4 = storage.get(key, lastRevision - 1);
        assertArrayEquals(value3, entry4.value());
    }
}
