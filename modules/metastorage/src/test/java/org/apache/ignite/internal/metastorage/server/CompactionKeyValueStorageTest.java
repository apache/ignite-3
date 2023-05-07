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
public abstract class CompactionKeyValueStorageTest extends AbstractKeyValueStorageTest {
    private final HybridClock clock = new HybridClockImpl();

    @Test
    public void testCompactionAfterLastRevision() {
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
    public void testCompactionAfterTombstone() {
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
    public void testCompactionBetweenMultipleWrites() {
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

        Entry entry4 = storage.get(key, lastRevision);
        assertArrayEquals(value4, entry4.value());

        Entry entry3 = storage.get(key, lastRevision - 1);
        assertArrayEquals(value3, entry3.value());

        Entry entry2 = storage.get(key, lastRevision - 2);
        assertArrayEquals(value2, entry2.value());

        // Previous value, must be removed due to compaction.
        Entry entry1 = storage.get(key, lastRevision - 3);
        assertTrue(entry1.empty());
    }

    @Test
    public void testCompactionAfterTombstoneRemovesTombstone() {
        byte[] key = key(0);
        byte[] value1 = keyValue(0, 0);
        byte[] value2 = keyValue(0, 1);

        storage.put(key, value1, clock.now());

        storage.remove(key, clock.now());

        HybridTimestamp ts = clock.now();

        storage.put(key, value2, clock.now());

        storage.remove(key, clock.now());

        long lastRevision = storage.revision();

        storage.compact(ts);

        // Last operation was remove, so this is a tombstone.
        Entry entry3 = storage.get(key, lastRevision);
        assertTrue(entry3.tombstone());

        Entry entry2 = storage.get(key, lastRevision - 1);
        assertArrayEquals(value2, entry2.value());

        // Previous value, must be removed due to compaction.
        Entry entry1 = storage.get(key, lastRevision - 2);
        assertTrue(entry1.empty());
    }

    @Test
    public void testCompactEmptyStorage() {
        storage.compact(clock.now());
    }

    @Test
    public void testCompactionBetweenRevisionsOfOneKey() {
        byte[] key = key(0);
        byte[] value1 = keyValue(0, 0);
        byte[] value2 = keyValue(0, 1);

        storage.put(key, value1, clock.now());

        storage.put(key(1), keyValue(1, 0), clock.now());

        HybridTimestamp ts = clock.now();

        storage.put(key, value2, clock.now());

        storage.compact(ts);

        // Both keys should exist, as low watermark's revision is higher than entry1's, but lesser than entry2's,
        // this means that entry1 is still needed.
        Entry entry2 = storage.get(key, storage.revision());
        assertArrayEquals(value2, entry2.value());

        Entry entry1 = storage.get(key, storage.revision() - 1);
        assertArrayEquals(value1, entry1.value());
    }

    @Test
    public void testInvokeCompactionBeforeAnyEntry() {
        byte[] key = key(0);
        byte[] value1 = keyValue(0, 0);
        byte[] value2 = keyValue(0, 1);

        HybridTimestamp ts = clock.now();

        storage.put(key, value1, clock.now());
        storage.put(key, value2, clock.now());

        storage.compact(ts);

        // No entry should be compacted.
        Entry entry2 = storage.get(key, storage.revision());
        assertArrayEquals(value2, entry2.value());

        Entry entry1 = storage.get(key, storage.revision() - 1);
        assertArrayEquals(value1, entry1.value());
    }
}
