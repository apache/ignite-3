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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Compaction tests. */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractCompactionKeyValueStorageTest extends AbstractKeyValueStorageTest {
    @WorkDirectory
    Path workDir;

    private final HybridClock clock = new HybridClockImpl();

    @Override
    @BeforeEach
    void setUp() {
        super.setUp();

        // TODO: IGNITE-23290 потом уйдёт
        storage.put(key(0), key(0), clock.now());
        storage.put(key(1), key(0), clock.now());
        storage.put(key(2), key(0), clock.now());
        storage.put(key(3), key(0), clock.now());
    }

    abstract boolean isPersistent();

    abstract void restartStorage(boolean clear) throws Exception;

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
        byte[] value = keyValue(0, 0);

        storage.put(key, value, clock.now());
        storage.remove(key, clock.now());

        long lastRevision = storage.revision();

        storage.compact(clock.now());

        // Current value, must be removed due to being a tombstone.
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

        HybridTimestamp compactTs = clock.now();

        storage.put(key, value3, clock.now());
        storage.put(key, value4, clock.now());

        long lastRevision = storage.revision();

        storage.compact(compactTs);

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

        HybridTimestamp compactTs = clock.now();

        storage.put(key, value2, clock.now());

        storage.remove(key, clock.now());

        long lastRevision = storage.revision();

        storage.compact(compactTs);

        // Last operation was remove, so this is a tombstone.
        Entry entry4 = storage.get(key, lastRevision);
        assertTrue(entry4.tombstone());

        Entry entry3 = storage.get(key, lastRevision - 1);
        assertArrayEquals(value2, entry3.value());

        // Previous value, must be removed due to compaction.
        Entry entry2 = storage.get(key, lastRevision - 2);
        assertTrue(entry2.empty());

        Entry entry1 = storage.get(key, lastRevision - 3);
        assertTrue(entry1.empty());
    }

    @Test
    public void testCompactEmptyStorage() {
        storage.compact(clock.now());
    }

    @Test
    public void testCompactionBetweenRevisionsOfOneKey() {
        byte[] key = key(0);
        byte[] value11 = keyValue(0, 0);
        byte[] value12 = keyValue(0, 1);

        storage.put(key, value11, clock.now());

        byte[] key2 = key(1);
        byte[] value2 = keyValue(1, 0);
        storage.put(key2, value2, clock.now());

        HybridTimestamp compactTs = clock.now();

        storage.put(key, value12, clock.now());

        storage.compact(compactTs);

        // Both keys should exist, as low watermark's revision is higher than entry11's, but lesser than entry12's,
        // this means that entry1 is still needed.
        Entry entry12 = storage.get(key, storage.revision());
        assertArrayEquals(value12, entry12.value());

        Entry entry11 = storage.get(key, storage.revision() - 1);
        assertArrayEquals(value11, entry11.value());

        Entry entry2 = storage.get(key2, storage.revision());
        assertArrayEquals(value2, entry2.value());
    }

    @Test
    public void testInvokeCompactionBeforeAnyEntry() {
        byte[] key = key(0);
        byte[] value1 = keyValue(0, 0);
        byte[] value2 = keyValue(0, 1);

        HybridTimestamp compactTs = clock.now();

        storage.put(key, value1, clock.now());
        storage.put(key, value2, clock.now());

        storage.compact(compactTs);

        // No entry should be compacted.
        Entry entry2 = storage.get(key, storage.revision());
        assertArrayEquals(value2, entry2.value());

        Entry entry1 = storage.get(key, storage.revision() - 1);
        assertArrayEquals(value1, entry1.value());
    }

    @Test
    void testSetAndGetCompactionRevision() {
        assertEquals(-1, storage.getCompactionRevision());

        storage.setCompactionRevision(0);
        assertEquals(0, storage.getCompactionRevision());

        storage.setCompactionRevision(1);
        assertEquals(1, storage.getCompactionRevision());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSetAndGetCompactionRevisionAndRestart(boolean clearStorage) throws Exception {
        storage.setCompactionRevision(1);

        restartStorage(clearStorage);
        assertEquals(-1, storage.getCompactionRevision());
    }

    @Test
    void testSaveCompactionRevision() {
        assumeTrue(isPersistent());

        storage.saveCompactionRevision(0);
        assertEquals(-1, storage.getCompactionRevision());

        storage.saveCompactionRevision(1);
        assertEquals(-1, storage.getCompactionRevision());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSaveCompactionRevisionAndRestart(boolean clearStorage) throws Exception {
        assumeTrue(isPersistent());

        storage.saveCompactionRevision(1);

        restartStorage(clearStorage);

        assertEquals(-1, storage.getCompactionRevision());
    }

    @Test
    void testSaveCompactionRevisionInSnapshot() {
        assumeTrue(isPersistent());

        storage.saveCompactionRevision(1);

        Path snapshotDir = workDir.resolve("snapshot");

        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());
        assertEquals(-1, storage.getCompactionRevision());

        storage.restoreSnapshot(snapshotDir);
        assertEquals(1, storage.getCompactionRevision());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSaveCompactionRevisionInSnapshotAndRestart(boolean clearStorage) throws Exception {
        assumeTrue(isPersistent());

        storage.saveCompactionRevision(1);

        Path snapshotDir = workDir.resolve("snapshot");
        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());

        restartStorage(clearStorage);

        storage.restoreSnapshot(snapshotDir);
        assertEquals(1, storage.getCompactionRevision());
    }
}
