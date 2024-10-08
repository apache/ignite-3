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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.toUtf8String;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.impl.CommandIdGenerator;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition.Type;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/** Compaction tests. */
@ExtendWith(WorkDirectoryExtension.class)
public abstract class AbstractCompactionKeyValueStorageTest extends AbstractKeyValueStorageTest {
    private static final byte[] FOO_KEY = fromString("foo");

    private static final byte[] BAR_KEY = fromString("bar");

    private static final byte[] SOME_KEY = fromString("someKey");

    private static final byte[] SOME_VALUE = fromString("someValue");

    private static final byte[] NOT_EXISTS_KEY = fromString("notExistsKey");

    @WorkDirectory
    Path workDir;

    private final HybridClock clock = new HybridClockImpl();

    @Override
    @BeforeEach
    void setUp() {
        super.setUp();

        // Revision = 1.
        storage.putAll(List.of(FOO_KEY, BAR_KEY), List.of(SOME_VALUE, SOME_VALUE), clock.now());
        // Revision = 2.
        storage.put(BAR_KEY, SOME_VALUE, clock.now());
        // Revision = 3.
        storage.put(FOO_KEY, SOME_VALUE, clock.now());
        // Revision = 4.
        storage.put(SOME_KEY, SOME_VALUE, clock.now());

        var fooKey = new ByteArray(FOO_KEY);
        var barKey = new ByteArray(BAR_KEY);

        // Revision = 5.
        var iif = new If(
                new AndCondition(new ExistenceCondition(Type.EXISTS, FOO_KEY), new ExistenceCondition(Type.EXISTS, BAR_KEY)),
                new Statement(ops(put(fooKey, SOME_VALUE), remove(barKey)).yield()),
                new Statement(ops(noop()).yield())
        );

        storage.invoke(iif, clock.now(), new CommandIdGenerator(UUID::randomUUID).newId());

        // Revision = 6.
        storage.remove(SOME_KEY, clock.now());

        // Revision = 7.
        // Special revision update to prevent tests from failing.
        storage.put(fromString("fake"), SOME_VALUE, clock.now());

        assertEquals(7, storage.revision());
        assertEquals(List.of(1, 3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(1, 2, 5 /** Tombstone */), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6 /** Tombstone */), collectRevisions(SOME_KEY));
    }

    @Test
    void testCompactRevision1() {
        storage.compact(1);

        assertEquals(List.of(3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(2, 5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    @Test
    void testCompactRevision2() {
        storage.compact(2);

        assertEquals(List.of(3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    @Test
    void testCompactRevision3() {
        storage.compact(3);

        assertEquals(List.of(5), collectRevisions(FOO_KEY));
        assertEquals(List.of(5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    @Test
    void testCompactRevision4() {
        storage.compact(4);

        assertEquals(List.of(5), collectRevisions(FOO_KEY));
        assertEquals(List.of(5), collectRevisions(BAR_KEY));
        assertEquals(List.of(6), collectRevisions(SOME_KEY));
    }

    @Test
    void testCompactRevision5() {
        storage.compact(5);

        assertEquals(List.of(5), collectRevisions(FOO_KEY));
        assertEquals(List.of(), collectRevisions(BAR_KEY));
        assertEquals(List.of(6), collectRevisions(SOME_KEY));
    }

    @Test
    void testCompactRevision6() {
        storage.compact(6);

        assertEquals(List.of(5), collectRevisions(FOO_KEY));
        assertEquals(List.of(), collectRevisions(BAR_KEY));
        assertEquals(List.of(), collectRevisions(SOME_KEY));
    }

    @Test
    void testCompactRevisionSequentially() {
        testCompactRevision1();
        testCompactRevision2();
        testCompactRevision3();
        testCompactRevision4();
        testCompactRevision5();
        testCompactRevision6();
    }

    @Test
    void testRevisionsAfterRestart() {
        storage.compact(6);

        Path snapshotDir = workDir.resolve("snapshot");

        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());

        storage.restoreSnapshot(snapshotDir);

        assertEquals(List.of(5), collectRevisions(FOO_KEY));
        assertEquals(List.of(), collectRevisions(BAR_KEY));
        assertEquals(List.of(), collectRevisions(SOME_KEY));
    }

    @Test
    void testCompactBeforeStopIt() {
        storage.stopCompaction();

        storage.compact(6);

        assertEquals(List.of(1, 3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(1, 2, 5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    @Test
    void testSetAndGetCompactionRevision() {
        assertEquals(-1, storage.getCompactionRevision());

        storage.setCompactionRevision(0);
        assertEquals(0, storage.getCompactionRevision());

        storage.setCompactionRevision(1);
        assertEquals(1, storage.getCompactionRevision());
    }

    @Test
    void testSetAndGetCompactionRevisionAndRestart() throws Exception {
        storage.setCompactionRevision(1);

        restartStorage();
        assertEquals(-1, storage.getCompactionRevision());
    }

    @Test
    void testSaveCompactionRevisionDoesNotChangeRevisionInMemory() {
        storage.saveCompactionRevision(0);
        assertEquals(-1, storage.getCompactionRevision());

        storage.saveCompactionRevision(1);
        assertEquals(-1, storage.getCompactionRevision());
    }

    @Test
    void testSaveCompactionRevisionAndRestart() throws Exception {
        storage.saveCompactionRevision(1);

        restartStorage();

        assertEquals(-1, storage.getCompactionRevision());
    }

    @Test
    void testSaveCompactionRevisionInSnapshot() {
        storage.saveCompactionRevision(1);

        Path snapshotDir = workDir.resolve("snapshot");

        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());
        assertEquals(-1, storage.getCompactionRevision());

        storage.restoreSnapshot(snapshotDir);
        assertEquals(1, storage.getCompactionRevision());
    }

    @Test
    void testSaveCompactionRevisionInSnapshotAndRestart() throws Exception {
        storage.saveCompactionRevision(1);

        Path snapshotDir = workDir.resolve("snapshot");
        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());

        restartStorage();

        storage.restoreSnapshot(snapshotDir);
        assertEquals(1, storage.getCompactionRevision());
    }

    @Test
    void testCompactDontSetAndSaveCompactionRevision() throws Exception {
        storage.compact(1);
        assertEquals(-1, storage.getCompactionRevision());

        restartStorage();
        assertEquals(-1, storage.getCompactionRevision());
    }

    @Test
    void testRestoreFromSnapshotWithoutSaveCompactionRevision() throws Exception {
        Path snapshotDir = workDir.resolve("snapshot");
        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());

        restartStorage();

        storage.restoreSnapshot(snapshotDir);
        assertEquals(-1, storage.getCompactionRevision());
    }

    @Test
    void testEntryOperationTimestampAfterCompaction() {
        storage.compact(6);

        assertNull(storage.get(BAR_KEY).timestamp());
    }

    @Test
    void testTimestampByRevisionAfterCompaction() {
        storage.compact(1);

        assertThrows(CompactedException.class, () -> storage.timestampByRevision(1));
        assertDoesNotThrow(() -> storage.timestampByRevision(2));
        assertDoesNotThrow(() -> storage.timestampByRevision(3));

        storage.compact(2);

        assertThrows(CompactedException.class, () -> storage.timestampByRevision(1));
        assertThrows(CompactedException.class, () -> storage.timestampByRevision(2));
        assertDoesNotThrow(() -> storage.timestampByRevision(3));

        storage.compact(3);

        assertThrows(CompactedException.class, () -> storage.timestampByRevision(1));
        assertThrows(CompactedException.class, () -> storage.timestampByRevision(2));
        assertThrows(CompactedException.class, () -> storage.timestampByRevision(3));
    }

    @Test
    void testRevisionByTimestampAfterCompaction() {
        HybridTimestamp timestamp1 = storage.timestampByRevision(1);
        HybridTimestamp timestamp2 = storage.timestampByRevision(2);
        HybridTimestamp timestamp3 = storage.timestampByRevision(3);

        storage.compact(1);

        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp1));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp1.subtractPhysicalTime(1)));
        assertDoesNotThrow(() -> storage.revisionByTimestamp(timestamp2));
        assertDoesNotThrow(() -> storage.revisionByTimestamp(timestamp3));

        storage.compact(2);

        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp1));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp1.subtractPhysicalTime(1)));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp2));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp2.subtractPhysicalTime(1)));
        assertDoesNotThrow(() -> storage.revisionByTimestamp(timestamp3));

        storage.compact(3);

        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp1));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp1.subtractPhysicalTime(1)));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp2));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp2.subtractPhysicalTime(1)));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp3));
        assertThrows(CompactedException.class, () -> storage.revisionByTimestamp(timestamp3.subtractPhysicalTime(1)));
    }

    @Test
    void testGetSingleEntryLatestAndCompaction() {
        storage.setCompactionRevision(6);

        assertDoesNotThrow(() -> storage.get(FOO_KEY));
        assertDoesNotThrow(() -> storage.get(BAR_KEY));
        assertDoesNotThrow(() -> storage.get(NOT_EXISTS_KEY));
    }

    @Test
    void testGetSingleEntryAndCompactionForFooKey() {
        // FOO_KEY has revisions: [1, 3, 5].
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetSingleValue(FOO_KEY, 1);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(FOO_KEY, 2);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetSingleValue(FOO_KEY, 2);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(FOO_KEY, 3);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetSingleValue(FOO_KEY, 3);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(FOO_KEY, 4);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetSingleValue(FOO_KEY, 4);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(FOO_KEY, 5);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetSingleValue(FOO_KEY, 4);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(FOO_KEY, 5);

        storage.setCompactionRevision(6);
        assertThrowsCompactedExceptionForGetSingleValue(FOO_KEY, 4);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(FOO_KEY, 5);
    }

    @Test
    void testGetSingleEntryAndCompactionForBarKey() {
        // BAR_KEY has revisions: [1, 2, 5 (tombstone)].
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetSingleValue(BAR_KEY, 1);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(BAR_KEY, 2);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetSingleValue(BAR_KEY, 2);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(BAR_KEY, 3);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetSingleValue(BAR_KEY, 3);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(BAR_KEY, 4);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetSingleValue(BAR_KEY, 4);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(BAR_KEY, 5);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetSingleValue(BAR_KEY, 5);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(BAR_KEY, 6);

        storage.setCompactionRevision(6);
        assertThrowsCompactedExceptionForGetSingleValue(BAR_KEY, 6);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(BAR_KEY, 7);
    }

    @Test
    void testGetSingleEntryAndCompactionForNotExistsKey() {
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 1);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 2);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 2);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 3);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 3);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 4);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 4);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 5);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 5);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 6);

        storage.setCompactionRevision(6);
        assertThrowsCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 6);
        assertDoesNotThrowCompactedExceptionForGetSingleValue(NOT_EXISTS_KEY, 7);
    }

    private List<Integer> collectRevisions(byte[] key) {
        var revisions = new ArrayList<Integer>();

        for (int revision = 0; revision <= storage.revision(); revision++) {
            Entry entry = storage.get(key, revision);

            if (!entry.empty() && entry.revision() == revision) {
                revisions.add(revision);
            }
        }

        return revisions;
    }

    private static byte[] fromString(String s) {
        return s.getBytes(UTF_8);
    }

    private void assertThrowsCompactedExceptionForGetSingleValue(byte[] key, long endRevisionInclusive) {
        for (long i = 0; i <= endRevisionInclusive; i++) {
            long revisionUpperBound = i;

            assertThrows(
                    CompactedException.class,
                    () -> storage.get(key, revisionUpperBound),
                    () -> String.format("key=%s, revision=%s", toUtf8String(key), revisionUpperBound)
            );
        }
    }

    private void assertDoesNotThrowCompactedExceptionForGetSingleValue(byte[] key, long startRevisionInclusive) {
        for (long i = startRevisionInclusive; i <= storage.revision(); i++) {
            long revisionUpperBound = i;

            assertDoesNotThrow(
                    () -> storage.get(key, revisionUpperBound),
                    () -> String.format("key=%s, revision=%s", toUtf8String(key), revisionUpperBound)
            );
        }
    }
}
