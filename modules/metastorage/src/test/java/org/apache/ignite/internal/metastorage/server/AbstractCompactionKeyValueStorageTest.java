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
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.ignite.internal.util.Cursor;
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
        assertEquals(List.of(1, 2, 5/* Tombstone */), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6/* Tombstone */), collectRevisions(SOME_KEY));
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

    @Test
    void testGetAllLatestAndCompaction() {
        storage.setCompactionRevision(6);

        assertDoesNotThrow(() -> storage.getAll(List.of(FOO_KEY, BAR_KEY, NOT_EXISTS_KEY)));
    }

    @Test
    void testGetAllAndCompactionForFooKey() {
        // FOO_KEY has revisions: [1, 3, 5].
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetAll(1, FOO_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(2, FOO_KEY);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetAll(2, FOO_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(3, FOO_KEY);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetAll(3, FOO_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(4, FOO_KEY);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetAll(4, FOO_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(5, FOO_KEY);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetAll(4, FOO_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(5, FOO_KEY);

        storage.setCompactionRevision(6);
        assertThrowsCompactedExceptionForGetAll(4, FOO_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(5, FOO_KEY);
    }

    @Test
    void testGetAllAndCompactionForBarKey() {
        // BAR_KEY has revisions: [1, 2, 5 (tombstone)].
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetAll(1, BAR_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(2, BAR_KEY);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetAll(2, BAR_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(3, BAR_KEY);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetAll(3, BAR_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(4, BAR_KEY);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetAll(4, BAR_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(5, BAR_KEY);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetAll(5, BAR_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(6, BAR_KEY);

        storage.setCompactionRevision(6);
        assertThrowsCompactedExceptionForGetAll(6, BAR_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(7, BAR_KEY);
    }

    @Test
    void testGetAllAndCompactionForNotExistsKey() {
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetAll(1, NOT_EXISTS_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(2, NOT_EXISTS_KEY);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetAll(2, NOT_EXISTS_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(3, NOT_EXISTS_KEY);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetAll(3, NOT_EXISTS_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(4, NOT_EXISTS_KEY);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetAll(4, NOT_EXISTS_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(5, NOT_EXISTS_KEY);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetAll(5, NOT_EXISTS_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(6, NOT_EXISTS_KEY);

        storage.setCompactionRevision(6);
        assertThrowsCompactedExceptionForGetAll(6, NOT_EXISTS_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(7, NOT_EXISTS_KEY);
    }

    @Test
    void testGetAllAndCompactionForMultipleKeys() {
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetAll(1, FOO_KEY, BAR_KEY, NOT_EXISTS_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(2, FOO_KEY, BAR_KEY, NOT_EXISTS_KEY);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetAll(5, FOO_KEY, BAR_KEY, NOT_EXISTS_KEY);
        assertThrowsCompactedExceptionForGetAll(5, FOO_KEY, BAR_KEY);
        assertThrowsCompactedExceptionForGetAll(5, FOO_KEY, NOT_EXISTS_KEY);
        assertThrowsCompactedExceptionForGetAll(5, BAR_KEY, NOT_EXISTS_KEY);
        assertDoesNotThrowCompactedExceptionForGetAll(6, FOO_KEY, BAR_KEY, NOT_EXISTS_KEY);
    }

    @Test
    void testGetListAndCompactionForFooKey() {
        // FOO_KEY has revisions: [1, 3, 5].
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 1, 1);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 1, 2);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 1, 3);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 2, 2);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 2, 3);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 1, 1);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 1, 2);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 2, 2);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 1, 3);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 2, 3);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 3, 3);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 1, 3);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 2, 3);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 3, 3);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 3, 4);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 4, 4);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 1, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 2, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 4, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 3, 5);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 3, 4);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 4, 4);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 1, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 2, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 3, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 4, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 5, 5);

        storage.setCompactionRevision(5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 1, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 2, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 3, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 4, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 5, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 5, 6);

        storage.setCompactionRevision(6);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 1, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 2, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 3, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 4, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 5, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(FOO_KEY, 5, 6);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 6, 6);
        assertThrowsCompactedExceptionForGetList(FOO_KEY, 6, 7);
    }

    @Test
    void testGetListAndCompactionForBarKey() {
        // BAR_KEY has revisions: [1, 2, 5 (tombstone)].
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 1, 1);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 1, 2);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 1, 3);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 2, 2);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 2, 3);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 1, 1);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 1, 2);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 2, 2);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 1, 3);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 2, 3);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 3, 3);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 1, 3);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 2, 3);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 3, 3);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 3, 4);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 4, 4);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 1, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 2, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 4, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 3, 5);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 3, 4);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 4, 4);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 1, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 2, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 3, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 4, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 5, 5);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 1, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 2, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 3, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 4, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 5, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 5, 6);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 6, 6);

        storage.setCompactionRevision(6);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 1, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 2, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 3, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 4, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 5, 5);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 5, 6);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 6, 6);
        assertThrowsCompactedExceptionForGetList(BAR_KEY, 6, 7);
        assertDoesNotThrowsCompactedExceptionForGetList(BAR_KEY, 7, 7);
    }

    @Test
    void testGetListAndCompactionForNotExistsKey() {
        storage.setCompactionRevision(1);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 1, 1);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 1, 2);
        assertDoesNotThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 2, 2);

        storage.setCompactionRevision(2);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 1, 2);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 2, 2);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 2, 3);
        assertDoesNotThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 3, 3);

        storage.setCompactionRevision(3);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 2, 3);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 3, 3);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 3, 4);
        assertDoesNotThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 4, 4);

        storage.setCompactionRevision(4);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 3, 4);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 4, 4);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 4, 5);
        assertDoesNotThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 5, 5);

        storage.setCompactionRevision(5);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 4, 5);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 5, 5);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 5, 6);
        assertDoesNotThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 6, 6);

        storage.setCompactionRevision(6);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 5, 6);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 6, 6);
        assertThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 6, 7);
        assertDoesNotThrowsCompactedExceptionForGetList(NOT_EXISTS_KEY, 7, 7);
    }

    @Test
    void testRangeLatestAndCompaction() {
        storage.setCompactionRevision(6);

        assertDoesNotThrow(() -> {
            try (Cursor<Entry> cursor = storage.range(FOO_KEY, null)) {
                cursor.hasNext();
                cursor.next();
            }
        });
    }

    @Test
    void testRangeAndCompaction() {
        try (Cursor<Entry> cursorBeforeSetCompactionRevision = storage.range(FOO_KEY, null, 5)) {
            storage.setCompactionRevision(5);

            assertThrows(CompactedException.class, () -> storage.range(FOO_KEY, null, 1));
            assertThrows(CompactedException.class, () -> storage.range(FOO_KEY, null, 3));
            assertThrows(CompactedException.class, () -> storage.range(FOO_KEY, null, 5));

            assertDoesNotThrow(() -> {
                try (Cursor<Entry> range = storage.range(FOO_KEY, null, 6)) {
                    range.hasNext();
                    range.next();
                }
            });

            assertDoesNotThrow(cursorBeforeSetCompactionRevision::hasNext);
            assertDoesNotThrow(cursorBeforeSetCompactionRevision::next);
        }
    }

    /**
     * Tests {@link KeyValueStorage#range(byte[], byte[])} and {@link KeyValueStorage#range(byte[], byte[], long)} for the case when
     * cursors should or should not return the last element after compaction. For {@link #FOO_KEY} and {@link #BAR_KEY}, the last revisions
     * are 5, a regular entry and a tombstone. Keys with their revisions are added in {@link #setUp()}.
     *
     * <p>Consider the situation:</p>
     * <ul>
     *     <li>Made {@link KeyValueStorage#setCompactionRevision(long) KeyValueStorage.setCompactionRevision(5)}.</li>
     *     <li>Waited for all cursors to end with revision {@code 5} or less.</li>
     *     <li>Made {@link KeyValueStorage#compact(long) KeyValueStorage.compact(5)}.</li>
     *     <li>Invoke {@link KeyValueStorage#range} for last revision and revision {@code 6} for {@link #FOO_KEY} and {@link #BAR_KEY}.
     *     <ul>
     *         <li>For {@link #FOO_KEY}, we need to return a entry with revision {@code 5}, since it will not be removed from the storage
     *         after compaction.</li>
     *         <li>For {@link #BAR_KEY}, we should not return anything, since the key will be deleted after compaction.</li>
     *     </ul>
     *     </li>
     * </ul>
     */
    @Test
    void testRangeAndCompactionForCaseReadLastEntries() {
        storage.setCompactionRevision(5);

        try (
                Cursor<Entry> rangeFooKeyCursorLatest = storage.range(FOO_KEY, storage.nextKey(FOO_KEY));
                Cursor<Entry> rangeFooKeyCursorBounded = storage.range(FOO_KEY, storage.nextKey(FOO_KEY), 6);
                Cursor<Entry> rangeBarKeyCursorLatest = storage.range(BAR_KEY, storage.nextKey(BAR_KEY));
                Cursor<Entry> rangeBarKeyCursorBounded = storage.range(BAR_KEY, storage.nextKey(BAR_KEY), 6)
        ) {
            // Must see the latest revision of the FOO_KEY as it will not be removed from storage by the compaction.
            assertEquals(List.of(5L), collectRevisions(rangeFooKeyCursorLatest));
            assertEquals(List.of(5L), collectRevisions(rangeFooKeyCursorBounded));

            // Must not see the latest revision of the BAR_KEY, as it will have to be removed from the storage by the compaction.
            assertEquals(List.of(), collectRevisions(rangeBarKeyCursorLatest));
            assertEquals(List.of(), collectRevisions(rangeBarKeyCursorBounded));
        }
    }

    /**
     * Tests {@link KeyValueStorage#range(byte[], byte[])} and {@link KeyValueStorage#range(byte[], byte[], long)} for the case when they
     * were invoked on a revision (for example, on revision {@code 5}) before invoking
     * {@link KeyValueStorage#setCompactionRevision(long) KeyValueStorage.setCompactionRevision(5)} but before invoking
     * {@link KeyValueStorage#compact(long) KeyValueStorage.compact(5)}. Such cursors should return entries since nothing should be
     * removed yet until they are completed. Keys are chosen for convenience. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testRangeAfterSetCompactionRevisionButBeforeStartCompaction() {
        try (
                Cursor<Entry> rangeFooKeyCursorLatest = storage.range(FOO_KEY, storage.nextKey(FOO_KEY));
                Cursor<Entry> rangeFooKeyCursorBounded = storage.range(FOO_KEY, storage.nextKey(FOO_KEY), 5);
                Cursor<Entry> rangeBarKeyCursorLatest = storage.range(BAR_KEY, storage.nextKey(BAR_KEY));
                Cursor<Entry> rangeBarKeyCursorBounded = storage.range(BAR_KEY, storage.nextKey(BAR_KEY), 5)
        ) {
            storage.setCompactionRevision(5);

            assertEquals(List.of(5L), collectRevisions(rangeFooKeyCursorLatest));
            assertEquals(List.of(5L), collectRevisions(rangeFooKeyCursorBounded));

            assertEquals(List.of(5L), collectRevisions(rangeBarKeyCursorLatest));
            assertEquals(List.of(5L), collectRevisions(rangeBarKeyCursorBounded));
        }
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

    private List<Long> collectRevisions(Cursor<Entry> cursor) {
        return cursor.stream().map(Entry::revision).collect(toList());
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

    private void assertThrowsCompactedExceptionForGetAll(long endRevisionInclusive, byte[]... keys) {
        for (long i = 0; i <= endRevisionInclusive; i++) {
            long revisionUpperBound = i;

            assertThrows(
                    CompactedException.class,
                    () -> storage.getAll(List.of(keys), revisionUpperBound),
                    () -> String.format("keys=%s, revision=%s", toUtf8String(keys), revisionUpperBound)
            );
        }
    }

    private void assertDoesNotThrowCompactedExceptionForGetAll(long startRevisionInclusive, byte[]... keys) {
        for (long i = startRevisionInclusive; i <= storage.revision(); i++) {
            long revisionUpperBound = i;

            assertDoesNotThrow(
                    () -> storage.getAll(List.of(keys), revisionUpperBound),
                    () -> String.format("keys=%s, revision=%s", toUtf8String(keys), revisionUpperBound)
            );
        }
    }

    private void assertThrowsCompactedExceptionForGetList(byte[] key, long revLowerBound, long revUpperBound) {
        assertThrows(
                CompactedException.class,
                () -> storage.get(key, revLowerBound, revUpperBound),
                () -> String.format("key=%s, revLowerBound=%s, revUpperBound=%s", toUtf8String(key), revLowerBound, revUpperBound)
        );
    }

    private void assertDoesNotThrowsCompactedExceptionForGetList(byte[] key, long revLowerBound, long revUpperBound) {
        assertDoesNotThrow(
                () -> storage.get(key, revLowerBound, revUpperBound),
                () -> String.format("key=%s, revLowerBound=%s, revUpperBound=%s", toUtf8String(key), revLowerBound, revUpperBound)
        );
    }

    private static String toUtf8String(byte[]... keys) {
        return Arrays.stream(keys)
                .map(KeyValueStorageUtils::toUtf8String)
                .collect(joining(", ", "[", "]"));
    }
}
