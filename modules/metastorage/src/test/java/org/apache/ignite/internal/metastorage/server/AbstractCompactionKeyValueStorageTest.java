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
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.server.KeyValueUpdateContext.kvContext;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willTimeoutFast;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.impl.CommandIdGenerator;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition.Type;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.junit.jupiter.api.AfterEach;
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

    private final ClusterTimeImpl clusterTime = new ClusterTimeImpl(NODE_NAME, new IgniteSpinBusyLock(), clock);

    private final PendingComparableValuesTracker<Long, Void> updateCompactionRevisionInWatchEvenQueue
            = new PendingComparableValuesTracker<>(Long.MIN_VALUE);

    ReadOperationForCompactionTracker readOperationForCompactionTracker = new ReadOperationForCompactionTracker();

    @Override
    @BeforeEach
    void setUp() {
        super.setUp();

        // Revision = 1.
        storage.putAll(List.of(FOO_KEY, BAR_KEY), List.of(SOME_VALUE, SOME_VALUE), kvContext(clock.now()));
        // Revision = 2.
        storage.put(BAR_KEY, SOME_VALUE, kvContext(clock.now()));
        // Revision = 3.
        storage.put(FOO_KEY, SOME_VALUE, kvContext(clock.now()));
        // Revision = 4.
        storage.put(SOME_KEY, SOME_VALUE, kvContext(clock.now()));

        var fooKey = new ByteArray(FOO_KEY);
        var barKey = new ByteArray(BAR_KEY);

        // Revision = 5.
        var iif = new If(
                new AndCondition(new ExistenceCondition(Type.EXISTS, FOO_KEY), new ExistenceCondition(Type.EXISTS, BAR_KEY)),
                new Statement(ops(put(fooKey, SOME_VALUE), remove(barKey)).yield()),
                new Statement(ops(noop()).yield())
        );

        storage.invoke(iif, kvContext(clock.now()), new CommandIdGenerator(UUID.randomUUID()).newId());

        // Revision = 6.
        storage.remove(SOME_KEY, kvContext(clock.now()));

        // Revision = 7.
        // Special revision update to prevent tests from failing.
        storage.put(fromString("fake"), SOME_VALUE, kvContext(clock.now()));

        assertEquals(7, storage.revision());
        assertEquals(List.of(1, 3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(1, 2, 5/* Tombstone */), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6/* Tombstone */), collectRevisions(SOME_KEY));

        storage.registerCompactionRevisionUpdateListener(
                compactionRevision -> updateCompactionRevisionInWatchEvenQueue.update(compactionRevision, null)
        );
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        closeAllManually(super::tearDown, clusterTime);
    }

    /**
     * Tests {@link KeyValueStorage#compact(long)} for a specific single revision, to simplify testing, see examples in the method
     * description. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testCompactRevision1() {
        storage.compact(1);

        assertEquals(List.of(1, 3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(2, 5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    /**
     * Tests {@link KeyValueStorage#compact(long)} for a specific single revision, to simplify testing, see examples in the method
     * description. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testCompactRevision2() {
        storage.compact(2);

        assertEquals(List.of(3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(2, 5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    /**
     * Tests {@link KeyValueStorage#compact(long)} for a specific single revision, to simplify testing, see examples in the method
     * description. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testCompactRevision3() {
        storage.compact(3);

        assertEquals(List.of(3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(2, 5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    /**
     * Tests {@link KeyValueStorage#compact(long)} for a specific single revision, to simplify testing, see examples in the method
     * description. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testCompactRevision4() {
        storage.compact(4);

        assertEquals(List.of(5), collectRevisions(FOO_KEY));
        assertEquals(List.of(5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    /**
     * Tests {@link KeyValueStorage#compact(long)} for a specific single revision, to simplify testing, see examples in the method
     * description. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testCompactRevision5() {
        storage.compact(5);

        assertEquals(List.of(5), collectRevisions(FOO_KEY));
        assertEquals(List.of(), collectRevisions(BAR_KEY));
        assertEquals(List.of(6), collectRevisions(SOME_KEY));
    }

    /**
     * Tests {@link KeyValueStorage#compact(long)} for a specific single revision, to simplify testing, see examples in the method
     * description. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testCompactRevision6() {
        storage.compact(6);

        assertEquals(List.of(5), collectRevisions(FOO_KEY));
        assertEquals(List.of(), collectRevisions(BAR_KEY));
        assertEquals(List.of(), collectRevisions(SOME_KEY));
    }

    /**
     * Tests that compaction works correctly when it updates a large number of keys at the same time.
     */
    @Test
    void testCompactLargeNumberOfKeys() {
        int count = 1000;

        List<byte[]> keys = IntStream.range(0, count).mapToObj(k -> ("uniqueKey" + k).getBytes(UTF_8)).collect(toList());

        for (int i = 0; i < 2; i++) {
            storage.putAll(
                    keys,
                    Collections.nCopies(count, new byte[5]),
                    kvContext(clock.now())
            );
        }

        long revision = storage.revision();
        storage.compact(revision - 1);

        for (byte[] key : keys) {
            //noinspection NumericCastThatLosesPrecision
            assertEquals(List.of((int) revision), collectRevisions(key));
        }
    }

    /**
     * Tests {@link KeyValueStorage#compact(long)} as if it were called for each revision sequentially, see examples in the method
     * description. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testCompactRevisionSequentially() {
        testCompactRevision1();
        testCompactRevision2();
        testCompactRevision3();
        testCompactRevision4();
        testCompactRevision5();
        testCompactRevision6();
    }

    /**
     * Tests that after the storage is recovered, compacted keys will not be returned. Keys with their revisions are added in
     * {@link #setUp()}.
     */
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

    /**
     * Tests stopping the compaction. Since it is impossible to predict what the result will be if you stop somewhere in the middle of the
     * compaction, it is easiest to stop before the compaction starts. Keys with their revisions are added in {@link #setUp()}.
     */
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
    void testSaveCompactionRevision() {
        HybridTimestamp now0 = clock.now();
        HybridTimestamp now1 = clock.now();

        startWatches();

        storage.saveCompactionRevision(0, new KeyValueUpdateContext(1, 1, now0));
        assertEquals(-1, storage.getCompactionRevision());
        assertEquals(new IndexWithTerm(1, 1), storage.getIndexWithTerm());
        assertThat(clusterTime.waitFor(now0), willCompleteSuccessfully());

        storage.saveCompactionRevision(1, new KeyValueUpdateContext(2, 2, now1));
        assertEquals(-1, storage.getCompactionRevision());
        assertEquals(new IndexWithTerm(2, 2), storage.getIndexWithTerm());
        assertThat(clusterTime.waitFor(now1), willCompleteSuccessfully());
    }

    @Test
    void testSaveCompactionRevisionAndRestart() throws Exception {
        storage.saveCompactionRevision(1, new KeyValueUpdateContext(1, 1, clock.now()));

        restartStorage();

        // No safe time check as it is only saved with revision update.
        assertEquals(1, storage.getCompactionRevision());
        assertEquals(new IndexWithTerm(1, 1), storage.getIndexWithTerm());
    }

    @Test
    void testSaveCompactionRevisionInSnapshot() {
        storage.saveCompactionRevision(1, new KeyValueUpdateContext(1, 1, clock.now()));

        Path snapshotDir = workDir.resolve("snapshot");

        // No safe time check as it is only saved with revision update.
        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());
        assertEquals(-1, storage.getCompactionRevision());
        assertEquals(new IndexWithTerm(1, 1), storage.getIndexWithTerm());

        // No safe time check as it is only saved with revision update.
        storage.restoreSnapshot(snapshotDir);
        assertEquals(1, storage.getCompactionRevision());
        assertEquals(new IndexWithTerm(1, 1), storage.getIndexWithTerm());
    }

    @Test
    void testSaveCompactionRevisionInSnapshotAndRestart() throws Exception {
        storage.saveCompactionRevision(1, new KeyValueUpdateContext(1, 1, clock.now()));

        Path snapshotDir = workDir.resolve("snapshot");
        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());

        restartStorage();

        // No safe time check as it is only saved with revision update.
        storage.restoreSnapshot(snapshotDir);
        assertEquals(1, storage.getCompactionRevision());
        assertEquals(new IndexWithTerm(1, 1), storage.getIndexWithTerm());
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

    /**
     * Tests {@link Entry#timestamp()} for a key that will be fully removed from storage after compaction. This case would be suitable for
     * the {@link #BAR_KEY}, since its last revision is a tombstone. Keys with their revisions are added in {@link #setUp()}.
     */
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

    /**
     * Tests that {@link KeyValueStorage#get(byte[])} will not throw the {@link CompactedException} for all keys after compacting to the
     * penultimate repository revision. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testGetSingleEntryLatestAndCompaction() {
        storage.setCompactionRevision(6);

        assertDoesNotThrow(() -> storage.get(FOO_KEY));
        assertDoesNotThrow(() -> storage.get(BAR_KEY));
        assertDoesNotThrow(() -> storage.get(NOT_EXISTS_KEY));
    }

    /**
     * Tests {@link KeyValueStorage#get(byte[], long)} using examples from the description only for the {@link #FOO_KEY} for which the last
     * revision is <b>not</b> tombstone. Only one key is considered so that the tests are not too long. Keys with their revisions are
     * added in {@link #setUp()}.
     */
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
    void testDoNotDeleteOnExactMatchCompaction() {
        // FOO_KEY has revisions: [1, 3, 5].
        storage.setCompactionRevision(3);

        // Value is visible from the perspective of revision 4.
        Entry entryBefore = storage.get(FOO_KEY, 4);
        assertEquals(3, entryBefore.revision());

        storage.compact(3);

        // 4 is above 3, so reads from the perspective of revision 4 should keep working the same way.
        Entry entryAfter = storage.get(FOO_KEY, 4);
        assertEquals(entryBefore, entryAfter);
    }

    @Test
    void testReadCompactedTombstoneConcurrent() {
        KeyValueUpdateContext context = kvContext(hybridTimestamp(10L));

        byte[] extraKey = key(-1);
        byte[] extraValue = keyValue(0, 0);

        for (int i = 0; i < 1000; i++) {
            byte[] key = key(i);
            byte[] value = keyValue(i, i);

            storage.put(key, value, context);
            storage.remove(key, context);

            long revision = storage.revision();

            // Increase revision, so that compaction on "revision" can safely be executed.
            storage.put(extraKey, extraValue, context);

            runRace(
                    () -> {
                        storage.setCompactionRevision(revision);
                        storage.compact(revision);
                    },
                    () -> {
                        // We update the same value in order to cause a race in "writeBatch" method, that would leave already compacted
                        // revisions in a list of revisions associated with this key.
                        storage.put(key, value, context);
                    }
            );

            // Not a real assertion. It's for a developer that reads this test. Yes, you.
            assertEquals(revision + 2, storage.revision());

            long nextToLast = storage.revision() - 1;

            // Here we read the previous value of the key that's been inserted right now. This read from the storage must NOT fail.
            // WatchProcessor does exactly the same thing, and it must work, otherwise Ignite nodes will fail in "remove+put" scenarios.
            assertDoesNotThrow(() -> {
                Entry entry = storage.get(key, nextToLast);

                assertTrue(entry.empty());
            });

            // Check that compaction itself can handle the same case with a "writeBatch" race.
            storage.setCompactionRevision(nextToLast);
            storage.compact(nextToLast);
            storage.remove(key, context);
        }
    }

    /**
     * Tests {@link KeyValueStorage#get(byte[], long)} using examples from the description only for the {@link #BAR_KEY} for which the last
     * revision is tombstone. Only one key is considered so that the tests are not too long. Keys with their revisions are added in
     * {@link #setUp()}.
     */
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

    /**
     * Tests {@link KeyValueStorage#get(byte[], long)} using examples from the description only for the {@link #NOT_EXISTS_KEY} for which
     * was never present in the storage. Only one key is considered so that the tests are not too long. Keys with their revisions are added
     * in {@link #setUp()}.
     */
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

    /**
     * Tests that {@link KeyValueStorage#getAll(List)} will not throw the {@link CompactedException} for all keys after compacting to the
     * penultimate repository revision. Keys with their revisions are added in {@link #setUp()}.
     */
    @Test
    void testGetAllLatestAndCompaction() {
        storage.setCompactionRevision(6);

        assertDoesNotThrow(() -> storage.getAll(List.of(FOO_KEY, BAR_KEY, NOT_EXISTS_KEY)));
    }

    /**
     * Tests {@link KeyValueStorage#getAll(List, long)} using examples from the description only for the {@link #FOO_KEY} for which the
     * last revision is <b>not</b> tombstone. Only one key is considered so that the tests are not too long. Keys with their revisions are
     * added in {@link #setUp()}.
     */
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

    /**
     * Tests {@link KeyValueStorage#getAll(List, long)} using examples from the description only for the {@link #BAR_KEY} for which the
     * last revision is tombstone. Only one key is considered so that the tests are not too long. Keys with their revisions are added in
     * {@link #setUp()}.
     */
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

    /**
     * Tests {@link KeyValueStorage#getAll(List, long)} using examples from the description only for the {@link #NOT_EXISTS_KEY} for which
     * was never present in the storage. Only one key is considered so that the tests are not too long. Keys with their revisions are added
     * in {@link #setUp()}.
     */
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

    /**
     * Tests {@link KeyValueStorage#getAll(List, long)} using examples from the description for several keys at once; it is enough to
     * consider only two cases of compaction. Only one key is considered so that the tests are not too long. Keys with their revisions are
     * added in {@link #setUp()}.
     */
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

    /**
     * Tests that {@link KeyValueStorage#range(byte[], byte[])} and cursor methods will not throw {@link CompactedException} after
     * compacting to the penultimate revision. The key is chosen randomly. Keys with their revisions are added in {@link #setUp()}.
     */
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

    /**
     * Tests {@link KeyValueStorage#range(byte[], byte[], long)} and cursor methods as described in the method. The key is chosen randomly.
     * Keys with their revisions are added in {@link #setUp()}.
     */
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

            assertEquals(List.of(), collectRevisions(rangeBarKeyCursorLatest));
            assertEquals(List.of(), collectRevisions(rangeBarKeyCursorBounded));
        }
    }

    @Test
    void testReadOperationsFutureWithoutReadOperations() {
        assertTrue(readOperationForCompactionTracker.collect(0).isDone());
        assertTrue(readOperationForCompactionTracker.collect(1).isDone());
    }

    /**
     * Tests tracking only read operations from the storage, reading from the leader tracks {@link MetaStorageManagerImpl} itself.
     * <ul>
     *     <li>Create read operations, we only need cursors since reading one entry or a batch is synchronized with
     *     {@link KeyValueStorage#setCompactionRevision}.</li>
     *     <li>Set a new compaction revision via {@link KeyValueStorage#setCompactionRevision}.</li>
     *     <li>Wait for the completion of read operations on the new compaction revision.</li>
     * </ul>
     *
     * <p>The keys are chosen randomly. Keys with their revisions are added in {@link #setUp()}.</p>
     */
    @Test
    void testReadOperationsFuture() throws Exception {
        Cursor<Entry> range0 = storage.range(FOO_KEY, FOO_KEY);
        Cursor<Entry> range1 = storage.range(BAR_KEY, BAR_KEY, 5);

        try {
            storage.setCompactionRevision(3);

            CompletableFuture<Void> readOperationsFuture = readOperationForCompactionTracker.collect(3);
            assertFalse(readOperationsFuture.isDone());

            range0.stream().forEach(entry -> {});
            assertFalse(readOperationsFuture.isDone());

            range1.stream().forEach(entry -> {});
            assertFalse(readOperationsFuture.isDone());

            range0.close();
            assertFalse(readOperationsFuture.isDone());

            range1.close();
            assertTrue(readOperationsFuture.isDone());
        } catch (Throwable t) {
            IgniteUtils.closeAll(range0, range1);
        }
    }

    /**
     * Tests that cursors created after {@link KeyValueStorage#setCompactionRevision} will not affect future from
     * {@link ReadOperationForCompactionTracker#collect} on a new compaction revision.
     */
    @Test
    void testReadOperationsFutureForReadOperationAfterSetCompactionRevision() throws Exception {
        Cursor<Entry> rangeBeforeSetCompactionRevision = storage.range(FOO_KEY, FOO_KEY);
        Cursor<Entry> rangeAfterSetCompactionRevision0 = null;
        Cursor<Entry> rangeAfterSetCompactionRevision1 = null;

        try {
            storage.setCompactionRevision(3);

            rangeAfterSetCompactionRevision0 = storage.range(FOO_KEY, FOO_KEY);
            rangeAfterSetCompactionRevision1 = storage.range(FOO_KEY, FOO_KEY, 5);

            CompletableFuture<Void> readOperationsFuture = readOperationForCompactionTracker.collect(3);
            assertFalse(readOperationsFuture.isDone());

            rangeBeforeSetCompactionRevision.close();
            assertTrue(readOperationsFuture.isDone());

            rangeAfterSetCompactionRevision0.close();
            rangeAfterSetCompactionRevision1.close();
        } catch (Throwable t) {
            IgniteUtils.closeAll(
                    rangeBeforeSetCompactionRevision,
                    rangeAfterSetCompactionRevision0,
                    rangeAfterSetCompactionRevision1
            );
        }
    }

    /** Tests {@link KeyValueStorage#updateCompactionRevision} case from method description when storage is in recovery state. */
    @Test
    void testUpdateCompactionRevisionAndStorageInRecoveryState() {
        storage.setRecoveryRevisionsListener(currentRevisions -> {});
        storage.updateCompactionRevision(1, kvContext(clock.now()));
        assertEquals(1, storage.getCompactionRevision());

        storage.setRecoveryRevisionsListener(null);
        storage.updateCompactionRevision(2, kvContext(clock.now()));
        assertEquals(1, storage.getCompactionRevision());

        assertThat(
                allOf(
                        updateCompactionRevisionInWatchEvenQueue.waitFor(1L),
                        updateCompactionRevisionInWatchEvenQueue.waitFor(2L)
                ),
                willTimeoutFast()
        );
    }

    /** Tests {@link KeyValueStorage#updateCompactionRevision} case from method description when storage is in recovery state. */
    @Test
    void testUpdateCompactionRevisionWithoutStartWatches() {
        storage.updateCompactionRevision(1, kvContext(clock.now()));
        assertEquals(-1, storage.getCompactionRevision());

        assertThat(updateCompactionRevisionInWatchEvenQueue.waitFor(1L), willTimeoutFast());
    }

    /** Tests {@link KeyValueStorage#updateCompactionRevision} case from method description when storage is <b>not</b> in recovery state. */
    @Test
    void testUpdateCompactionRevision() {
        startWatches();

        storage.updateCompactionRevision(1, kvContext(clock.now()));

        assertThat(updateCompactionRevisionInWatchEvenQueue.waitFor(1L), willCompleteSuccessfully());
        assertEquals(1, storage.getCompactionRevision());
    }

    @Test
    void testConcurrentReadAndCompaction() {
        KeyValueUpdateContext context = kvContext(hybridTimestamp(10L));

        for (int i = 0; i < 500; i++) {
            byte[] key = key(i);
            byte[] value = keyValue(i, i);

            storage.put(key, value, context);
            long revision = storage.revision();
            storage.remove(key, context);

            runRace(
                    () -> {
                        storage.setCompactionRevision(revision);
                        storage.compact(revision);
                    },
                    () -> {
                        for (int j = 0; j < 1000; j++) {
                            try {
                                Entry entry = storage.get(key, revision);

                                assertEquals(revision, entry.revision());
                                assertArrayEquals(value, entry.value());
                            } catch (CompactedException ignore) {
                                // Expected outcome. Loop can be stopped.
                                return;
                            }
                        }
                    }
            );
        }
    }

    @Test
    void testConcurrentUpdateAndCompaction() {
        KeyValueUpdateContext context = kvContext(hybridTimestamp(10L));

        for (int i = 0; i < 1000; i++) {
            byte[] key = key(i);
            byte[] value = keyValue(i, i);

            storage.put(key, value, context);
            long revision = storage.revision();
            storage.remove(key, context);

            runRace(
                    () -> {
                        storage.setCompactionRevision(revision);
                        storage.compact(revision);
                    },
                    () -> {
                        // We update the same value in order to cause a race in "writeBatch" method, that would leave already compacted
                        // revisions in a list of revisions associated with this key.
                        storage.put(key, value, context);
                    }
            );

            assertThrows(
                    CompactedException.class,
                    () -> storage.get(key, revision)
            );

            storage.remove(key, context);
        }
    }

    @Test
    void testConcurrentReadAllAndCompaction() {
        KeyValueUpdateContext context = kvContext(hybridTimestamp(10L));

        int numberOfKeys = 15;
        for (int i = 0; i < 800; i++) {
            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();
            for (int j = 0; j < numberOfKeys; j++) {
                int k = i * numberOfKeys + j;

                keys.add(key(k));
                values.add(keyValue(k, k));
            }

            storage.putAll(keys, values, context);
            long revision = storage.revision();
            storage.removeAll(keys, context);

            runRace(
                    () -> {
                        storage.setCompactionRevision(revision);
                        storage.compact(revision);
                    },
                    () -> {
                        for (int j = 0; j < 1000; j++) {
                            try {
                                List<Entry> entries = storage.getAll(keys, revision);

                                assertEquals(numberOfKeys, entries.size());
                                for (int k = 0; k < numberOfKeys; k++) {
                                    Entry entry = entries.get(k);

                                    assertEquals(revision, entry.revision());
                                    assertArrayEquals(values.get(k), entry.value());
                                }
                            } catch (CompactedException ignore) {
                                // Expected outcome. Loop can be stopped.
                                return;
                            }
                        }
                    }
            );
        }
    }

    @Test
    void testConcurrentRangeAndCompaction() {
        KeyValueUpdateContext context = kvContext(hybridTimestamp(10L));

        int numberOfKeys = 3;
        for (int i = 0; i < 100; i++) {
            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();
            Map<ByteArray, byte[]> keyValueMap = new HashMap<>();

            String prefix = "key" + i + "-";
            for (int j = 0; j < numberOfKeys; j++) {
                int k = i * numberOfKeys + j;

                ByteArray key = ByteArray.fromString(prefix + j);
                byte[] value = keyValue(k, k);

                keys.add(key.bytes());
                values.add(value);

                keyValueMap.put(key, value);
            }

            storage.putAll(keys, values, context);
            long revision = storage.revision();
            storage.removeAll(keys, context);

            runRace(
                    () -> {
                        storage.setCompactionRevision(revision);

                        AbstractKeyValueStorage abstractStorage = (AbstractKeyValueStorage) storage;

                        CompletableFuture<Void> fut = abstractStorage.readOperationForCompactionTracker.collect(revision);
                        fut.get(1, TimeUnit.SECONDS);

                        storage.compact(revision);
                    },
                    () -> {
                        for (int j = 0; j < 1000; j++) {
                            byte[] from = ByteArray.fromString(prefix).bytes();
                            byte[] to = RocksUtils.incrementPrefix(from);

                            Cursor<Entry> cursor;
                            try {
                                cursor = storage.range(from, to, revision);
                            } catch (CompactedException ignore) {
                                // Expected outcome. Loop can be stopped.
                                return;
                            }

                            try (cursor) {
                                for (int k = 0; k < numberOfKeys; k++) {
                                    assertTrue(cursor.hasNext());

                                    Entry entry = cursor.next();

                                    assertEquals(revision, entry.revision());

                                    ByteArray key = new ByteArray(entry.key());
                                    byte[] expectedValue = keyValueMap.get(key);
                                    assertNotNull(expectedValue, key.toString());

                                    assertArrayEquals(expectedValue, entry.value());
                                }

                                assertFalse(cursor.hasNext());
                            }
                        }
                    }
            );
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

    private static List<Long> collectRevisions(Cursor<Entry> cursor) {
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

    private void startWatches() {
        storage.startWatches(storage.revision() + 1, new WatchEventHandlingCallback() {
            @Override
            public void onSafeTimeAdvanced(HybridTimestamp newSafeTime) {
                clusterTime.updateSafeTime(newSafeTime);
            }
        });
    }

    private static String toUtf8String(byte[]... keys) {
        return Arrays.stream(keys)
                .map(KeyValueStorageUtils::toUtf8String)
                .collect(joining(", ", "[", "]"));
    }
}
