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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.CommandIdGenerator;
import org.apache.ignite.internal.metastorage.server.ExistenceCondition.Type;
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
    private static final byte[] FOO_KEY = fromString("foo");

    private static final byte[] BAR_KEY = fromString("bar");

    private static final byte[] SOME_KEY = fromString("someKey");

    private static final byte[] SOME_VALUE = fromString("someValue");

    @WorkDirectory
    Path workDir;

    private final HybridClock clock = new HybridClockImpl();

    @Override
    @BeforeEach
    void setUp() {
        super.setUp();

        storage.putAll(List.of(FOO_KEY, BAR_KEY), List.of(SOME_VALUE, SOME_VALUE), clock.now());
        storage.put(BAR_KEY, SOME_VALUE, clock.now());
        storage.put(FOO_KEY, SOME_VALUE, clock.now());
        storage.put(SOME_KEY, SOME_VALUE, clock.now());

        var fooKey = new ByteArray(FOO_KEY);
        var barKey = new ByteArray(BAR_KEY);

        var iif = new If(
                new AndCondition(new ExistenceCondition(Type.EXISTS, FOO_KEY), new ExistenceCondition(Type.EXISTS, BAR_KEY)),
                new Statement(ops(put(fooKey, SOME_VALUE), remove(barKey)).yield()),
                new Statement(ops(noop()).yield())
        );

        storage.invoke(iif, clock.now(), new CommandIdGenerator(UUID::randomUUID).newId());

        storage.remove(SOME_KEY, clock.now());

        // Special revision update to prevent tests from failing.
        storage.put(fromString("fake"), SOME_VALUE, clock.now());

        assertEquals(List.of(1, 3, 5), collectRevisions(FOO_KEY));
        assertEquals(List.of(1, 2, 5), collectRevisions(BAR_KEY));
        assertEquals(List.of(4, 6), collectRevisions(SOME_KEY));
    }

    abstract boolean isPersistent();

    abstract void restartStorage(boolean clear) throws Exception;

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
    void testSaveCompactionRevisionDoesNotChangeRevisionInMemory() {
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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testCompactDontSetAndSaveCompactionRevision(boolean clearStorage) throws Exception {
        storage.compact(1);
        assertEquals(-1, storage.getCompactionRevision());

        restartStorage(clearStorage);
        assertEquals(-1, storage.getCompactionRevision());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testRestoreFromSnapshotWithoutSaveCompactionRevision(boolean clearStorage) throws Exception {
        assumeTrue(isPersistent());

        Path snapshotDir = workDir.resolve("snapshot");
        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());

        restartStorage(clearStorage);

        storage.restoreSnapshot(snapshotDir);
        assertEquals(-1, storage.getCompactionRevision());
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
}
