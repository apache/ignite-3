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

package org.apache.ignite.internal.cluster.management.raft;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.rocksdb.RocksUtils;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Base class for testing {@link ClusterStateStorage} implementations.
 */
public abstract class AbstractClusterStateStorageTest extends IgniteAbstractTest {
    private ClusterStateStorage storage;

    abstract ClusterStateStorage createStorage(String nodeName);

    @BeforeEach
    void setUp(TestInfo testInfo) {
        storage = createStorage(testNodeName(testInfo, 0));

        assertThat(storage.startAsync(), willCompleteSuccessfully());
    }

    @AfterEach
    void tearDown() {
        assertThat(storage.stopAsync(), willCompleteSuccessfully());
    }

    /**
     * Tests the {@link ClusterStateStorage#get} and {@link ClusterStateStorage#put} methods.
     */
    @Test
    void testGetAndPut() {
        byte[] key1 = "key1".getBytes(UTF_8);
        byte[] key2 = "key2".getBytes(UTF_8);

        assertThat(storage.get(key1), is(nullValue()));
        assertThat(storage.get(key2), is(nullValue()));

        byte[] value1 = "value1".getBytes(UTF_8);
        byte[] value2 = "value2".getBytes(UTF_8);

        storage.put(key1, value1);
        storage.put(key2, value2);

        assertThat(storage.get(key1), is(equalTo(value1)));
        assertThat(storage.get(key2), is(equalTo(value2)));
    }

    /**
     * Tests that {@link ClusterStateStorage#put} replaces previous values.
     */
    @Test
    void testPutReplace() {
        byte[] key = "key".getBytes(UTF_8);

        byte[] value1 = "value1".getBytes(UTF_8);

        storage.put(key, value1);

        assertThat(storage.get(key), is(value1));

        byte[] value2 = "value2".getBytes(UTF_8);

        storage.put(key, value2);

        assertThat(storage.get(key), is(equalTo(value2)));
    }

    @Test
    void testReplaceAll() {
        byte[] key1 = "key1".getBytes(UTF_8);
        byte[] key2 = "key2".getBytes(UTF_8);
        byte[] key3 = "keg".getBytes(UTF_8);

        byte[] value1 = "value1".getBytes(UTF_8);
        byte[] value2 = "value2".getBytes(UTF_8);
        byte[] value3 = "value3".getBytes(UTF_8);

        storage.put(key1, value1);
        storage.put(key2, value2);
        storage.put(key3, value3);

        // Replace by prefix common for all keys
        storage.replaceAll("ke".getBytes(UTF_8), key1, value2);

        assertThat(storage.get(key1), is(equalTo(value2)));
        assertThat(storage.get(key2), is(nullValue()));
        assertThat(storage.get(key3), is(nullValue()));
    }

    @Test
    void testReplaceAllNonExistentPrefix() {
        byte[] key1 = "key1".getBytes(UTF_8);
        byte[] key2 = "key2".getBytes(UTF_8);
        byte[] key3 = "keg".getBytes(UTF_8);

        byte[] value1 = "value1".getBytes(UTF_8);
        byte[] value2 = "value2".getBytes(UTF_8);
        byte[] value3 = "value3".getBytes(UTF_8);

        storage.put(key1, value1);
        storage.put(key2, value2);
        storage.put(key3, value3);

        // Replace by nonexistent prefix
        storage.replaceAll("bar".getBytes(UTF_8), key1, value3);

        assertThat(storage.get(key1), is(equalTo(value3)));
        assertThat(storage.get(key2), is(equalTo(value2)));
        assertThat(storage.get(key3), is(equalTo(value3)));
    }

    @Test
    void testReplaceAllTwoKeys() {
        byte[] key1 = "key1".getBytes(UTF_8);
        byte[] key2 = "key2".getBytes(UTF_8);
        byte[] key3 = "keg".getBytes(UTF_8);

        byte[] value1 = "value1".getBytes(UTF_8);
        byte[] value2 = "value2".getBytes(UTF_8);
        byte[] value3 = "value3".getBytes(UTF_8);

        storage.put(key1, value1);
        storage.put(key2, value2);
        storage.put(key3, value3);

        // Replace by prefix common for two keys
        storage.replaceAll("key".getBytes(UTF_8), key1, value3);

        assertThat(storage.get(key1), is(equalTo(value3)));
        assertThat(storage.get(key2), is(nullValue()));
        assertThat(storage.get(key3), is(equalTo(value3)));
    }

    /**
     * Tests the {@link ClusterStateStorage#remove} method.
     */
    @Test
    void testRemove() {
        byte[] key1 = "key1".getBytes(UTF_8);
        byte[] key2 = "key2".getBytes(UTF_8);

        byte[] value1 = "value1".getBytes(UTF_8);
        byte[] value2 = "value2".getBytes(UTF_8);

        storage.put(key1, value1);
        storage.put(key2, value2);

        storage.remove(key1);

        assertThat(storage.get(key1), is(nullValue()));
        assertThat(storage.get(key2), is(equalTo(value2)));
    }

    /**
     * Tests that {@link ClusterStateStorage#remove} works correctly if a value is missing.
     */
    @Test
    void testRemoveNonExistent() {
        byte[] key = "key".getBytes(UTF_8);

        storage.remove(key);

        assertThat(storage.get(key), is(nullValue()));
    }

    /**
     * Tests the {@link ClusterStateStorage#removeAll} method.
     */
    @Test
    void testRemoveAll() {
        storage.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        storage.put("key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
        storage.put("key3".getBytes(UTF_8), "value3".getBytes(UTF_8));

        storage.removeAll(List.of(
                "key1".getBytes(UTF_8),
                "key2".getBytes(UTF_8),
                "key4".getBytes(UTF_8) // does not exist in storage
        ));

        assertThat(storage.getWithPrefix("key".getBytes(UTF_8), (k, v) -> new String(v, UTF_8)), contains("value3"));
    }

    /**
     * Tests the {@link ClusterStateStorage#getWithPrefix} method.
     */
    @Test
    void testGetWithPrefix() {
        storage.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        storage.put("key2".getBytes(UTF_8), "value2".getBytes(UTF_8));
        storage.put("foo".getBytes(UTF_8), "value3".getBytes(UTF_8));

        assertThat(storage.getWithPrefix("ke".getBytes(UTF_8), (k, v) -> new String(v, UTF_8)), containsInAnyOrder("value1", "value2"));
    }

    /**
     * Tests the {@link ClusterStateStorage#getWithPrefix} method (corner case, when keys are close together lexicographically).
     */
    @Test
    void testGetWithPrefixBorder() {
        byte[] key1 = "key1".getBytes(UTF_8);
        byte[] key2 = RocksUtils.incrementPrefix(key1);

        storage.put(key1, "value1".getBytes(UTF_8));
        storage.put(key2, "value2".getBytes(UTF_8));

        assertThat(storage.getWithPrefix(key1, (k, v) -> new String(v, UTF_8)), containsInAnyOrder("value1"));
    }

    /**
     * Tests that {@link ClusterStateStorage#getWithPrefix} method works correctly over empty ranges.
     */
    @Test
    void testGetWithPrefixEmpty() {
        storage.put("key1".getBytes(UTF_8), "value1".getBytes(UTF_8));
        storage.put("key2".getBytes(UTF_8), "value2".getBytes(UTF_8));

        assertThat(storage.getWithPrefix("foo".getBytes(UTF_8), (k, v) -> new String(v, UTF_8)), is(empty()));
    }

    /**
     * Tests creating and restoring snapshots.
     */
    @Test
    void testSnapshot(TestInfo testInfo) throws Exception {
        Path snapshotDir = workDir.resolve("snapshot");

        Files.createDirectory(snapshotDir);

        byte[] key1 = "key1".getBytes(UTF_8);
        byte[] key2 = "key2".getBytes(UTF_8);

        byte[] value1 = "value1".getBytes(UTF_8);
        byte[] value2 = "value2".getBytes(UTF_8);

        storage.put(key1, value1);
        storage.put(key2, value2);

        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());

        assertThat(storage.stopAsync(), willCompleteSuccessfully());

        storage = createStorage(testNodeName(testInfo, 1));

        assertThat(storage.startAsync(), willCompleteSuccessfully());

        assertThat(storage.get(key1), is(nullValue()));
        assertThat(storage.get(key2), is(nullValue()));

        storage.restoreSnapshot(snapshotDir);

        assertThat(storage.get(key1), is(value1));
        assertThat(storage.get(key2), is(value2));

        // Try restoring a snapshot a second time.
        assertThat(storage.snapshot(snapshotDir), willCompleteSuccessfully());

        storage.restoreSnapshot(snapshotDir);

        assertThat(storage.get(key1), is(value1));
        assertThat(storage.get(key2), is(value2));
    }

    /**
     * Tests that writes coming after a snapshot is started do not get reflected in the snapshot.
     *
     * @throws Exception If something goes wrong.
     */
    @Test
    void snapshotShouldNotContainWritesAddedAfterItsStart() throws Exception {
        final int entriesInSnapshot = 100_000;

        for (int i = 0; i < entriesInSnapshot; i++) {
            putKeyValue(i);
        }

        Path snapshotDirPath = workDir.resolve("snapshot");
        Files.createDirectories(snapshotDirPath);

        CompletableFuture<Void> snapshotFuture = storage.snapshot(snapshotDirPath);

        for (int i = entriesInSnapshot; i < entriesInSnapshot + 1000; i++) {
            putKeyValue(i);
        }

        snapshotFuture.join();

        storage.restoreSnapshot(snapshotDirPath);

        byte[] keyAddedAfterSnapshotStart = key(entriesInSnapshot);
        assertThat(storage.get(keyAddedAfterSnapshotStart), is(nullValue()));
    }

    @Test
    void throwsNodeStoppingException() {
        assertThat(storage.stopAsync(), willCompleteSuccessfully());

        assertThrowsWithCause(() -> storage.get(BYTE_EMPTY_ARRAY), NodeStoppingException.class);
        assertThrowsWithCause(() -> storage.put(BYTE_EMPTY_ARRAY, BYTE_EMPTY_ARRAY), NodeStoppingException.class);
        assertThrowsWithCause(() -> storage.remove(BYTE_EMPTY_ARRAY), NodeStoppingException.class);
        assertThrowsWithCause(() -> storage.removeAll(List.of(BYTE_EMPTY_ARRAY)), NodeStoppingException.class);
        assertThrowsWithCause(() -> storage.replaceAll(BYTE_EMPTY_ARRAY, BYTE_EMPTY_ARRAY, BYTE_EMPTY_ARRAY), NodeStoppingException.class);
        assertThrowsWithCause(() -> storage.getWithPrefix(BYTE_EMPTY_ARRAY, (k, v) -> null), NodeStoppingException.class);
        assertThrowsWithCause(() -> storage.restoreSnapshot(workDir), NodeStoppingException.class);

        assertThat(storage.snapshot(workDir), willThrow(NodeStoppingException.class));
    }

    private void putKeyValue(int n) {
        storage.put(key(n), ("value" + n).getBytes(UTF_8));
    }

    private static byte[] key(int n) {
        return ("key" + n).getBytes(UTF_8);
    }
}
