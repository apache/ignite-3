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
import static java.util.function.Function.identity;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MAX_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.MIN_VALUE;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.impl.CommandIdGenerator;
import org.apache.ignite.internal.metastorage.server.ValueCondition.Type;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests for key-value storage implementations.
 */
public abstract class BasicOperationsKeyValueStorageTest extends AbstractKeyValueStorageTest {
    @Test
    public void testPut() {
        byte[] key = key(1);
        byte[] val = keyValue(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        putToMs(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        Entry e = storage.get(key);

        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(1, e.revision());
        assertEquals(1, e.updateCounter());

        putToMs(key, val);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertFalse(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
    }

    @Test
    void getWithRevisionBound() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);

        byte[] key2 = key(2);
        byte[] val21 = keyValue(2, 21);
        byte[] val22 = keyValue(2, 22);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        byte[] key4 = key(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Regular put.
        putToMs(key1, val1);

        // Rewrite.
        putToMs(key2, val21);
        putToMs(key2, val22);

        // Remove.
        putToMs(key3, val3);
        removeFromMs(key3);

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());

        // Bounded by revision 2.
        Entry key1EntryBounded2 = storage.get(key1, 2);

        assertNotNull(key1EntryBounded2);
        assertEquals(1, key1EntryBounded2.revision());
        assertEquals(1, key1EntryBounded2.updateCounter());
        assertArrayEquals(val1, key1EntryBounded2.value());
        assertFalse(key1EntryBounded2.tombstone());
        assertFalse(key1EntryBounded2.empty());

        Entry key2EntryBounded2 = storage.get(key2, 2);

        assertNotNull(key2EntryBounded2);
        assertEquals(2, key2EntryBounded2.revision());
        assertEquals(2, key2EntryBounded2.updateCounter());
        assertArrayEquals(val21, key2EntryBounded2.value());
        assertFalse(key2EntryBounded2.tombstone());
        assertFalse(key2EntryBounded2.empty());

        Entry key3EntryBounded2 = storage.get(key3, 2);

        assertNotNull(key3EntryBounded2);
        assertEquals(0, key3EntryBounded2.revision());
        assertEquals(0, key3EntryBounded2.updateCounter());
        assertNull(key3EntryBounded2.value());
        assertFalse(key3EntryBounded2.tombstone());
        assertTrue(key3EntryBounded2.empty());

        // Bounded by revision 5.
        Entry key1EntryBounded5 = storage.get(key1, 5);

        assertNotNull(key1EntryBounded5);
        assertEquals(1, key1EntryBounded5.revision());
        assertEquals(1, key1EntryBounded5.updateCounter());
        assertArrayEquals(val1, key1EntryBounded5.value());
        assertFalse(key1EntryBounded5.tombstone());
        assertFalse(key1EntryBounded5.empty());

        Entry key2EntryBounded5 = storage.get(key2, 5);

        assertNotNull(key2EntryBounded5);
        assertEquals(3, key2EntryBounded5.revision());
        assertEquals(3, key2EntryBounded5.updateCounter());
        assertArrayEquals(val22, key2EntryBounded5.value());
        assertFalse(key2EntryBounded5.tombstone());
        assertFalse(key2EntryBounded5.empty());

        Entry key3EntryBounded5 = storage.get(key3, 5);

        assertNotNull(key3EntryBounded5);
        assertEquals(5, key3EntryBounded5.revision());
        assertEquals(5, key3EntryBounded5.updateCounter());
        assertTrue(key3EntryBounded5.tombstone());
        assertNull(key3EntryBounded5.value());
        assertFalse(key3EntryBounded5.empty());
    }

    @Test
    void getWithRevisionLowerUpperBound() {
        byte[] key1 = key(1);
        byte[] key2 = key(2);

        byte[] val1 = keyValue(1, 1);
        byte[] val2 = keyValue(1, 2);
        byte[] val3 = keyValue(2, 3);
        byte[] val4 = keyValue(1, 4);
        byte[] val5 = keyValue(1, 5);
        byte[] val6 = keyValue(2, 6);
        byte[] val7 = keyValue(2, 7);
        byte[] val8 = keyValue(1, 8);
        byte[] val9 = keyValue(1, 9);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val1);
        putToMs(key1, val2);
        putToMs(key2, val3);
        putToMs(key1, val4);
        putToMs(key1, val5);
        putToMs(key2, val6);
        putToMs(key2, val7);
        putToMs(key1, val8);
        putToMs(key1, val9);

        removeFromMs(key1);

        assertEquals(10, storage.revision());
        assertEquals(10, storage.updateCounter());

        // Check that a lower revision and an upper revision are inclusive.
        // Check that entry with another key is not included in a result list.
        List<Entry> entries1 = storage.get(key1, 2, 5);
        List<byte[]> values1 = entries1.stream().map(entry -> entry.value()).collect(Collectors.toList());

        assertEquals(3, entries1.size());
        assertArrayEquals(val2, values1.get(0));
        assertArrayEquals(val4, values1.get(1));
        assertArrayEquals(val5, values1.get(2));

        // Check that entries with another key and revision equals to lower revision and to the upper revision are not inclusive.
        List<Entry> entries2 = storage.get(key1, 3, 6);
        List<byte[]> values2 = entries2.stream().map(entry -> entry.value()).collect(Collectors.toList());

        assertEquals(2, entries2.size());
        assertArrayEquals(val4, values2.get(0));
        assertArrayEquals(val5, values2.get(1));

        // Get one entry. The lower and the upper revision are equal.
        List<Entry> entries3 = storage.get(key1, 8, 8);
        List<byte[]> values3 = entries3.stream().map(entry -> entry.value()).collect(Collectors.toList());

        assertEquals(1, entries3.size());
        assertArrayEquals(val8, values3.get(0));

        // Try to get entries when the revision range doesn't contain entries with the key.
        List<Entry> entries4 = storage.get(key1, 6, 7);

        assertTrue(entries4.isEmpty());

        // Try to get entries when the storage doesn't contain entries with specified revisions.
        List<Entry> entries5 = storage.get(key1, 20, 30);

        assertTrue(entries5.isEmpty());

        // Get a tombstone.
        List<Entry> entries6 = storage.get(key1, 10, 10);

        assertEquals(1, entries6.size());
        assertTrue(entries6.get(0).tombstone());
        assertNull(entries6.get(0).value());

        // Check validation asserts.
        assertThrows(AssertionError.class, () -> storage.get(key1, -1, 1));
        assertThrows(AssertionError.class, () -> storage.get(key1, 1, -1));
        assertThrows(AssertionError.class, () -> storage.get(key1, 2, 1));
    }

    @Test
    void getAll() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);

        byte[] key2 = key(2);
        byte[] val21 = keyValue(2, 21);
        byte[] val22 = keyValue(2, 22);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        byte[] key4 = key(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Regular put.
        putToMs(key1, val1);

        // Rewrite.
        putToMs(key2, val21);
        putToMs(key2, val22);

        // Remove.
        putToMs(key3, val3);
        removeFromMs(key3);

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());

        Collection<Entry> entries = storage.getAll(List.of(key1, key2, key3, key4));

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val22, e2.value());

        // Test removed value.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(5, e3.revision());
        assertEquals(5, e3.updateCounter());
        assertTrue(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());
    }

    @Test
    void getAllWithRevisionBound() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);

        byte[] key2 = key(2);
        byte[] val21 = keyValue(2, 21);
        byte[] val22 = keyValue(2, 22);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        byte[] key4 = key(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Regular put.
        putToMs(key1, val1);

        // Rewrite.
        putToMs(key2, val21);
        putToMs(key2, val22);

        // Remove.
        putToMs(key3, val3);
        removeFromMs(key3);

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());

        // Bounded by revision 2.
        Collection<Entry> entries = storage.getAll(List.of(key1, key2, key3, key4), 2);

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test while not rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(2, e2.revision());
        assertEquals(2, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val21, e2.value());

        // Values with larger revision don't exist yet.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertTrue(e3.empty());

        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertTrue(e4.empty());

        // Bounded by revision 4.
        entries = storage.getAll(List.of(key1, key2, key3, key4), 4);

        assertEquals(4, entries.size());

        map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test rewritten value.
        e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val22, e2.value());

        // Test not removed value.
        e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(4, e3.revision());
        assertEquals(4, e3.updateCounter());
        assertFalse(e3.tombstone());
        assertFalse(e3.empty());
        assertArrayEquals(val3, e3.value());

        // Value with larger revision doesn't exist yet.
        e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertTrue(e4.empty());
    }

    @Test
    public void putAll() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);

        byte[] key2 = key(2);
        byte[] val21 = keyValue(2, 21);
        byte[] val22 = keyValue(2, 22);

        byte[] key3 = key(3);
        byte[] val31 = keyValue(3, 31);
        byte[] val32 = keyValue(3, 32);

        byte[] key4 = key(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Must be rewritten.
        putToMs(key2, val21);

        // Remove. Tombstone must be replaced by new value.
        putToMs(key3, val31);
        removeFromMs(key3);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        putAllToMs(List.of(key1, key2, key3), List.of(val1, val22, val32));

        assertEquals(4, storage.revision());
        assertEquals(6, storage.updateCounter());

        Collection<Entry> entries = storage.getAll(List.of(key1, key2, key3, key4));

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(4, e1.revision());
        assertEquals(4, e1.updateCounter());
        assertFalse(e1.tombstone());
        assertFalse(e1.empty());
        assertArrayEquals(val1, e1.value());

        // Test rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(4, e2.revision());
        assertEquals(5, e2.updateCounter());
        assertFalse(e2.tombstone());
        assertFalse(e2.empty());
        assertArrayEquals(val22, e2.value());

        // Test removed value.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(4, e3.revision());
        assertEquals(6, e3.updateCounter());
        assertFalse(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());
    }

    @Test
    public void testRemove() {
        byte[] key = key(1);
        byte[] val = keyValue(1, 1);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        // Remove non-existent entry.
        removeFromMs(key);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());
        assertTrue(storage.get(key).empty());

        putToMs(key, val);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // Remove existent entry.
        removeFromMs(key);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        Entry e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());

        // Remove already removed entry (tombstone can't be removed).
        removeFromMs(key);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        e = storage.get(key);

        assertFalse(e.empty());
        assertTrue(e.tombstone());
        assertEquals(2, e.revision());
        assertEquals(2, e.updateCounter());
    }

    @Test
    public void removeAll() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);

        byte[] key2 = key(2);
        byte[] val21 = keyValue(2, 21);
        byte[] val22 = keyValue(2, 22);

        byte[] key3 = key(3);
        byte[] val31 = keyValue(3, 31);

        byte[] key4 = key(4);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Regular put.
        putToMs(key1, val1);

        // Rewrite.
        putToMs(key2, val21);
        putToMs(key2, val22);

        // Remove. Tombstone must not be removed again.
        putToMs(key3, val31);
        removeFromMs(key3);

        assertEquals(5, storage.revision());
        assertEquals(5, storage.updateCounter());

        removeAllFromMs(List.of(key1, key2, key3, key4));

        assertEquals(6, storage.revision());
        assertEquals(7, storage.updateCounter()); // Only two keys are updated.

        Collection<Entry> entries = storage.getAll(List.of(key1, key2, key3, key4));

        assertEquals(4, entries.size());

        Map<ByteArray, Entry> map = entries.stream().collect(Collectors.toMap(e -> new ByteArray(e.key()), identity()));

        // Test regular put value.
        Entry e1 = map.get(new ByteArray(key1));

        assertNotNull(e1);
        assertEquals(6, e1.revision());
        assertEquals(6, e1.updateCounter());
        assertTrue(e1.tombstone());
        assertFalse(e1.empty());

        // Test rewritten value.
        Entry e2 = map.get(new ByteArray(key2));

        assertNotNull(e2);
        assertEquals(6, e2.revision());
        assertEquals(7, e2.updateCounter());
        assertTrue(e2.tombstone());
        assertFalse(e2.empty());

        // Test removed value.
        Entry e3 = map.get(new ByteArray(key3));

        assertNotNull(e3);
        assertEquals(5, e3.revision());
        assertEquals(5, e3.updateCounter());
        assertTrue(e3.tombstone());
        assertFalse(e3.empty());

        // Test empty value.
        Entry e4 = map.get(new ByteArray(key4));

        assertNotNull(e4);
        assertFalse(e4.tombstone());
        assertTrue(e4.empty());
    }

    @Test
    public void invokeWithRevisionCondition_successBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new RevisionCondition(RevisionCondition.Type.EQUAL, key1, 1),
                List.of(put(new ByteArray(key1), val12), put(new ByteArray(key2), val2)),
                List.of(put(new ByteArray(key3), val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val12, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithRevisionCondition_failureBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new RevisionCondition(RevisionCondition.Type.EQUAL, key1, 2),
                List.of(put(new ByteArray(key3), val3)),
                List.of(put(new ByteArray(key1), val12), put(new ByteArray(key2), val2))
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val12, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Success" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithExistsCondition_successBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new ExistenceCondition(ExistenceCondition.Type.EXISTS, key1),
                List.of(put(new ByteArray(key1), val12), put(new ByteArray(key2), val2)),
                List.of(put(new ByteArray(key3), val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val12, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithExistsCondition_failureBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new ExistenceCondition(ExistenceCondition.Type.EXISTS, key3),
                List.of(put(new ByteArray(key3), val3)),
                List.of(put(new ByteArray(key1), val12), put(new ByteArray(key2), val2))
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val12, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Success" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithNotExistsCondition_successBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, key2),
                List.of(put(new ByteArray(key1), val12), put(new ByteArray(key2), val2)),
                List.of(put(new ByteArray(key3), val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val12, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithNotExistsCondition_failureBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new ExistenceCondition(ExistenceCondition.Type.NOT_EXISTS, key1),
                List.of(put(new ByteArray(key3), val3)),
                List.of(
                        put(new ByteArray(key1), val12),
                        put(new ByteArray(key2), val2)
                )
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val12, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Success" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithTombstoneCondition_successBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);
        removeFromMs(key1); // Should be tombstone after remove.

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        boolean branch = invokeOnMs(
                new TombstoneCondition(TombstoneCondition.Type.TOMBSTONE, key1),
                List.of(put(new ByteArray(key2), val2)),
                List.of(put(new ByteArray(key3), val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(3, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertTrue(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertNull(e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithTombstoneCondition_failureBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new TombstoneCondition(TombstoneCondition.Type.TOMBSTONE, key1),
                List.of(put(new ByteArray(key2), val2)),
                List.of(put(new ByteArray(key3), val3))
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertArrayEquals(val11, e1.value());

        Entry e3 = storage.get(key3);

        assertFalse(e3.empty());
        assertFalse(e3.tombstone());
        assertEquals(2, e3.revision());
        assertEquals(2, e3.updateCounter());
        assertArrayEquals(val3, e3.value());

        // "Success" branch isn't applied.
        Entry e2 = storage.get(key2);

        assertTrue(e2.empty());
    }

    @Test
    public void invokeWithNotTombstoneCondition_successBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new TombstoneCondition(TombstoneCondition.Type.NOT_TOMBSTONE, key1),
                List.of(put(new ByteArray(key2), val2)),
                List.of(put(new ByteArray(key3), val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(3, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(1, e1.revision());
        assertEquals(1, e1.updateCounter());
        assertArrayEquals(val11, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(2, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithNotTombstoneCondition_failureBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);
        removeFromMs(key1); // Should be tombstone after remove.

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        boolean branch = invokeOnMs(
                new TombstoneCondition(TombstoneCondition.Type.NOT_TOMBSTONE, key1),
                List.of(put(new ByteArray(key2), val2)),
                List.of(put(new ByteArray(key3), val3))
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(3, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertTrue(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertNull(e1.value());

        Entry e3 = storage.get(key3);

        assertFalse(e3.empty());
        assertFalse(e3.tombstone());
        assertEquals(3, e3.revision());
        assertEquals(3, e3.updateCounter());
        assertArrayEquals(val3, e3.value());

        // "Success" branch isn't applied.
        Entry e2 = storage.get(key2);

        assertTrue(e2.empty());
    }

    @Test
    public void invokeWithValueCondition_successBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new ValueCondition(ValueCondition.Type.EQUAL, key1, val11),
                List.of(
                        put(new ByteArray(key1), val12),
                        put(new ByteArray(key2), val2)
                ),
                List.of(put(new ByteArray(key3), val3))
        );

        // "Success" branch is applied.
        assertTrue(branch);
        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val12, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Failure" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeWithValueCondition_failureBranch() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val11);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        boolean branch = invokeOnMs(
                new ValueCondition(ValueCondition.Type.EQUAL, key1, val12),
                List.of(put(new ByteArray(key3), val3)),
                List.of(
                        put(new ByteArray(key1), val12),
                        put(new ByteArray(key2), val2)
                )
        );

        // "Failure" branch is applied.
        assertFalse(branch);
        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);

        assertFalse(e1.empty());
        assertFalse(e1.tombstone());
        assertEquals(2, e1.revision());
        assertEquals(2, e1.updateCounter());
        assertArrayEquals(val12, e1.value());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(2, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(val2, e2.value());

        // "Success" branch isn't applied.
        Entry e3 = storage.get(key3);

        assertTrue(e3.empty());
    }

    @Test
    public void invokeOperations() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        // No-op.
        boolean branch = invokeOnMs(
                new ValueCondition(ValueCondition.Type.EQUAL, key1, val1),
                List.of(Operations.noop()),
                List.of(Operations.noop())
        );

        assertTrue(branch);

        // No updates.
        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        // Put.
        branch = invokeOnMs(
                new ValueCondition(ValueCondition.Type.EQUAL, key1, val1),
                List.of(
                        put(new ByteArray(key2), val2),
                        put(new ByteArray(key3), val3)
                ),
                List.of(Operations.noop())
        );

        assertTrue(branch);

        assertEquals(3, storage.revision());
        assertEquals(5, storage.updateCounter());

        Entry e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertFalse(e2.tombstone());
        assertEquals(3, e2.revision());
        assertEquals(3, e2.updateCounter());
        assertArrayEquals(key2, e2.key());
        assertArrayEquals(val2, e2.value());

        Entry e3 = storage.get(key3);

        assertFalse(e3.empty());
        assertFalse(e3.tombstone());
        assertEquals(3, e3.revision());
        assertEquals(4, e3.updateCounter());
        assertArrayEquals(key3, e3.key());
        assertArrayEquals(val3, e3.value());

        // Remove.
        branch = invokeOnMs(
                new ValueCondition(ValueCondition.Type.EQUAL, key1, val1),
                List.of(
                        remove(new ByteArray(key2)),
                        remove(new ByteArray(key3))
                ),
                List.of(Operations.noop())
        );

        assertTrue(branch);

        assertEquals(4, storage.revision());
        assertEquals(8, storage.updateCounter());

        e2 = storage.get(key2);

        assertFalse(e2.empty());
        assertTrue(e2.tombstone());
        assertEquals(4, e2.revision());
        assertEquals(6, e2.updateCounter());
        assertArrayEquals(key2, e2.key());

        e3 = storage.get(key3);

        assertFalse(e3.empty());
        assertTrue(e3.tombstone());
        assertEquals(4, e3.revision());
        assertEquals(7, e3.updateCounter());
        assertArrayEquals(key3, e3.key());
    }

    /**
     * <pre>
     *   if (key1.value == val1 || exist(key2))
     *       if (key3.revision == 3):
     *           put(key1, rval1) <------ TEST FOR THIS BRANCH
     *           return 1
     *       else
     *           put(key1, rval1)
     *           remove(key2)
     *           return 2
     *   else
     *       put(key3, rval3)
     *       return 3
     * </pre>
     */
    @Test
    public void multiInvokeOperationsBranch1() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);
        byte[] rval1 = keyValue(1, 4);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);
        byte[] rval3 = keyValue(2, 6);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        putToMs(key2, val2);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        putToMs(key3, val3);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        If iif = new If(
                new OrCondition(new ValueCondition(Type.EQUAL, key1, val1), new ExistenceCondition(ExistenceCondition.Type.EXISTS, key2)),
                new Statement(new If(
                        new RevisionCondition(RevisionCondition.Type.EQUAL, key3, 3),
                        new Statement(ops(put(new ByteArray(key1), rval1)).yield(1)),
                        new Statement(ops(put(new ByteArray(key1), rval1), remove(new ByteArray(key2))).yield(2)))),
                new Statement(ops(put(new ByteArray(key3), rval3)).yield(3))
        );

        StatementResult branch = invokeOnMs(iif);

        assertEquals(1, branch.getAsInt());

        assertEquals(4, storage.revision());
        assertEquals(5, storage.updateCounter());

        Entry e1 = storage.get(key1);
        assertEquals(4, e1.revision());
        assertArrayEquals(rval1, e1.value());

        Entry e2 = storage.get(key2);
        assertEquals(2, e2.revision());

        Entry e3 = storage.get(key3);
        assertEquals(3, e3.revision());
        assertArrayEquals(val3, e3.value());
    }

    /**
     * <pre>
     *   if (key1.value == val1 || exist(key2))
     *       if (key3.revision == 3):
     *           put(key1, rval1)
     *           return 1
     *       else
     *           put(key1, rval1) <------ TEST FOR THIS BRANCH
     *           remove(key2)
     *           return 2
     *   else
     *       put(key3, rval3)
     *       return 3
     * </pre>
     */
    @Test
    public void multiInvokeOperationsBranch2() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);
        byte[] rval1 = keyValue(1, 4);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);
        byte[] rval3 = keyValue(2, 6);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val1);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        putToMs(key2, val2);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        putToMs(key3, val3);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        putToMs(key3, val3);

        assertEquals(4, storage.revision());
        assertEquals(4, storage.updateCounter());

        If iif = new If(
                new OrCondition(new ValueCondition(Type.EQUAL, key1, val1), new ExistenceCondition(ExistenceCondition.Type.EXISTS, key2)),
                new Statement(new If(
                        new RevisionCondition(RevisionCondition.Type.EQUAL, key3, 3),
                        new Statement(ops(put(new ByteArray(key1), rval1)).yield()),
                        new Statement(ops(put(new ByteArray(key1), rval1), remove(new ByteArray(key2))).yield(2)))),
                new Statement(ops(put(new ByteArray(key3), rval3)).yield(3)));

        StatementResult branch = invokeOnMs(iif);

        assertEquals(2, branch.getAsInt());

        assertEquals(5, storage.revision());
        assertEquals(7, storage.updateCounter());

        Entry e1 = storage.get(key1);
        assertEquals(5, e1.revision());
        assertArrayEquals(rval1, e1.value());

        Entry e2 = storage.get(key2);
        assertEquals(5, e2.revision());
        assertTrue(e2.tombstone());

        Entry e3 = storage.get(key3);
        assertEquals(4, e3.revision());
        assertArrayEquals(val3, e3.value());
    }

    /**
     * <pre>
     *   if (key1.value == val1 || exist(key2))
     *       if (key3.revision == 3):
     *           put(key1, rval1)
     *           return 1
     *       else
     *           put(key1, rval1)
     *           remove(key2)
     *           return 2
     *   else
     *       put(key3, rval3) <------ TEST FOR THIS BRANCH
     *       return 3
     * </pre>
     */
    @Test
    public void multiInvokeOperationsBranch3() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);
        byte[] rval1 = keyValue(1, 4);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);
        byte[] rval3 = keyValue(2, 6);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putToMs(key1, val2);

        assertEquals(1, storage.revision());
        assertEquals(1, storage.updateCounter());

        putToMs(key3, val3);

        assertEquals(2, storage.revision());
        assertEquals(2, storage.updateCounter());

        If iif = new If(
                new OrCondition(new ValueCondition(Type.EQUAL, key1, val1), new ExistenceCondition(ExistenceCondition.Type.EXISTS, key2)),
                new Statement(new If(
                        new RevisionCondition(RevisionCondition.Type.EQUAL, key3, 3),
                        new Statement(ops(put(new ByteArray(key1), rval1)).yield(1)),
                        new Statement(ops(put(new ByteArray(key1), rval1), remove(new ByteArray(key2))).yield(2)))),
                new Statement(ops(put(new ByteArray(key3), rval3)).yield(3)));

        StatementResult branch = invokeOnMs(iif);

        assertEquals(3, branch.getAsInt());

        assertEquals(3, storage.revision());
        assertEquals(4, storage.updateCounter());

        Entry e1 = storage.get(key1);
        assertEquals(1, e1.revision());
        assertArrayEquals(val2, e1.value());

        Entry e2 = storage.get(key2);
        assertTrue(e2.empty());

        Entry e3 = storage.get(key3);
        assertEquals(3, e3.revision());
        assertArrayEquals(rval3, e3.value());
    }

    @Test
    public void rangeCursor() {
        byte[] key1 = key(1);
        byte[] val1 = keyValue(1, 1);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 2);

        byte[] key3 = key(3);
        byte[] val3 = keyValue(3, 3);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        putAllToMs(List.of(key1, key2, key3), List.of(val1, val2, val3));

        assertEquals(1, storage.revision());
        assertEquals(3, storage.updateCounter());

        // Range for latest revision without max bound.
        try (Cursor<Entry> cur = storage.range(key1, null)) {
            assertTrue(cur.hasNext());

            Entry e1 = cur.next();

            assertFalse(e1.empty());
            assertFalse(e1.tombstone());
            assertArrayEquals(key1, e1.key());
            assertArrayEquals(val1, e1.value());
            assertEquals(1, e1.revision());
            assertEquals(1, e1.updateCounter());

            assertTrue(cur.hasNext());

            Entry e2 = cur.next();

            assertFalse(e2.empty());
            assertFalse(e2.tombstone());
            assertArrayEquals(key2, e2.key());
            assertArrayEquals(val2, e2.value());
            assertEquals(1, e2.revision());
            assertEquals(2, e2.updateCounter());

            // Deliberately don't call cur.hasNext()

            Entry e3 = cur.next();

            assertFalse(e3.empty());
            assertFalse(e3.tombstone());
            assertArrayEquals(key3, e3.key());
            assertArrayEquals(val3, e3.value());
            assertEquals(1, e3.revision());
            assertEquals(3, e3.updateCounter());

            assertFalse(cur.hasNext());

            try {
                cur.next();

                fail();
            } catch (NoSuchElementException e) {
                // No-op.
            }
        }

        // Range for latest revision with max bound.
        try (Cursor<Entry> cur = storage.range(key1, key3)) {
            assertTrue(cur.hasNext());

            Entry e1 = cur.next();

            assertFalse(e1.empty());
            assertFalse(e1.tombstone());
            assertArrayEquals(key1, e1.key());
            assertArrayEquals(val1, e1.value());
            assertEquals(1, e1.revision());
            assertEquals(1, e1.updateCounter());

            assertTrue(cur.hasNext());

            Entry e2 = cur.next();

            assertFalse(e2.empty());
            assertFalse(e2.tombstone());
            assertArrayEquals(key2, e2.key());
            assertArrayEquals(val2, e2.value());
            assertEquals(1, e2.revision());
            assertEquals(2, e2.updateCounter());

            assertFalse(cur.hasNext());

            try {
                cur.next();

                fail();
            } catch (NoSuchElementException e) {
                // No-op.
            }
        }
    }

    @Test
    public void testStartWatchesForNextRevision() {
        putToMs(key(0), keyValue(0, 0));

        long appliedRevision = storage.revision();

        storage.startWatches(1, new OnRevisionAppliedCallback() {
            @Override
            public void onSafeTimeAdvanced(HybridTimestamp newSafeTime) {
                // No-op.
            }

            @Override
            public void onRevisionApplied(long revision) {
                // No-op.
            }
        });

        CompletableFuture<byte[]> fut = new CompletableFuture<>();

        storage.watchExact(key(0), appliedRevision + 1, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                fut.complete(event.entryEvent().newEntry().value());

                return nullCompletedFuture();
            }

            @Override
            public void onError(Throwable e) {
                fut.completeExceptionally(e);
            }
        });

        byte[] newValue = keyValue(0, 1);

        putToMs(key(0), newValue);

        assertThat(fut, willBe(newValue));
    }

    @Test
    public void watchLexicographicTest() {
        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        byte[] key = key(0);
        byte[] val = keyValue(0, 0);

        int count = 1000; // Exceeds 1 byte

        CompletableFuture<Void> awaitFuture = watchExact(key, 1, count, (event, state) -> {
            assertTrue(event.single());

            Entry entry = event.entryEvent().newEntry();

            byte[] entryKey = entry.key();

            assertEquals((long) state, entry.revision());

            assertArrayEquals(key, entryKey);
        });

        for (int i = 0; i < count; i++) {
            putToMs(key, val);
        }

        assertEquals(count, storage.revision());
        assertEquals(count, storage.updateCounter());

        assertThat(awaitFuture, willCompleteSuccessfully());
    }

    @Test
    public void testWatchRange() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);

        byte[] key2 = key(2);
        byte[] val21 = keyValue(2, 21);
        byte[] val22 = keyValue(2, 22);

        byte[] key3 = key(3);
        byte[] val31 = keyValue(3, 31);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        // Watch for all updates starting from revision 2.
        CompletableFuture<Void> awaitFuture = watchRange(key1, null, 2, 2, (event, state) -> {
            if (state == 1) {
                assertFalse(event.single());

                Map<ByteArray, EntryEvent> map = event.entryEvents().stream()
                        .collect(Collectors.toMap(evt -> new ByteArray(evt.newEntry().key()), identity()));

                assertEquals(2, map.size());

                // First update under revision.
                EntryEvent e2 = map.get(new ByteArray(key2));

                assertNotNull(e2);

                Entry oldEntry2 = e2.oldEntry();

                assertFalse(oldEntry2.empty());
                assertFalse(oldEntry2.tombstone());
                assertEquals(1, oldEntry2.revision());
                assertEquals(2, oldEntry2.updateCounter());
                assertArrayEquals(key2, oldEntry2.key());
                assertArrayEquals(val21, oldEntry2.value());

                Entry newEntry2 = e2.newEntry();

                assertFalse(newEntry2.empty());
                assertFalse(newEntry2.tombstone());
                assertEquals(2, newEntry2.revision());
                assertEquals(3, newEntry2.updateCounter());
                assertArrayEquals(key2, newEntry2.key());
                assertArrayEquals(val22, newEntry2.value());

                // Second update under revision.
                EntryEvent e3 = map.get(new ByteArray(key3));

                assertNotNull(e3);

                Entry oldEntry3 = e3.oldEntry();

                assertTrue(oldEntry3.empty());
                assertFalse(oldEntry3.tombstone());
                assertArrayEquals(key3, oldEntry3.key());

                Entry newEntry3 = e3.newEntry();

                assertFalse(newEntry3.empty());
                assertFalse(newEntry3.tombstone());
                assertEquals(2, newEntry3.revision());
                assertEquals(4, newEntry3.updateCounter());
                assertArrayEquals(key3, newEntry3.key());
                assertArrayEquals(val31, newEntry3.value());
            } else if (state == 2) {
                assertTrue(event.single());

                EntryEvent e1 = event.entryEvent();

                Entry oldEntry1 = e1.oldEntry();

                assertFalse(oldEntry1.empty());
                assertFalse(oldEntry1.tombstone());
                assertEquals(1, oldEntry1.revision());
                assertEquals(1, oldEntry1.updateCounter());
                assertArrayEquals(key1, oldEntry1.key());
                assertArrayEquals(val11, oldEntry1.value());

                Entry newEntry1 = e1.newEntry();

                assertFalse(newEntry1.empty());
                assertTrue(newEntry1.tombstone());
                assertEquals(3, newEntry1.revision());
                assertEquals(5, newEntry1.updateCounter());
                assertArrayEquals(key1, newEntry1.key());
                assertNull(newEntry1.value());
            }
        });

        putAllToMs(List.of(key1, key2), List.of(val11, val21));

        assertEquals(1, storage.revision());
        assertEquals(2, storage.updateCounter());

        putAllToMs(List.of(key2, key3), List.of(val22, val31));

        assertEquals(2, storage.revision());
        assertEquals(4, storage.updateCounter());

        removeFromMs(key1);

        assertThat(awaitFuture, willCompleteSuccessfully());
    }

    @Test
    public void testWatchExact() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);
        byte[] val12 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val21 = keyValue(2, 21);
        byte[] val22 = keyValue(2, 22);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        CompletableFuture<Void> awaitFuture = watchExact(key1, 1, 2, (event, state) -> {
            if (state == 1) {
                assertTrue(event.single());

                EntryEvent e1 = event.entryEvent();

                Entry oldEntry1 = e1.oldEntry();

                assertTrue(oldEntry1.empty());
                assertFalse(oldEntry1.tombstone());

                Entry newEntry1 = e1.newEntry();

                assertFalse(newEntry1.empty());
                assertFalse(newEntry1.tombstone());
                assertEquals(1, newEntry1.revision());
                assertEquals(1, newEntry1.updateCounter());
                assertArrayEquals(key1, newEntry1.key());
                assertArrayEquals(val11, newEntry1.value());
            } else if (state == 2) {
                assertTrue(event.single());

                EntryEvent e1 = event.entryEvent();

                Entry oldEntry1 = e1.oldEntry();

                assertFalse(oldEntry1.empty());
                assertFalse(oldEntry1.tombstone());
                assertEquals(1, oldEntry1.revision());
                assertEquals(1, oldEntry1.updateCounter());
                assertArrayEquals(key1, oldEntry1.key());
                assertArrayEquals(val11, oldEntry1.value());

                Entry newEntry1 = e1.newEntry();

                assertFalse(newEntry1.empty());
                assertFalse(newEntry1.tombstone());
                assertEquals(3, newEntry1.revision());
                assertEquals(4, newEntry1.updateCounter());
                assertArrayEquals(key1, newEntry1.key());
                assertArrayEquals(val12, newEntry1.value());
            }
        });

        putAllToMs(List.of(key1, key2), List.of(val11, val21));

        assertEquals(1, storage.revision());
        assertEquals(2, storage.updateCounter());

        putToMs(key2, val22);
        putToMs(key1, val12);

        assertThat(awaitFuture, willCompleteSuccessfully());
    }

    @Test
    public void watchExactSkipNonMatchingEntries() {
        byte[] key1 = key(1);
        byte[] val1v1 = keyValue(1, 11);
        byte[] val1v2 = keyValue(1, 12);

        byte[] key2 = key(2);
        byte[] val2 = keyValue(2, 21);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        CompletableFuture<Void> awaitFuture = watchExact(key2, 1, 1, (event, state) -> {
            assertTrue(event.single());

            EntryEvent e1 = event.entryEvent();

            Entry oldEntry1 = e1.oldEntry();

            assertTrue(oldEntry1.empty());
            assertFalse(oldEntry1.tombstone());

            Entry newEntry1 = e1.newEntry();

            assertFalse(newEntry1.empty());
            assertFalse(newEntry1.tombstone());
            assertEquals(3, newEntry1.revision());
            assertEquals(3, newEntry1.updateCounter());
            assertArrayEquals(key2, newEntry1.key());
            assertArrayEquals(val2, newEntry1.value());
        });

        putToMs(key1, val1v1);
        putToMs(key1, val1v2);
        putToMs(key2, val2);

        assertEquals(3, storage.revision());
        assertEquals(3, storage.updateCounter());

        assertThat(awaitFuture, willCompleteSuccessfully());
    }

    @Test
    public void watchExactForKeys() {
        byte[] key1 = key(1);
        byte[] val11 = keyValue(1, 11);

        byte[] key2 = key(2);
        byte[] val21 = keyValue(2, 21);
        byte[] val22 = keyValue(2, 22);

        byte[] key3 = key(3);
        byte[] val31 = keyValue(3, 31);
        byte[] val32 = keyValue(3, 32);

        assertEquals(0, storage.revision());
        assertEquals(0, storage.updateCounter());

        CompletableFuture<Void> awaitFuture = watchExact(List.of(key1, key2), 1, 2, (event, state) -> {
            if (state == 1) {
                assertFalse(event.single());
            } else if (state == 2) {
                assertTrue(event.single());
            }
        });

        putAllToMs(List.of(key1, key2, key3), List.of(val11, val21, val31));

        assertEquals(1, storage.revision());
        assertEquals(3, storage.updateCounter());

        putToMs(key2, val22);

        putToMs(key3, val32);

        assertThat(awaitFuture, willCompleteSuccessfully());
    }

    /**
     * Tests that, if a watch throws an exception, its {@code onError} method is invoked.
     */
    @Test
    void testWatchErrorHandling() {
        byte[] value = "value".getBytes(UTF_8);

        var key = "foo".getBytes(UTF_8);

        WatchListener mockListener1 = mock(WatchListener.class);
        WatchListener mockListener2 = mock(WatchListener.class);
        WatchListener mockListener3 = mock(WatchListener.class);

        when(mockListener1.onUpdate(any())).thenReturn(nullCompletedFuture());

        when(mockListener2.onUpdate(any())).thenReturn(nullCompletedFuture());

        when(mockListener3.onUpdate(any())).thenReturn(nullCompletedFuture());

        var exception = new IllegalStateException();

        doThrow(exception).when(mockListener2).onUpdate(any());

        storage.watchExact(key, 1, mockListener1);
        storage.watchExact(key, 1, mockListener2);
        storage.watchExact(key, 1, mockListener3);

        OnRevisionAppliedCallback mockCallback = mock(OnRevisionAppliedCallback.class);

        storage.startWatches(1, mockCallback);

        putToMs(key, value);

        verify(mockListener1, timeout(10_000)).onUpdate(any());

        verify(mockListener2, timeout(10_000)).onError(exception);

        verify(mockListener3, timeout(10_000)).onUpdate(any());

        verify(mockCallback, never()).onRevisionApplied(anyLong());
    }

    @Test
    public void testRevisionByTimestamp() {
        // Verify that in case of empty storage -1 will be returned.
        assertEquals(-1, storage.revisionByTimestamp(MIN_VALUE));
        assertEquals(-1, storage.revisionByTimestamp(hybridTimestamp(2)));
        assertEquals(-1, storage.revisionByTimestamp(hybridTimestamp(5)));
        assertEquals(-1, storage.revisionByTimestamp(hybridTimestamp(7)));
        assertEquals(-1, storage.revisionByTimestamp(hybridTimestamp(10)));
        assertEquals(-1, storage.revisionByTimestamp(hybridTimestamp(12)));
        assertEquals(-1, storage.revisionByTimestamp(hybridTimestamp(15)));
        assertEquals(-1, storage.revisionByTimestamp(hybridTimestamp(17)));
        assertEquals(-1, storage.revisionByTimestamp(MAX_VALUE));

        // Populate storage with some data in order to have following revision to timestamp mapping:
        // 1 -> 5
        // 2 -> 10
        // 3 -> 15
        {
            storage.put(key(1), keyValue(1, 1), hybridTimestamp(5));
            assertEquals(1, storage.revision());

            storage.put(key(1), keyValue(1, 1), hybridTimestamp(10));
            assertEquals(2, storage.revision());

            storage.put(key(2), keyValue(2, 2), hybridTimestamp(15));
            assertEquals(3, storage.revision());
        }

        // Check revisionByTimestamp()
        {
            assertEquals(-1, storage.revisionByTimestamp(MIN_VALUE));

            // There's no revision associated with 2, so closest left one is expected.
            assertEquals(-1, storage.revisionByTimestamp(hybridTimestamp(2)));

            // Exact matching 1 -> 5
            assertEquals(1, storage.revisionByTimestamp(hybridTimestamp(5)));

            // There's no revision associated with 7, so closest left one is expected.
            assertEquals(1, storage.revisionByTimestamp(hybridTimestamp(7)));

            // Exact matching 2 -> 10
            assertEquals(2, storage.revisionByTimestamp(hybridTimestamp(10)));

            // There's no revision associated with 12, so closest left one is expected.
            assertEquals(2, storage.revisionByTimestamp(hybridTimestamp(12)));

            // Exact matching 3 -> 15
            assertEquals(3, storage.revisionByTimestamp(hybridTimestamp(15)));

            // There's no revision associated with 17, so closest left one is expected.
            assertEquals(3, storage.revisionByTimestamp(hybridTimestamp(17)));

            assertEquals(3, storage.revisionByTimestamp(MAX_VALUE));
        }
    }

    private CompletableFuture<Void> watchExact(
            byte[] key, long revision, int expectedNumCalls, BiConsumer<WatchEvent, Integer> testCondition
    ) {
        return watch(listener -> storage.watchExact(key, revision, listener), testCondition, expectedNumCalls);
    }

    private CompletableFuture<Void> watchExact(
            Collection<byte[]> keys, long revision, int expectedNumCalls, BiConsumer<WatchEvent, Integer> testCondition
    ) {
        return watch(listener -> storage.watchExact(keys, revision, listener), testCondition, expectedNumCalls);
    }

    private CompletableFuture<Void> watchRange(
            byte[] keyFrom, byte @Nullable [] keyTo, long revision, int expectedNumCalls, BiConsumer<WatchEvent, Integer> testCondition
    ) {
        return watch(listener -> storage.watchRange(keyFrom, keyTo, revision, listener), testCondition, expectedNumCalls);
    }

    private CompletableFuture<Void> watch(
            Consumer<WatchListener> watchMethod, BiConsumer<WatchEvent, Integer> testCondition, int expectedNumCalls
    ) {
        var state = new AtomicInteger();

        var resultFuture = new CompletableFuture<Void>();

        watchMethod.accept(new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                try {
                    var curState = state.incrementAndGet();

                    testCondition.accept(event, curState);

                    if (curState == expectedNumCalls) {
                        resultFuture.complete(null);
                    }

                    return nullCompletedFuture();
                } catch (Exception e) {
                    resultFuture.completeExceptionally(e);
                }

                return nullCompletedFuture();
            }

            @Override
            public void onError(Throwable e) {
                resultFuture.completeExceptionally(e);
            }
        });

        storage.startWatches(1, new OnRevisionAppliedCallback() {
            @Override
            public void onSafeTimeAdvanced(HybridTimestamp newSafeTime) {
                // No-op.
            }

            @Override
            public void onRevisionApplied(long revision) {
                // No-op.
            }
        });

        return resultFuture;
    }

    void putToMs(byte[] key, byte[] value) {
        storage.put(key, value, MIN_VALUE);
    }

    private void putAllToMs(List<byte[]> keys, List<byte[]> values) {
        storage.putAll(keys, values, MIN_VALUE);
    }

    private void removeFromMs(byte[] key) {
        storage.remove(key, MIN_VALUE);
    }

    private void removeAllFromMs(List<byte[]> keys) {
        storage.removeAll(keys, MIN_VALUE);
    }

    private boolean invokeOnMs(Condition condition, Collection<Operation> success, Collection<Operation> failure) {
        return storage.invoke(
                condition,
                success,
                failure,
                MIN_VALUE,
                new CommandIdGenerator(() -> UUID.randomUUID().toString()).newId()
        );
    }

    private StatementResult invokeOnMs(If iif) {
        return storage.invoke(
                iif,
                MIN_VALUE,
                new CommandIdGenerator(() -> UUID.randomUUID().toString()).newId()
        );
    }
}
