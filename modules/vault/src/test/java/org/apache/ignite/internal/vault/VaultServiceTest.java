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

package org.apache.ignite.internal.vault;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.util.Cursor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Base class for testing {@link VaultService} implementations.
 */
public abstract class VaultServiceTest {
    /** Vault. */
    private VaultService vaultService;

    /**
     * Before each.
     */
    @BeforeEach
    public void setUp() throws IOException {
        vaultService = getVaultService();

        vaultService.start();
    }

    /**
     * After each.
     */
    @AfterEach
    void tearDown() {
        vaultService.close();
    }

    /**
     * Returns the vault service that will be tested.
     */
    protected abstract VaultService getVaultService();

    /**
     * Tests regular behaviour of the {@link VaultService#put} method.
     */
    @Test
    public void testPut() {
        ByteArray key = getKey(1);

        assertThat(vaultService.get(key), is(nullValue(VaultEntry.class)));

        byte[] val = getValue(1);

        vaultService.put(key, val);

        assertThat(vaultService.get(key), is(equalTo(new VaultEntry(key, val))));

        // test idempotency
        vaultService.put(key, val);

        assertThat(vaultService.get(key), is(equalTo(new VaultEntry(key, val))));
    }

    /**
     * Tests that the {@link VaultService#put} method removes the given {@code key} if {@code value} equalTo {@code null}.
     */
    @Test
    public void testPutWithNull() {
        ByteArray key = getKey(1);

        byte[] val = getValue(1);

        vaultService.put(key, val);

        assertThat(vaultService.get(key), is(equalTo(new VaultEntry(key, val))));

        vaultService.put(key, null);

        assertThat(vaultService.get(key), is(nullValue(VaultEntry.class)));
    }

    /**
     * Tests regular behaviour of the {@link VaultService#remove} method.
     */
    @Test
    public void testRemove() {
        ByteArray key = getKey(1);

        // Remove non-existent value.
        assertDoesNotThrow(() -> vaultService.remove(key));

        assertThat(vaultService.get(key), is(nullValue(VaultEntry.class)));

        byte[] val = getValue(1);

        vaultService.put(key, val);

        assertThat(vaultService.get(key), is(equalTo(new VaultEntry(key, val))));

        // Remove existing value.
        vaultService.remove(key);

        assertThat(vaultService.get(key), is(nullValue(VaultEntry.class)));
    }

    /**
     * Tests regular behaviour of the {@link VaultService#putAll} method.
     */
    @Test
    public void testPutAll() {
        Map<ByteArray, byte[]> batch = IntStream.range(0, 10)
                .boxed()
                .collect(toMap(VaultServiceTest::getKey, VaultServiceTest::getValue));

        vaultService.putAll(batch);

        batch.forEach((k, v) -> assertThat(vaultService.get(k), is(equalTo(new VaultEntry(k, v)))));

        vaultService.putAll(batch);

        batch.forEach((k, v) -> assertThat(vaultService.get(k), is(equalTo(new VaultEntry(k, v)))));
    }

    /**
     * Tests that the {@link VaultService#putAll} method will remove keys, which values are {@code null}.
     */
    @Test
    public void testPutAllWithNull() {
        Map<ByteArray, byte[]> batch = IntStream.range(0, 10)
                .boxed()
                .collect(toMap(VaultServiceTest::getKey, VaultServiceTest::getValue));

        vaultService.putAll(batch);

        batch.forEach((k, v) -> assertThat(vaultService.get(k), is(equalTo(new VaultEntry(k, v)))));

        Map<ByteArray, byte[]> secondBatch = new HashMap<>();

        secondBatch.put(getKey(4), getValue(3));
        secondBatch.put(getKey(8), getValue(3));
        secondBatch.put(getKey(1), null);
        secondBatch.put(getKey(3), null);

        vaultService.putAll(secondBatch);

        assertThat(vaultService.get(getKey(4)), is(equalTo(new VaultEntry(getKey(4), getValue(3)))));
        assertThat(vaultService.get(getKey(8)), is(equalTo(new VaultEntry(getKey(8), getValue(3)))));
        assertThat(vaultService.get(getKey(1)), is(nullValue(VaultEntry.class)));
        assertThat(vaultService.get(getKey(3)), is(nullValue(VaultEntry.class)));
    }

    /**
     * Tests regular behaviour of the {@link VaultService#range} method.
     */
    @Test
    public void testRange() throws Exception {
        List<VaultEntry> entries = getRange(0, 10);

        Map<ByteArray, byte[]> batch = entries.stream().collect(toMap(VaultEntry::key, VaultEntry::value));

        vaultService.putAll(batch);

        List<VaultEntry> range = range(getKey(3), getKey(7));

        assertThat(range, equalTo(getRange(3, 7)));
    }

    /**
     * Tests that the {@link VaultService#range} returns valid entries when passed a larger range, than the available data.
     */
    @Test
    public void testRangeBoundaries() throws Exception {
        List<VaultEntry> entries = getRange(3, 5);

        Map<ByteArray, byte[]> batch = entries.stream().collect(toMap(VaultEntry::key, VaultEntry::value));

        vaultService.putAll(batch);

        List<VaultEntry> range = range(getKey(0), getKey(9));

        assertThat(range, equalTo(entries));
    }

    /**
     * Tests that the {@link VaultService#range} upper bound equalTo not included.
     */
    @Test
    public void testRangeNotIncludedBoundary() throws Exception {
        List<VaultEntry> entries = getRange(3, 5);

        Map<ByteArray, byte[]> batch = entries.stream().collect(toMap(VaultEntry::key, VaultEntry::value));

        vaultService.putAll(batch);

        List<VaultEntry> range = range(getKey(3), getKey(4));

        assertThat(range, contains(new VaultEntry(getKey(3), getValue(3))));
    }

    /**
     * Tests that an empty result equalTo returned when {@link VaultService#range} contains invalid boundaries.
     */
    @Test
    public void testRangeInvalidBoundaries() throws Exception {
        Map<ByteArray, byte[]> batch = getRange(3, 5).stream().collect(toMap(VaultEntry::key, VaultEntry::value));

        vaultService.putAll(batch);

        List<VaultEntry> range = range(getKey(4), getKey(1));

        assertThat(range, is(empty()));

        range = range(getKey(4), getKey(4));

        assertThat(range, is(empty()));
    }

    /**
     * Creates a test key.
     */
    private static ByteArray getKey(int k) {
        return new ByteArray("key" + k);
    }

    /**
     * Creates a test value.
     */
    private static byte[] getValue(int v) {
        return ("val" + v).getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Creates a range of test Vault entries.
     */
    private static List<VaultEntry> getRange(int from, int to) {
        return IntStream.range(from, to)
                .mapToObj(i -> new VaultEntry(getKey(i), getValue(i)))
                .collect(toList());
    }

    /**
     * Extracts the given range of values from the Vault.
     */
    private List<VaultEntry> range(ByteArray from, ByteArray to) throws Exception {
        try (Cursor<VaultEntry> cursor = vaultService.range(from, to)) {
            return cursor.stream().collect(toList());
        }
    }
}
