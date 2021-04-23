/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.vault.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.vault.common.VaultEntry;
import org.apache.ignite.internal.vault.common.VaultWatch;
import org.apache.ignite.internal.vault.service.VaultService;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Test for base vault contracts.
 */
public class VaultBaseContractsTest {
    /** Vault. */
    private VaultService storage;

    /**
     * Instantiate vault.
     */
    @BeforeEach
    public void setUp() {
        storage = new VaultServiceImpl();
    }

    /**
     * put contract
     */
    @Test
    public void put() throws ExecutionException, InterruptedException {
        ByteArray key = getKey(1);
        byte[] val = getValue(key, 1);

        assertNull(storage.get(key).get().value());

        storage.put(key, val);

        VaultEntry v = storage.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v.value());

        storage.put(key, val);

        v = storage.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v.value());
    }

    /**
     * remove contract.
     */
    @Test
    public void remove() throws ExecutionException, InterruptedException {
        ByteArray key = getKey(1);
        byte[] val = getValue(key, 1);

        assertNull(storage.get(key).get().value());

        // Remove non-existent value.
        storage.remove(key);

        assertNull(storage.get(key).get().value());

        storage.put(key, val);

        VaultEntry v = storage.get(key).get();

        assertFalse(v.empty());
        assertEquals(val, v.value());

        // Remove existent value.
        storage.remove(key);

        v = storage.get(key).get();

        assertNull(v.value());
    }

    /**
     * range contract.
     */
    @Test
    public void range() throws ExecutionException, InterruptedException {
        ByteArray key;

        Map<ByteArray, byte[]> values = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            key = getKey(i);

            values.put(key, getValue(key, i));

            assertNull(storage.get(key).get().value());
        }

        values.forEach((k, v) -> storage.put(k, v));

        for (Map.Entry<ByteArray, byte[]> entry : values.entrySet())
            assertEquals(entry.getValue(), storage.get(entry.getKey()).get().value());

        Iterator<VaultEntry> it = storage.range(getKey(3), getKey(7));

        List<VaultEntry> rangeRes = new ArrayList<>();

        it.forEachRemaining(rangeRes::add);

        assertEquals(4, rangeRes.size());

        //Check that we have exact range from "key3" to "key6"
        for (int i = 3; i < 7; i++)
            assertEquals(values.get(getKey(i)), rangeRes.get(i - 3).value());
    }

    /**
     * watch contract.
     */
    @Test
    public void watch() throws ExecutionException, InterruptedException {
        ByteArray key;

        Map<ByteArray, byte[]> values = new HashMap<>();

        for (int i = 0; i < 10; i++) {
            key = getKey(i);

            values.put(key, getValue(key, i));
        }

        values.forEach((k, v) -> storage.put(k, v));

        for (Map.Entry<ByteArray, byte[]> entry : values.entrySet())
            assertEquals(entry.getValue(), storage.get(entry.getKey()).get().value());

        AtomicInteger counter = new AtomicInteger();

        VaultWatch vaultWatch = new VaultWatch(changedValue -> counter.incrementAndGet());

        vaultWatch.startKey(getKey(3));
        vaultWatch.endKey(getKey(7));

        storage.watch(vaultWatch);

        for (int i = 3; i < 7; i++)
            storage.put(getKey(i), ("new" + i).getBytes());

        Thread.sleep(500);

        assertEquals(4, counter.get());
    }

    /**
     * putAll contract.
     */
    @Test
    public void putAllAndRevision() throws ExecutionException, InterruptedException {
        Map<ByteArray, byte[]> entries = new HashMap<>();

        int entriesNum = 100;

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            entries.put(key, getValue(key, i));
        }

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            assertNull(storage.get(key).get().value());
        }

        storage.putAll(entries, 1L);

        for (int i = 0; i < entriesNum; i++) {
            ByteArray key = getKey(i);

            assertEquals(entries.get(key), storage.get(key).get().value());
        }

        assertEquals(1L, storage.appliedRevision().get());
    }

    /**
     * Creates key for vault entry.
     */
    private static ByteArray getKey(int k) {
        return ByteArray.fromString("key" + k);
    }

    /**
     * Creates value represented by byte array.
     */
    private static byte[] getValue(ByteArray k, int v) {
        return ("key" + k + '_' + "val" + v).getBytes();
    }
}
