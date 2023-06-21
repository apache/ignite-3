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

package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.LocalMetaStorageManager;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.SimpleInMemoryKeyValueStorage;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests {@link LocalMetaStorageManager}.
 */
public class LocalMetaStorageManagerTest extends IgniteAbstractTest {
    private MetaStorageManagerImpl metaStorageManager;

    @BeforeEach
    public void beforeTest() {
        VaultManager vaultMgr = new VaultManager(new InMemoryVaultService());

        KeyValueStorage keyValueStorage = new SimpleInMemoryKeyValueStorage("test");

        metaStorageManager = StandaloneMetaStorageManager.create(vaultMgr, keyValueStorage);

        metaStorageManager.start();
    }

    @AfterEach
    public void afterTest() throws Exception {
        metaStorageManager.stop();
    }

    @Test
    public void getWithRevisionLowerUpperBoundFromLocalStorage() {
        ByteArray key1 = new ByteArray(toBytes("key1"));

        byte[] val1 = toBytes("value1");
        byte[] val2 = toBytes("value1");

        metaStorageManager.put(key1, val1);
        metaStorageManager.put(key1, val2);

        LocalMetaStorageManager localStorage = metaStorageManager.getLocalStorage();

        List<Entry> entries = localStorage.get(key1.bytes(), 1, 2);

        assertEquals(2, entries.size());

        assertArrayEquals(val1, entries.get(0).value());
        assertArrayEquals(val2, entries.get(1).value());
    }
}
