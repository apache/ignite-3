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

package org.apache.ignite.internal.vault.inmemory;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.CursorUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultService;
import org.jetbrains.annotations.Nullable;

/**
 * Simple in-memory representation of the Vault Service.
 */
public class InMemoryVaultService implements VaultService {
    /** Map to store values. */
    private final NavigableMap<ByteArray, byte[]> storage = new TreeMap<>();

    /** Mutex. */
    private final Object mux = new Object();

    @Override
    public void start() {
        // No-op.
    }

    @Override
    public void close() {
        // No-op.
    }

    @Override
    public @Nullable VaultEntry get(ByteArray key) {
        synchronized (mux) {
            byte[] value = storage.get(key);

            return value == null ? null : new VaultEntry(key, value);
        }
    }

    @Override
    public void put(ByteArray key, byte @Nullable [] val) {
        synchronized (mux) {
            storage.put(key, val);
        }
    }

    @Override
    public void remove(ByteArray key) {
        synchronized (mux) {
            storage.remove(key);
        }
    }

    @Override
    public Cursor<VaultEntry> range(ByteArray fromKey, ByteArray toKey) {
        if (fromKey.compareTo(toKey) >= 0) {
            return CursorUtils.emptyCursor();
        }

        synchronized (mux) {
            return storage.subMap(fromKey, toKey).entrySet().stream()
                    .map(e -> new VaultEntry(new ByteArray(e.getKey()), e.getValue()))
                    .collect(collectingAndThen(toList(), Cursor::fromIterable));
        }
    }

    @Override
    public void putAll(Map<ByteArray, byte[]> vals) {
        synchronized (mux) {
            for (Map.Entry<ByteArray, byte[]> entry : vals.entrySet()) {
                if (entry.getValue() == null) {
                    storage.remove(entry.getKey());
                } else {
                    storage.put(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}
