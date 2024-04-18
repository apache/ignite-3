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

import java.util.Map;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Defines interface for accessing to the Vault service.
 */
public interface VaultService extends ManuallyCloseable {
    /**
     * Starts the service.
     */
    void start();

    /**
     * Retrieves an entry for the given key.
     *
     * @param key Key. Cannot be {@code null}.
     * @return Entry for the given key, or {@code null} no such mapping exists.
     */
    @Nullable VaultEntry get(ByteArray key);

    /**
     * Writes a given value to the Vault. If the value is {@code null}, then the previous value under the same key (if any) will
     * be deleted.
     *
     * @param key Vault key. Cannot be {@code null}.
     * @param val Value. If value is equal to {@code null}, then the previous value under the same key (if any) will
     *      be deleted.
     */
    void put(ByteArray key, byte @Nullable [] val);

    /**
     * Removes a value from the vault.
     *
     * @param key Vault key. Cannot be {@code null}.
     */
    void remove(ByteArray key);

    /**
     * Returns a view of the portion of the vault whose keys range from {@code fromKey}, inclusive, to {@code toKey}, exclusive.
     *
     * @param fromKey Start key of range (inclusive). Cannot be {@code null}.
     * @param toKey End key of range (exclusive). Cannot be {@code null}.
     * @return Iterator built upon entries corresponding to the given range.
     */
    Cursor<VaultEntry> range(ByteArray fromKey, ByteArray toKey);

    /**
     * Inserts or updates entries with given keys and given values. If a given value in {@code vals} is {@code null},
     * then the corresponding key will be deleted.
     *
     * @param vals The map of keys and corresponding values. Cannot be {@code null}.
     */
    void putAll(Map<ByteArray, byte[]> vals);

    /**
     * Closes the service.
     */
    @Override
    void close();
}
