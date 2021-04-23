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

package org.apache.ignite.internal.vault.service;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.vault.common.VaultEntry;
import org.apache.ignite.internal.vault.common.VaultWatch;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/**
 * Defines interface for accessing to a vault service.
 */
public interface VaultService {
    /**
     * Read key from vault storage or {@code null} if this storage contains no mapping for the key.
     *
     * @param key Key.
     */
    CompletableFuture<VaultEntry> get(ByteArray key);

    /**
     * Write value with key to vault.
     *
     * @param key Vault key.
     * @param val Value.
     */
    CompletableFuture<Void> put(ByteArray key, byte[] val);

    /**
     * Remove value with key from vault.
     *
     * @param key Vault key.
     */
    CompletableFuture<Void> remove(ByteArray key);

    /**
     * Returns a view of the portion of vault whose keys range from fromKey, inclusive, to toKey, exclusive.
     */
    Iterator<VaultEntry> range(ByteArray fromKey, ByteArray toKey);

    /**
     * Subscribes on vault storage updates for the given key.
     *
     * @param vaultWatch Watch which will notify for each update.
     * @return Subscription identifier. Could be used in {@link #stopWatch} method in order to cancel subscription.
     */
    @NotNull
    CompletableFuture<IgniteUuid> watch(@NotNull VaultWatch vaultWatch);

    /**
     * Cancels subscription for the given identifier.
     *
     * @param id Subscription identifier.
     * @return Completed future in case of operation success. Couldn't be {@code null}.
     */
    @NotNull
    CompletableFuture<Void> stopWatch(@NotNull IgniteUuid id);

    /**
     * Inserts or updates entries with given keys and given values and non-negative revision.
     *
     * @param vals The map of keys and corresponding values. Couldn't be {@code null} or empty.
     * @param revision Revision for entries. Couldn't be negative.
     * @return Completed future.
     */
    @NotNull
    CompletableFuture<Void> putAll(@NotNull Map<ByteArray, byte[]> vals, long revision);

    /**
     * @return Applied revision for {@link VaultService#putAll} operation.
     */
    @NotNull
    CompletableFuture<Long> appliedRevision();
}
