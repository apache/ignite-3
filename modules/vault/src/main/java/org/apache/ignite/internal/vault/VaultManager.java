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

package org.apache.ignite.internal.vault;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.Nullable;

/**
 * VaultManager is responsible for handling {@link VaultService} lifecycle and providing interface for managing local keys.
 */
public class VaultManager implements IgniteComponent {
    /** Special key, which reserved for storing the name of the current node. */
    private static final ByteArray NODE_NAME = new ByteArray("node_name");

    /** Instance of vault. */
    private final VaultService vaultSvc;

    /**
     * Default constructor.
     *
     * @param vaultSvc Instance of vault.
     */
    public VaultManager(VaultService vaultSvc) {
        this.vaultSvc = vaultSvc;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        vaultSvc.start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        // TODO: IGNITE-15161 Implement component's stop.
        vaultSvc.close();
    }

    /**
     * See {@link VaultService#get}.
     *
     * @param key Key. Cannot be {@code null}.
     * @return Future that resolves into an entry for the given key, or {@code null} if no such mapping exists.
     */
    public CompletableFuture<VaultEntry> get(ByteArray key) {
        return vaultSvc.get(key);
    }

    /**
     * See {@link VaultService#put}.
     *
     * @param key Vault key. Cannot be {@code null}.
     * @param val Value. If value is equal to {@code null}, then previous value with key will be deleted if there was any mapping.
     * @return Future representing pending completion of the operation. Cannot be {@code null}.
     */
    public CompletableFuture<Void> put(ByteArray key, byte @Nullable [] val) {
        return vaultSvc.put(key, val);
    }

    /**
     * See {@link VaultService#remove}.
     *
     * @param key Vault key. Cannot be {@code null}.
     * @return Future representing pending completion of the operation. Cannot be {@code null}.
     */
    public CompletableFuture<Void> remove(ByteArray key) {
        return vaultSvc.remove(key);
    }

    /**
     * See {@link VaultService#range}.
     *
     * @param fromKey Start key of range (inclusive). Cannot be {@code null}.
     * @param toKey   End key of range (exclusive). Cannot be {@code null}.
     * @return Iterator built upon entries corresponding to the given range.
     */
    public Cursor<VaultEntry> range(ByteArray fromKey, ByteArray toKey) {
        return vaultSvc.range(fromKey, toKey);
    }

    /**
     * Inserts or updates entries with given keys and given values. If the given value in {@code vals} is {@code null}, then corresponding
     * value with key will be deleted if there was any mapping.
     *
     * @param vals The map of keys and corresponding values. Cannot be {@code null} or empty.
     * @return Future representing pending completion of the operation. Cannot be {@code null}.
     */
    public CompletableFuture<Void> putAll(Map<ByteArray, byte[]> vals) {
        return vaultSvc.putAll(vals);
    }

    /**
     * Persist node name to the vault.
     *
     * @param name node name to persist. Cannot be null.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> putName(String name) {
        if (name.isBlank()) {
            throw new IllegalArgumentException("Name must not be empty");
        }

        return put(NODE_NAME, name.getBytes(UTF_8));
    }

    /**
     * Returns {@code CompletableFuture} which, when complete, returns the node name, if was stored earlier, or {@code null} otherwise.
     */
    public CompletableFuture<String> name() {
        return vaultSvc.get(NODE_NAME).thenApply(name -> name == null ? null : new String(name.value(), UTF_8));
    }
}
