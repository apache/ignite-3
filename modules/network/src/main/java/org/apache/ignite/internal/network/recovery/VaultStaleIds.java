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

package org.apache.ignite.internal.network.recovery;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.joining;

import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;

/**
 * {@link StaleIds} implementation using Vault as a persistent storage.
 */
public class VaultStaleIds implements StaleIds {
    private static final ByteArray STALE_IDS_KEY = new ByteArray("network.staleIds");

    private static final int DEFAULT_MAX_IDS_TO_REMEMBER = 10_000;

    private final VaultManager vaultManager;

    private final int maxIdsToRemember;

    private Set<UUID> staleIds;

    public VaultStaleIds(VaultManager vaultManager) {
        this(vaultManager, DEFAULT_MAX_IDS_TO_REMEMBER);
    }

    public VaultStaleIds(VaultManager vaultManager, int maxIdsToRemember) {
        this.vaultManager = vaultManager;
        this.maxIdsToRemember = maxIdsToRemember;
    }

    @Override
    public synchronized boolean isIdStale(UUID nodeId) {
        loadFromVaultIfFirstOperation();

        return staleIds.contains(nodeId);
    }

    private void loadFromVaultIfFirstOperation() {
        if (staleIds == null) {
            staleIds = loadStaleIdsFromVault();
        }
    }

    private Set<UUID> loadStaleIdsFromVault() {
        VaultEntry entry = vaultManager.get(STALE_IDS_KEY);

        if (entry == null) {
            return new LinkedHashSet<>();
        }

        String[] idsArray = new String(entry.value(), UTF_8).split("\n");

        Set<UUID> result = new LinkedHashSet<>();

        for (String id : idsArray) {
            result.add(UUID.fromString(id));
        }

        return result;
    }

    @Override
    public synchronized void markAsStale(UUID nodeId) {
        loadFromVaultIfFirstOperation();

        staleIds.add(nodeId);

        int idsToRemove = staleIds.size() - maxIdsToRemember;

        Iterator<UUID> iterator = staleIds.iterator();
        for (int i = 0; i < idsToRemove; i++) {
            iterator.next();
            iterator.remove();
        }

        saveIdsToVault();
    }

    private void saveIdsToVault() {
        String joinedIds = staleIds.stream().map(UUID::toString).collect(joining("\n"));

        vaultManager.put(STALE_IDS_KEY, joinedIds.getBytes(UTF_8));
    }
}
