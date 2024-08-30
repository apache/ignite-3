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

package org.apache.ignite.internal.disaster.system.storage;

import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.Nullable;

/**
 * Storage (over Vault) used by the cluster reset tools.
 */
public class ClusterResetStorage {
    protected static final ByteArray RESET_CLUSTER_MESSAGE_VAULT_KEY = new ByteArray("systemRecovery.resetClusterMessage");

    private final VaultManager vault;

    public ClusterResetStorage(VaultManager vault) {
        this.vault = vault;
    }

    protected final <T> @Nullable T readFromVault(ByteArray key) {
        VaultEntry entry = vault.get(key);
        return entry != null ? ByteUtils.fromBytes(entry.value()) : null;
    }

    /**
     * Reads {@link ResetClusterMessage}; returns {@code null} if it's not saved.
     */
    public @Nullable ResetClusterMessage readResetClusterMessage() {
        return readFromVault(RESET_CLUSTER_MESSAGE_VAULT_KEY);
    }

    /**
     * Removes saved {@link ResetClusterMessage}.
     */
    public void removeResetClusterMessage() {
        vault.remove(RESET_CLUSTER_MESSAGE_VAULT_KEY);
    }
}
