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

package org.apache.ignite.runner.internal.storage;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.ConfigurationStorageListener;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.internal.vault.VaultManager;

public class LocalConfigurationStorage implements ConfigurationStorage {
    private final VaultManager vaultMgr;

    public LocalConfigurationStorage(VaultManager vaultMgr) {
        this.vaultMgr = vaultMgr;
    }

    @Override public Data readAll() throws StorageException {
        return null;
    }

    @Override public CompletableFuture<Boolean> write(Map<String, Serializable> newValues, long version) {
        return null;
    }

    @Override public void addListener(ConfigurationStorageListener listener) {

    }

    @Override public void removeListener(ConfigurationStorageListener listener) {

    }

    @Override public void notifyApplied(long storageRevision) {

    }
}
