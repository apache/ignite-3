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

package org.apache.ignite.internal.storage.engine;

import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.worker.ThreadAssertions;

/**
 * {@link StorageEngine} that wraps storages it creates to perform thread assertions read/write operations.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingStorageEngine implements StorageEngine {
    private final StorageEngine storageEngine;

    /** Constructor. */
    public ThreadAssertingStorageEngine(StorageEngine storageEngine) {
        this.storageEngine = storageEngine;
    }

    @Override
    public String name() {
        return storageEngine.name();
    }

    @Override
    public void start() throws StorageException {
        storageEngine.start();
    }

    @Override
    public void stop() throws StorageException {
        storageEngine.stop();
    }

    @Override
    public boolean isVolatile() {
        return storageEngine.isVolatile();
    }

    @Override
    public MvTableStorage createMvTable(StorageTableDescriptor tableDescriptor, StorageIndexDescriptorSupplier indexDescriptorSupplier) {
        MvTableStorage tableStorage = storageEngine.createMvTable(tableDescriptor, indexDescriptorSupplier);
        return new ThreadAssertingMvTableStorage(tableStorage);
    }

    @Override
    public void dropMvTable(int tableId) {
        storageEngine.dropMvTable(tableId);
    }
}
