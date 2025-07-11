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

package org.apache.ignite.internal.table.distributed.storage;

import static java.util.Collections.emptySet;

import java.util.Set;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * {@link StorageEngine} implementation used when the node currently does not have the storage profile declared for the table
 * and/or the corresponding storage engine. Such an object is used as a placeholder to have ability to create a table object,
 * but any storage functionality will be absent (exceptions will be thrown).
 *
 * <p>Before a node starts getting assignments for partitions of such a table, both storage engine and storage profile must be created
 * on the node and this placeholder object must be replaced with a 'real' storage object.
 */
public class NullStorageEngine implements StorageEngine {
    @Override
    public String name() {
        return "null";
    }

    @Override
    public void start() throws StorageException {
        // No-op.
    }

    @Override
    public void stop() throws StorageException {
        // No-op.
    }

    @Override
    public boolean isVolatile() {
        return throwNoEngineException(null);
    }

    @Override
    public MvTableStorage createMvTable(StorageTableDescriptor tableDescriptor, StorageIndexDescriptorSupplier indexDescriptorSupplier) {
        return new NullMvTableStorage(tableDescriptor);
    }

    @Override
    public void destroyMvTable(int tableId) {
        throwNoEngineException(tableId);
    }

    private static <T> T throwNoEngineException(@Nullable Integer tableId) {
        throw new StorageException("Table uses an unknown storage profile or engine, so current node either should not receive "
                + "any assignments, or storage profile addition is not handled properly"
                + (tableId == null ? "" : " [tableId=" + tableId + "]"));
    }

    @Override
    public Set<Integer> tableIdsOnDisk() {
        return emptySet();
    }
}
