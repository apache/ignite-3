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

package org.apache.ignite.internal.storage.impl;

import static org.mockito.Mockito.spy;

import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;

/**
 * Test implementation of the {@link StorageEngine} based on class {@link ConcurrentSkipListMap}.
 */
public class TestStorageEngine implements StorageEngine {
    /** Engine name. */
    public static final String ENGINE_NAME = "test";

    @Override
    public String name() {
        return ENGINE_NAME;
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
        return true;
    }

    @Override
    public TestMvTableStorage createMvTable(
            StorageTableDescriptor tableDescriptor,
            StorageIndexDescriptorSupplier indexDescriptorSupplier
    ) throws StorageException {
        return spy(new TestMvTableStorage(tableDescriptor));
    }

    @Override
    public void dropMvTable(int tableId) {
        // No-op.
    }
}
