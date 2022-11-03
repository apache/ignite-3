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

package org.apache.ignite.internal.storage.index;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;

/**
 * Decorator for {@link HashIndexStorage} on full rebalance.
 *
 * <p>Allows you to use methods to change data, throws {@link IllegalStateException} when reading data.
 */
public class HashIndexStorageOnRebalance implements HashIndexStorage {
    private static final String ERROR_MESSAGE = "Hash index storage is in full rebalancing, data reading is not available.";

    private final HashIndexStorage delegate;

    /**
     * Constructor.
     *
     * @param delegate Delegate.
     */
    public HashIndexStorageOnRebalance(HashIndexStorage delegate) {
        this.delegate = delegate;
    }

    @Override
    public HashIndexDescriptor indexDescriptor() {
        return delegate.indexDescriptor();
    }

    @Override
    public void destroy() throws StorageException {
        delegate.destroy();
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        throw creteDataWriteOnlyException();
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        delegate.put(row);
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        delegate.remove(row);
    }

    private IllegalStateException creteDataWriteOnlyException() {
        return new IllegalStateException(ERROR_MESSAGE);
    }
}
