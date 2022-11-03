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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;

/**
 * Decorator class for {@link HashIndexStorage}.
 */
public class HashIndexStorageDecorator implements HashIndexStorage {
    private static final VarHandle DELEGATE;

    static {
        try {
            DELEGATE = MethodHandles.lookup().findVarHandle(HashIndexStorageDecorator.class, "delegate", HashIndexStorage.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile HashIndexStorage delegate;

    /**
     * Constructor.
     *
     * @param delegate Delegate.
     */
    public HashIndexStorageDecorator(HashIndexStorage delegate) {
        this.delegate = delegate;
    }

    /**
     * Replaces a delegate.
     *
     * @param newDelegate New delegate.
     * @return Previous delegate.
     */
    public HashIndexStorage replaceDelegate(HashIndexStorage newDelegate) {
        return (HashIndexStorage) DELEGATE.getAndSet(this, newDelegate);
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
        return delegate.get(key);
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        delegate.put(row);
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        delegate.remove(row);
    }
}
