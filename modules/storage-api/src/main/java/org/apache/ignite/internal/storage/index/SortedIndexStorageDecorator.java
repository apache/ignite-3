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
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.Nullable;

/**
 * Decorator class for {@link SortedIndexStorage}.
 */
public class SortedIndexStorageDecorator implements SortedIndexStorage {
    private static final VarHandle DELEGATE;

    static {
        try {
            DELEGATE = MethodHandles.lookup().findVarHandle(SortedIndexStorageDecorator.class, "delegate", SortedIndexStorage.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile SortedIndexStorage delegate;

    /**
     * Constructor.
     *
     * @param delegate Delegate.
     */
    public SortedIndexStorageDecorator(SortedIndexStorage delegate) {
        this.delegate = delegate;
    }

    /**
     * Replaces a delegate.
     *
     * @param newDelegate New delegate.
     * @return Previous delegate.
     */
    public SortedIndexStorage replaceDelegate(SortedIndexStorage newDelegate) {
        return (SortedIndexStorage) DELEGATE.getAndSet(this, newDelegate);
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

    @Override
    public SortedIndexDescriptor indexDescriptor() {
        return delegate.indexDescriptor();
    }

    @Override
    public Cursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return delegate.scan(lowerBound, upperBound, flags);
    }
}
