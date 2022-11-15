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

package org.apache.ignite.internal.storage.pagememory.index.hash;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageClosedException;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumnsFreeList;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of Hash index storage using Page Memory.
 */
public class PageMemoryHashIndexStorage implements HashIndexStorage {
    private static final VarHandle STARTED;

    static {
        try {
            STARTED = MethodHandles.lookup().findVarHandle(PageMemoryHashIndexStorage.class, "started", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /** Index descriptor. */
    private final HashIndexDescriptor descriptor;

    /** Free list to store index columns. */
    private final IndexColumnsFreeList freeList;

    /** Hash index tree instance. */
    private final HashIndexTree hashIndexTree;

    /** Partition id. */
    private final int partitionId;

    /** Lowest possible RowId according to signed long ordering. */
    private final RowId lowestRowId;

    /** Highest possible RowId according to signed long ordering. */
    private final RowId highestRowId;

    private volatile boolean started = true;

    /**
     * Constructor.
     *
     * @param descriptor Hash index descriptor.
     * @param freeList Free list to store index columns.
     * @param hashIndexTree Hash index tree instance.
     */
    public PageMemoryHashIndexStorage(HashIndexDescriptor descriptor, IndexColumnsFreeList freeList, HashIndexTree hashIndexTree) {
        this.descriptor = descriptor;
        this.freeList = freeList;
        this.hashIndexTree = hashIndexTree;

        partitionId = hashIndexTree.partitionId();

        lowestRowId = new RowId(partitionId, Long.MIN_VALUE, Long.MIN_VALUE);

        highestRowId = new RowId(partitionId, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Override
    public HashIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        checkClosed();

        IndexColumns indexColumns = new IndexColumns(partitionId, key.byteBuffer());

        HashIndexRow lowerBound = new HashIndexRow(indexColumns, lowestRowId);
        HashIndexRow upperBound = new HashIndexRow(indexColumns, highestRowId);

        Cursor<HashIndexRow> cursor;

        try {
            cursor = hashIndexTree.find(lowerBound, upperBound);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        }

        return new Cursor<>() {
            @Override
            public void close() throws Exception {
                cursor.close();
            }

            @Override
            public boolean hasNext() {
                checkClosed();

                return cursor.hasNext();
            }

            @Override
            public RowId next() {
                checkClosed();

                return cursor.next().rowId();
            }
        };
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        checkClosed();

        IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

        try {
            HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

            var insert = new InsertHashIndexRowInvokeClosure(hashIndexRow, freeList, hashIndexTree.inlineSize());

            hashIndexTree.invoke(hashIndexRow, null, insert);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to put value into index", e);
        }
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        checkClosed();

        IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

        try {
            HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

            var remove = new RemoveHashIndexRowInvokeClosure(hashIndexRow, freeList);

            hashIndexTree.invoke(hashIndexRow, null, remove);

            // Performs actual deletion from freeList if necessary.
            remove.afterCompletion();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to remove value from index", e);
        }
    }

    @Override
    public void destroy() throws StorageException {
        close0(true);
    }

    /**
     * Closes the hash index storage.
     */
    public void close() {
        close0(false);
    }

    /**
     * Closes the hash index storage.
     *
     * @param destroy Whether to destroy data in storage.
     */
    private void close0(boolean destroy) {
        if (!STARTED.compareAndSet(this, true, false)) {
            return;
        }

        hashIndexTree.close();

        if (destroy) {
            //TODO IGNITE-17626 Implement.
        }
    }

    private void checkClosed() {
        if (!started) {
            throw new StorageClosedException("Storage is already closed");
        }
    }
}
