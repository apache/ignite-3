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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of Hash index storage using Page Memory.
 */
public class PageMemoryHashIndexStorage implements HashIndexStorage {
    private static final VarHandle CLOSED;

    static {
        try {
            CLOSED = MethodHandles.lookup().findVarHandle(PageMemoryHashIndexStorage.class, "closed", boolean.class);
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

    /** Busy lock for synchronous closing. */
    private final IgniteSpinBusyLock closeBusyLock = new IgniteSpinBusyLock();

    /** To avoid double closure. */
    @SuppressWarnings("unused")
    private volatile boolean closed;

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

        lowestRowId = RowId.lowestRowId(partitionId);

        highestRowId = RowId.highestRowId(partitionId);
    }

    @Override
    public HashIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            IndexColumns indexColumns = new IndexColumns(partitionId, key.byteBuffer());

            HashIndexRow lowerBound = new HashIndexRow(indexColumns, lowestRowId);
            HashIndexRow upperBound = new HashIndexRow(indexColumns, highestRowId);

            Cursor<HashIndexRow> cursor = hashIndexTree.find(lowerBound, upperBound);

            return new Cursor<>() {
                @Override
                public void close() {
                    cursor.close();
                }

                @Override
                public boolean hasNext() {
                    if (!closeBusyLock.enterBusy()) {
                        throwStorageClosedException();
                    }

                    try {
                        return cursor.hasNext();
                    } finally {
                        closeBusyLock.leaveBusy();
                    }
                }

                @Override
                public RowId next() {
                    if (!closeBusyLock.enterBusy()) {
                        throwStorageClosedException();
                    }

                    try {
                        return cursor.next().rowId();
                    } finally {
                        closeBusyLock.leaveBusy();
                    }
                }
            };
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to create scan cursor", e);
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

            HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

            var insert = new InsertHashIndexRowInvokeClosure(hashIndexRow, freeList, hashIndexTree.inlineSize());

            hashIndexTree.invoke(hashIndexRow, null, insert);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to put value into index", e);
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        if (!closeBusyLock.enterBusy()) {
            throwStorageClosedException();
        }

        try {
            IndexColumns indexColumns = new IndexColumns(partitionId, row.indexColumns().byteBuffer());

            HashIndexRow hashIndexRow = new HashIndexRow(indexColumns, row.rowId());

            var remove = new RemoveHashIndexRowInvokeClosure(hashIndexRow, freeList);

            hashIndexTree.invoke(hashIndexRow, null, remove);

            // Performs actual deletion from freeList if necessary.
            remove.afterCompletion();
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Failed to remove value from index", e);
        } finally {
            closeBusyLock.leaveBusy();
        }
    }

    @Override
    public void destroy() throws StorageException {
        // TODO: IGNITE-17626 Remove it
        throw new UnsupportedOperationException();
    }

    /**
     * Closes the hash index storage.
     */
    public void close() {
        if (!CLOSED.compareAndSet(this, false, true)) {
            return;
        }

        closeBusyLock.block();

        hashIndexTree.close();
    }

    /**
     * Throws an exception that the storage is already closed.
     */
    private void throwStorageClosedException() {
        throw new StorageClosedException();
    }
}
