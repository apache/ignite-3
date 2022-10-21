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

package org.apache.ignite.internal.table.distributed;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor.HashIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;
import org.apache.ignite.internal.util.Cursor;

/**
 * Facade to ease of work with pk constraint storage.
 *
 * <p>Encapsulates logic of conversion of ByteBuffer representing the table's key to storage key
 */
public class PkStorage {
    private static final BinaryTupleSchema PK_KEY_SCHEMA = BinaryTupleSchema.create(new Element[]{
            new Element(NativeTypes.BYTES, false)
    });

    /**
     * Creates a primary key constraint storage for given table.
     *
     * @param tableId An identifier of a table to create a constraint storage for.
     * @param storageFactory A storage factory to create a storage by.
     * @return A storage for pk constraint.
     */
    public static PkStorage createPkStorage(UUID tableId, Function<HashIndexDescriptor, IndexStorage> storageFactory) {
        IndexStorage storage = storageFactory.apply(new HashIndexDescriptor(tableId,
                List.of(new HashIndexColumnDescriptor("__rawKey", NativeTypes.BYTES, false))));

        return new PkStorage(storage);
    }

    private final IndexStorage storage;

    private PkStorage(IndexStorage storage) {
        this.storage = storage;
    }

    /**
     * Returns a cursor over {@code RowId}s associated with the given key.
     *
     * @throws StorageException If failed to read data.
     */
    public Cursor<RowId> get(ByteBuffer key) throws StorageException {
        return storage.get(toPkKeyTuple(key));
    }

    /**
     * Inserts the given row to the constraint's storage.
     *
     * @param rowId An identifier of a row in main storage.
     * @param key A buffer representing a primary key in a raw form.
     * @throws StorageException If failed to put data.
     */
    public void put(RowId rowId, ByteBuffer key) throws StorageException {
        storage.put(new IndexRowImpl(toPkKeyTuple(key), rowId));
    }

    /**
     * Removes the given row from the constraint's storage.
     *
     * @param rowId An identifier of a row in main storage.
     * @param key A buffer representing a primary key in a raw form.
     * @throws StorageException If failed to put data.
     */
    public void remove(RowId rowId, ByteBuffer key) throws StorageException {
        storage.remove(new IndexRowImpl(toPkKeyTuple(key), rowId));
    }

    /** Returns underlying storage. */
    public IndexStorage storage() {
        return storage;
    }

    private static BinaryTuple toPkKeyTuple(ByteBuffer bytes) {
        return new BinaryTuple(
                PK_KEY_SCHEMA,
                new BinaryTupleBuilder(1, false)
                        .appendElementBytes(bytes)
                        .build()
        );
    }
}
