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

import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.IndexRowImpl;
import org.apache.ignite.internal.storage.index.IndexStorage;

/**
 * An adapter that provides an index storage with a notion of the structure of a table row,
 * i.e. derives the index key from a given table row.
 */
public class TableSchemaAwareIndexStorage {
    private final UUID indexId;
    private final IndexStorage storage;
    private final Function<BinaryRow, BinaryTuple> indexRowResolver;

    /** Constructs the object. */
    public TableSchemaAwareIndexStorage(
            UUID indexId,
            IndexStorage storage,
            Function<BinaryRow, BinaryTuple> indexRowResolver
    ) {
        this.indexId = indexId;
        this.storage = storage;
        this.indexRowResolver = indexRowResolver;
    }

    /** Returns an identifier of the index. */
    public UUID id() {
        return indexId;
    }

    /**
     * Inserts the given table row to an index storage.
     *
     * @param tableRow A table row to insert.
     * @param rowId An identifier of a row in a main storage.
     */
    public void put(BinaryRow tableRow, RowId rowId) {
        BinaryTuple tuple = indexRowResolver.apply(tableRow);

        storage.put(new IndexRowImpl(tuple, rowId));
    }

    /**
     * Removes the given table row from an index storage.
     *
     * @param tableRow A table row to remove.
     * @param rowId An identifier of a row in a main storage.
     */
    public void remove(BinaryRow tableRow, RowId rowId) {
        BinaryTuple tuple = indexRowResolver.apply(tableRow);

        storage.remove(new IndexRowImpl(tuple, rowId));
    }
}
