/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Collection;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;

/**
 * Storage for a Hash Index.
 *
 * <p>This storage serves as an unordered mapping from a subset of a table's columns (a.k.a. index columns) to a set of {@link RowId}s
 * from a {@link org.apache.ignite.internal.storage.MvPartitionStorage} from the same table.
 *
 * @see org.apache.ignite.schema.definition.index.HashIndexDefinition
 */
public interface HashIndexStorage {
    /**
     * Returns the Index Descriptor of this storage.
     */
    HashIndexDescriptor indexDescriptor();

    /**
     * Returns a collection of {@code RowId}s that correspond to the given index key.
     */
    Collection<RowId> get(BinaryTuple key);

    /**
     * Adds the given index row to the index.
     */
    void put(IndexRow row);

    /**
     * Removes the given row from the index.
     *
     * <p>Removing a non-existent row is a no-op.
     */
    void remove(IndexRow row);
}
