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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;

/** Internal table view interface. */
public interface TableViewInternal extends Table {
    /**
     * Gets a table id.
     *
     * @return Table id as UUID.
     */
    int tableId();

    /** Returns an ID of a primary index, {@code -1} if not set. */
    int pkId();

    /**
     * Gets an internal table instance this view represents.
     *
     * @return Internal table instance.
     */
    InternalTable internalTable();

    /**
     * Gets a schema view for the table.
     *
     * @return Schema view.
     */
    SchemaRegistry schemaView();

    /**
     * Sets a schema view for the table.
     *
     * @param schemaReg Schema view.
     */
    void schemaView(SchemaRegistry schemaReg);

    /**
     * Returns a partition for a key tuple.
     *
     * @param key The tuple.
     * @return The partition.
     */
    int partition(Tuple key);

    /**
     * Returns a partition for a key.
     *
     * @param key The key.
     * @param keyMapper Key mapper
     * @return The partition.
     */
    <K> int partition(K key, Mapper<K> keyMapper);

    /**
     * Returns cluster node that is the leader of the corresponding partition group or throws an exception if
     * it cannot be found.
     *
     * @param partition Partition number.
     * @return Leader node of the partition group corresponding to the partition.
     */
    ClusterNode leaderAssignment(int partition);

    /**
     * Registers the index with given id in a table.
     *
     * @param indexDescriptor Index descriptor.
     * @param unique A flag indicating whether the given index unique or not.
     * @param searchRowResolver Function which converts given table row to an index key.
     */
    void registerHashIndex(
            StorageHashIndexDescriptor indexDescriptor,
            boolean unique,
            ColumnsExtractor searchRowResolver,
            PartitionSet partitions
    );

    /**
     * Registers the index with given id in a table.
     *
     * @param indexDescriptor Index descriptor.
     * @param searchRowResolver Function which converts given table row to an index key.
     */
    void registerSortedIndex(
            StorageSortedIndexDescriptor indexDescriptor,
            ColumnsExtractor searchRowResolver,
            PartitionSet partitions
    );

    /**
     * Unregisters given index from table.
     *
     * @param indexId An index id to unregister.
     */
    void unregisterIndex(int indexId);
}
