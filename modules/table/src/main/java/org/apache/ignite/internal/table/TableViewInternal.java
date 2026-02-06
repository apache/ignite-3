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

import java.util.Map;
import java.util.function.Supplier;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableStatsStalenessConfiguration;
import org.apache.ignite.internal.table.metrics.ReadWriteMetricSource;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/** Internal table view interface. */
public interface TableViewInternal extends Table {
    /**
     * Gets a table id.
     *
     * @return Table id as UUID.
     */
    int tableId();

    /**
     * Returns a zone id.
     *
     * @return Zone id.
     */
    int zoneId();

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
     * Returns a partition ID for a key tuple.
     *
     * @param key The tuple.
     * @return The partition ID.
     */
    int partitionId(Tuple key);

    /**
     * Returns a partition ID for a key.
     *
     * @param key The key.
     * @param keyMapper Key mapper
     * @return The partition ID.
     */
    <K> int partitionId(K key, Mapper<K> keyMapper);

    /** Returns a supplier of index storage wrapper factories for given partition. */
    TableIndexStoragesSupplier indexStorageAdapters(int partitionId);

    /** Returns a supplier of index locker factories for given partition. */
    Supplier<Map<Integer, IndexLocker>> indexesLockers(int partId);

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
     * @param unique A flag indicating whether the given index unique or not.
     * @param searchRowResolver Function which converts given table row to an index key.
     */
    void registerSortedIndex(
            StorageSortedIndexDescriptor indexDescriptor,
            boolean unique,
            ColumnsExtractor searchRowResolver,
            PartitionSet partitions
    );

    /**
     * Unregisters given index from table.
     *
     * @param indexId An index id to unregister.
     */
    void unregisterIndex(int indexId);

    /**
     * Returns a metric source for this table.
     *
     * @return Table metrics source.
     */
    ReadWriteMetricSource metrics();

    /**
     * Updates staleness configuration with provided parameters.
     *
     * <p>If parameter is {@code null}, then value from current configuration is used instead.
     *
     * @param staleRowsFraction A fraction of a partition to be modified before the data is considered to be "stale". Should be in
     *         range [0, 1].
     * @param minStaleRowsCount Minimal number of rows in partition to be modified before the data is considered to be "stale".
     *         Should be non-negative.
     */
    void updateStalenessConfiguration(
            @Nullable Double staleRowsFraction,
            @Nullable Long minStaleRowsCount
    );

    /** Returns current staleness configuration. */
    TableStatsStalenessConfiguration stalenessConfiguration();
}
