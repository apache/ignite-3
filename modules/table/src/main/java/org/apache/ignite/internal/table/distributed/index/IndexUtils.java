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

package org.apache.ignite.internal.table.distributed.index;

import java.util.HashSet;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptor.StorageColumnDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.PartitionSet;

/** Auxiliary class for working with indexes that can contain useful methods and constants. */
public class IndexUtils {
    /**
     * Registers an index to a table.
     *
     * @param table Table into which the index will be registered.
     * @param tableDescriptor Index table descriptor.
     * @param indexDescriptor Descriptor for the index to be registered.
     * @param partitionSet Partitions for which index storages will need to be created if they are missing.
     * @param schemaRegistry Table schema register.
     */
    public static void registerIndexToTable(
            TableViewInternal table,
            CatalogTableDescriptor tableDescriptor,
            CatalogIndexDescriptor indexDescriptor,
            PartitionSet partitionSet,
            SchemaRegistry schemaRegistry
    ) {
        var storageIndexDescriptor = StorageIndexDescriptor.create(tableDescriptor, indexDescriptor);

        var tableRowConverter = new TableRowToIndexKeyConverter(
                schemaRegistry,
                storageIndexDescriptor.columns().stream().map(StorageColumnDescriptor::name).toArray(String[]::new)
        );

        if (storageIndexDescriptor instanceof StorageSortedIndexDescriptor) {
            table.registerSortedIndex(
                    (StorageSortedIndexDescriptor) storageIndexDescriptor,
                    tableRowConverter,
                    partitionSet
            );
        } else {
            table.registerHashIndex(
                    (StorageHashIndexDescriptor) storageIndexDescriptor,
                    indexDescriptor.unique(),
                    tableRowConverter,
                    partitionSet
            );
        }
    }

    /**
     * Registers all indexes that were created in the catalog for the table into the table for all partitions from {@code partitionSet}.
     *
     * <p>Under the hood, index storages and structures are created to work with them in other components.</p>
     *
     * <p>It is recommended to call the method when updating the catalog in parallel cannot occur.</p>
     *
     * <p>This method can be used both during node recovery and rebalancing to ensure that indexes are available before they are
     * accessed.</p>
     *
     * @param table Table into which the index will be registered.
     * @param catalogService Catalog service.
     * @param partitionSet Partitions for which index storages will need to be created if they are missing.
     * @param schemaRegistry Table schema register.
     */
    public static void registerIndexesToTable(
            TableViewInternal table,
            CatalogService catalogService,
            PartitionSet partitionSet,
            SchemaRegistry schemaRegistry
    ) {
        int earliestCatalogVersion = catalogService.earliestCatalogVersion();
        int latestCatalogVersion = catalogService.latestCatalogVersion();

        int tableId = table.tableId();

        var indexIds = new HashSet<Integer>();

        for (int catalogVersion = earliestCatalogVersion; catalogVersion <= latestCatalogVersion; catalogVersion++) {
            CatalogTableDescriptor tableDescriptor = catalogService.table(tableId, catalogVersion);

            if (tableDescriptor == null) {
                continue;
            }

            for (CatalogIndexDescriptor indexDescriptor : catalogService.indexes(catalogVersion, tableId)) {
                if (indexIds.add(indexDescriptor.id())) {
                    registerIndexToTable(table, tableDescriptor, indexDescriptor, partitionSet, schemaRegistry);
                }
            }
        }
    }
}
