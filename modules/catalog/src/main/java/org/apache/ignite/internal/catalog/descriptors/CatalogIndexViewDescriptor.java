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

package org.apache.ignite.internal.catalog.descriptors;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType.HASH;

import org.apache.ignite.internal.catalog.Catalog;

/** Descriptor for the indexes system view. */
public class CatalogIndexViewDescriptor {
    /** Index ID. */
    private final int id;

    /** Index name. */
    private final String name;

    /** index type (SORTED/HASH). */
    private final String type;

    /** ID of the table the index is created for. */
    private final int tableId;

    /** Name of the table the index is created for. */
    private final String tableName;

    /** ID of the schema where the index is created. */
    private final int schemaId;

    /** Name of the schema where the index is created. */
    private final String schemaName;

    /** Unique constraint flag. */
    private final boolean unique;

    /** Columns used in the index separated by comma. For sorted index format is "{column_name} {collation}". */
    private final String columnsString;

    /** Status of the index. */
    private final String status;

    /** Constructor. */
    private CatalogIndexViewDescriptor(
            int id,
            String name,
            String type,
            int tableId,
            String tableName,
            int schemaId,
            String schemaName,
            boolean unique,
            String columnsString,
            String status
    ) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.tableId = tableId;
        this.tableName = tableName;
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.unique = unique;
        this.columnsString = columnsString;
        this.status = status;
    }

    /** Creates a CatalogIndexViewDescriptor. */
    public static CatalogIndexViewDescriptor fromCatalogIndexDescriptor(CatalogIndexDescriptor index, Catalog catalog) {
        CatalogTableDescriptor table = catalog.table(index.tableId());

        return new CatalogIndexViewDescriptor(
                index.id(),
                index.name(),
                index.indexType().name(),
                index.tableId(),
                table.name(),
                table.schemaId(),
                catalog.schema(table.schemaId()).name(),
                index.unique(),
                getColumnsString(index),
                index.status().name()
        );
    }

    /** Returns ID of the index. */
    public int id() {
        return id;
    }

    /** Returns name of the index. */
    public String name() {
        return name;
    }

    /** Returns list of columns used in the index. For sorted index format is "{column_name} {collation}". */
    public String columnsString() {
        return columnsString;
    }

    /** Returns type of the index. */
    public String type() {
        return type;
    }

    /** Returns ID of the table. */
    public int tableId() {
        return tableId;
    }

    /** Returns unique constraint flag. */
    public boolean unique() {
        return unique;
    }

    /** Returns name of the table. */
    public String tableName() {
        return tableName;
    }

    /** Returns ID of the schema. */
    public int schemaId() {
        return schemaId;
    }

    /** Returns name of the schema. */
    public String schemaName() {
        return schemaName;
    }

    /** Returns status of the index. */
    public String status() {
        return status;
    }

    private static String getColumnsString(CatalogIndexDescriptor indexDescriptor) {
        return indexDescriptor.indexType() == HASH
                ? String.join(", ", ((CatalogHashIndexDescriptor) indexDescriptor).columns())
                : ((CatalogSortedIndexDescriptor) indexDescriptor)
                        .columns()
                        .stream()
                        .map(column -> column.name() + (column.collation().asc() ? " ASC" : " DESC"))
                        .collect(joining(", "));
    }
}
