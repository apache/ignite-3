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

package org.apache.ignite.internal.catalog.commands;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.descriptors.ColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.HashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;

/**
 * Catalog utils.
 */
public class CatalogUtils {
    /**
     * Converts CreateTable command params to descriptor.
     *
     * @param id Table id.
     * @param params Parameters.
     * @return Table descriptor.
     */
    public static TableDescriptor fromParams(int id, CreateTableParams params) {
        return new TableDescriptor(id,
                params.tableName(),
                params.columns().stream().map(CatalogUtils::fromParams).collect(Collectors.toList()),
                params.primaryKeyColumns(),
                params.colocationColumns()
        );
    }

    /**
     * Converts CreateIndex command params to hash index descriptor.
     *
     * @param id Index id.
     * @param tableId Table id.
     * @param params Parameters.
     * @return Index descriptor.
     */
    public static IndexDescriptor fromParams(int id, int tableId, CreateHashIndexParams params) {
        return new HashIndexDescriptor(id,
                params.indexName(),
                tableId,
                false,
                params.columns()
        );
    }

    /**
     * Converts CreateIndex command params to sorted index descriptor.
     *
     * @param id Index id.
     * @param tableId Table id.
     * @param params Parameters.
     * @return Index descriptor.
     */
    public static IndexDescriptor fromParams(int id, int tableId, CreateSortedIndexParams params) {
        List<ColumnCollation> collations = params.collations();

        assert collations.size() == params.columns().size();

        List<IndexColumnDescriptor> columnDescriptors = IntStream.range(0, collations.size())
                .mapToObj(i -> new IndexColumnDescriptor(params.columns().get(i), collations.get(i)))
                .collect(Collectors.toList());

        return new SortedIndexDescriptor(id, params.indexName(), tableId, params.isUnique(), columnDescriptors);
    }

    /**
     * Converts AlterTableAdd command columns parameters to column descriptor.
     *
     * @param params Parameters.
     * @return Column descriptor.
     */
    public static TableColumnDescriptor fromParams(ColumnParams params) {
        return new TableColumnDescriptor(params.name(), params.type(), params.nullable(), params.defaultValueDefinition(),
                params.precision(), params.scale(), params.length());
    }
}
