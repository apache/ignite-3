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

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.sql.ColumnType;

/**
 * Catalog utils.
 */
public class CatalogUtils {
    private static final Map<ColumnType, Set<ColumnType>> ALTER_COLUMN_TYPE_TRANSITIONS = new EnumMap<>(ColumnType.class);

    static {
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT8, EnumSet.of(ColumnType.INT16, ColumnType.INT32, ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT16, EnumSet.of(ColumnType.INT32, ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT32, EnumSet.of(ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.FLOAT, EnumSet.of(ColumnType.DOUBLE));
    }

    /**
     * Converts CreateTable command params to descriptor.
     *
     * @param id Table id.
     * @param params Parameters.
     * @return Table descriptor.
     */
    public static CatalogTableDescriptor fromParams(int id, CreateTableParams params) {
        return new CatalogTableDescriptor(id,
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
    public static CatalogIndexDescriptor fromParams(int id, int tableId, CreateHashIndexParams params) {
        return new CatalogHashIndexDescriptor(id,
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
    public static CatalogIndexDescriptor fromParams(int id, int tableId, CreateSortedIndexParams params) {
        List<CatalogColumnCollation> collations = params.collations();

        assert collations.size() == params.columns().size();

        List<CatalogIndexColumnDescriptor> columnDescriptors = IntStream.range(0, collations.size())
                .mapToObj(i -> new CatalogIndexColumnDescriptor(params.columns().get(i), collations.get(i)))
                .collect(Collectors.toList());

        return new CatalogSortedIndexDescriptor(id, params.indexName(), tableId, params.isUnique(), columnDescriptors);
    }

    /**
     * Converts AlterTableAdd command columns parameters to column descriptor.
     *
     * @param params Parameters.
     * @return Column descriptor.
     */
    public static CatalogTableColumnDescriptor fromParams(ColumnParams params) {
        int precision = params.precision() != null ? params.precision() : 0;
        int scale = params.scale() != null ? params.scale() : 0;
        int length = params.length() != null ? params.length() : 0;
        DefaultValue defaultValue = params.defaultValueDefinition();

        return new CatalogTableColumnDescriptor(params.name(), params.type(), params.nullable(),
                precision, scale, length, defaultValue);
    }

    /**
     * Checks if the specified column type transition is supported.
     *
     * @param source Source column type.
     * @param target Target column type.
     * @return {@code True} if the specified type transition is supported, {@code false} otherwise.
     */
    public static boolean isSupportedColumnTypeChange(ColumnType source, ColumnType target) {
        Set<ColumnType> supportedTransitions = ALTER_COLUMN_TYPE_TRANSITIONS.get(source);

        return supportedTransitions != null && supportedTransitions.contains(target);
    }
}
