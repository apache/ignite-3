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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;

/**
 * Catalog utils.
 */
public class CatalogUtils {
    /**
     * Default TIMESTAMP type precision: microseconds.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 36
     */
    public static final int DEFAULT_TIMESTAMP_PRECISION = 6;

    /**
     * Default TIME type precision: seconds.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 36
     */
    public static final int DEFAULT_TIME_PRECISION = 0;

    /**
     * Default DECIMAL precision is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 25
     */
    public static final int DEFAULT_DECIMAL_PRECISION = 19;

    /**
     * Default DECIMAL scale is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 25
     */
    public static final int DEFAULT_DECIMAL_SCALE = 3;

    /**
     * Maximum TIME and TIMESTAMP precision is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 38
     */
    public static final int MAX_TIME_PRECISION = 9;

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
     * @param id Table ID.
     * @param zoneId Distributed zone ID.
     * @param params Parameters.
     * @return Table descriptor.
     */
    public static CatalogTableDescriptor fromParams(int id, int zoneId, CreateTableParams params) {
        return new CatalogTableDescriptor(
                id,
                params.tableName(),
                zoneId,
                CatalogTableDescriptor.INITIAL_TABLE_VERSION,
                params.columns().stream().map(CatalogUtils::fromParams).collect(toList()),
                params.primaryKeyColumns(),
                params.colocationColumns()
        );
    }

    /**
     * Converts CreateIndex command params to hash index descriptor.
     *
     * @param id Index ID.
     * @param tableId Table ID.
     * @param params Parameters.
     * @return Index descriptor.
     */
    public static CatalogHashIndexDescriptor fromParams(int id, int tableId, CreateHashIndexParams params) {
        return new CatalogHashIndexDescriptor(id, params.indexName(), tableId, params.unique(), params.columns());
    }

    /**
     * Converts CreateIndex command params to sorted index descriptor.
     *
     * @param id Index ID.
     * @param tableId Table ID.
     * @param params Parameters.
     * @return Index descriptor.
     */
    public static CatalogSortedIndexDescriptor fromParams(int id, int tableId, CreateSortedIndexParams params) {
        List<CatalogColumnCollation> collations = params.collations();

        assert collations.size() == params.columns().size() : "tableId=" + tableId + ", indexId=" + id;

        List<CatalogIndexColumnDescriptor> columnDescriptors = IntStream.range(0, collations.size())
                .mapToObj(i -> new CatalogIndexColumnDescriptor(params.columns().get(i), collations.get(i)))
                .collect(toList());

        return new CatalogSortedIndexDescriptor(id, params.indexName(), tableId, params.unique(), columnDescriptors);
    }

    /**
     * Converts CreateZone command params to descriptor.
     *
     * @param id Distribution zone ID.
     * @param params Parameters.
     * @return Distribution zone descriptor.
     */
    public static CatalogZoneDescriptor fromParams(int id, CreateZoneParams params) {
        return new CatalogZoneDescriptor(
                id,
                params.zoneName(),
                params.partitions(),
                params.replicas(),
                params.dataNodesAutoAdjust(),
                params.dataNodesAutoAdjustScaleUp(),
                params.dataNodesAutoAdjustScaleDown(),
                params.filter()
        );
    }

    /**
     * Converts AlterTableAdd command columns parameters to column descriptor.
     *
     * @param params Parameters.
     * @return Column descriptor.
     */
    public static CatalogTableColumnDescriptor fromParams(ColumnParams params) {
        int precision = params.precision() != null ? params.precision() : defaultPrecision(params.type());
        int scale = params.scale() != null ? params.scale() : defaultScale(params.type());
        int length = params.length() != null ? params.length() : defaultLength(params.type());

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

    private static int defaultPrecision(ColumnType columnType) {
        //TODO IGNITE-19938: Add REAL,FLOAT and DOUBLE precision. See SQL`16 part 2 section 6.1 syntax rule 29-31
        switch (columnType) {
            case NUMBER:
            case DECIMAL:
                return DEFAULT_DECIMAL_PRECISION;
            case TIME:
                return DEFAULT_TIME_PRECISION;
            case TIMESTAMP:
            case DATETIME:
                return DEFAULT_TIMESTAMP_PRECISION;
            default:
                /*
                 * Precision shall be great than 0.
                 * SQL`16 part 2 section 6.1 syntax rule 21
                 */
                return 1;
        }
    }

    private static int defaultScale(ColumnType columnType) {
        //TODO IGNITE-19938: Add REAL,FLOAT and DOUBLE precision. See SQL`16 part 2 section 6.1 syntax rule 29-31
        if (columnType == ColumnType.DECIMAL) {
            return DEFAULT_DECIMAL_SCALE;
        }

        /*
         * Default scale is 0.
         * SQL`16 part 2 section 6.1 syntax rule 22
         */
        return 0;
    }

    private static int defaultLength(ColumnType columnType) {
        //TODO IGNITE-19938: Return length for other types. See SQL`16 part 2 section 6.1 syntax rule 39
        switch (columnType) {
            case BITMASK:
            case STRING:
            case BYTE_ARRAY:
                return Integer.MAX_VALUE;
            default:
                /*
                 * Default length is 1.
                 * SQL`16 part 2 section 6.1 syntax rule 5
                 */
                return 1;
        }
    }
}
