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

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.descriptors.CatalogColumnCollation;
import org.apache.ignite.internal.catalog.descriptors.CatalogDataStorageDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog utils.
 */
public class CatalogUtils {
    /** Default number of distribution zone partitions. */
    public static final int DEFAULT_PARTITION_COUNT = 25;

    /** Default number of distribution zone replicas. */
    public static final int DEFAULT_REPLICA_COUNT = 1;

    /**
     * Default filter of distribution zone, which is a {@link com.jayway.jsonpath.JsonPath} expression for including all attributes of
     * nodes.
     */
    public static final String DEFAULT_FILTER = "$..*";

    /** Default distribution zone storage engine. */
    // TODO: IGNITE-19719 Should be defined differently
    public static final String DEFAULT_STORAGE_ENGINE = "aipersist";

    /** Default distribution zone storage engine data region. */
    // TODO: IGNITE-19719 Must be storage engine specific
    public static final String DEFAULT_DATA_REGION = "default";

    /** Infinite value for the distribution zone timers. */
    public static final int INFINITE_TIMER_VALUE = Integer.MAX_VALUE;

    /** Value for the distribution zone timers which means that data nodes changing will be started without waiting. */
    public static final int IMMEDIATE_TIMER_VALUE = 0;

    /** Max number of distribution zone partitions. */
    public static final int MAX_PARTITION_COUNT = 65_000;

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
     * <p>SQL`16 part 2 section 6.1 syntax rule 20
     */
    public static final int DEFAULT_DECIMAL_PRECISION = 19;

    /**
     * Default scale is 0.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 22
     */
    public static final int DEFAULT_SCALE = 0;

    /**
     * Maximum TIME and TIMESTAMP precision is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 38
     */
    public static final int MAX_TIME_PRECISION = 9;

    /**
     * Max DECIMAL precision is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 25
     */
    public static final int MAX_DECIMAL_PRECISION = Short.MAX_VALUE;

    /**
     * Max DECIMAL scale is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 25
     */
    public static final int MAX_DECIMAL_SCALE = Short.MAX_VALUE;

    /**
     * Default length is `1` if implicit.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 5
     */
    public static final int DEFAULT_LENGTH = 1;

    /**
     * Max length for VARCHAR and VARBINARY is implementation defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 8
     */
    public static final int DEFAULT_VARLEN_LENGTH = 2 << 15;

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
     * <p>If the following fields are not set, the default value is used for them:</p>
     * <ul>
     *     <li>{@link CreateZoneParams#partitions()} -> {@link #DEFAULT_PARTITION_COUNT};</li>
     *     <li>{@link CreateZoneParams#replicas()} -> {@link #DEFAULT_REPLICA_COUNT};</li>
     *     <li>{@link CreateZoneParams#dataNodesAutoAdjust()} -> {@link #INFINITE_TIMER_VALUE};</li>
     *     <li>{@link CreateZoneParams#dataNodesAutoAdjustScaleUp()} -> {@link #IMMEDIATE_TIMER_VALUE} or {@link #INFINITE_TIMER_VALUE} if
     *     {@link CreateZoneParams#dataNodesAutoAdjust()} not {@code null};</li>
     *     <li>{@link CreateZoneParams#dataNodesAutoAdjustScaleDown()} -> {@link #INFINITE_TIMER_VALUE};</li>
     *     <li>{@link CreateZoneParams#filter()} -> {@link #DEFAULT_FILTER};</li>
     *     <li>{@link CreateZoneParams#dataStorage()} -> {@link #DEFAULT_STORAGE_ENGINE} with {@link #DEFAULT_DATA_REGION};</li>
     * </ul>
     *
     * @param id Distribution zone ID.
     * @param params Parameters.
     * @return Distribution zone descriptor.
     */
    public static CatalogZoneDescriptor fromParams(int id, CreateZoneParams params) {
        DataStorageParams dataStorageParams = params.dataStorage() != null
                ? params.dataStorage()
                : DataStorageParams.builder().engine(DEFAULT_STORAGE_ENGINE).dataRegion(DEFAULT_DATA_REGION).build();

        return new CatalogZoneDescriptor(
                id,
                params.zoneName(),
                Objects.requireNonNullElse(params.partitions(), DEFAULT_PARTITION_COUNT),
                Objects.requireNonNullElse(params.replicas(), DEFAULT_REPLICA_COUNT),
                Objects.requireNonNullElse(params.dataNodesAutoAdjust(), INFINITE_TIMER_VALUE),
                Objects.requireNonNullElse(
                        params.dataNodesAutoAdjustScaleUp(),
                        params.dataNodesAutoAdjust() != null ? INFINITE_TIMER_VALUE : IMMEDIATE_TIMER_VALUE
                ),
                Objects.requireNonNullElse(params.dataNodesAutoAdjustScaleDown(), INFINITE_TIMER_VALUE),
                Objects.requireNonNullElse(params.filter(), DEFAULT_FILTER),
                fromParams(dataStorageParams)
        );
    }

    /**
     * Converts DataStorageParams to descriptor.
     *
     * @param params Parameters.
     * @return Data storage descriptor.
     */
    // TODO: IGNITE-19719 Must be storage engine specific
    public static CatalogDataStorageDescriptor fromParams(DataStorageParams params) {
        return new CatalogDataStorageDescriptor(params.engine(), params.dataRegion());
    }

    /**
     * Converts AlterTableAdd command columns parameters to column descriptor.
     *
     * @param params Parameters.
     * @return Column descriptor.
     */
    public static CatalogTableColumnDescriptor fromParams(ColumnParams params) {
        int precision = Objects.requireNonNullElse(params.precision(), defaultPrecision(params.type()));
        int scale = Objects.requireNonNullElse(params.scale(), DEFAULT_SCALE);
        int length = Objects.requireNonNullElse(params.length(), defaultLength(params.type()));

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
                return 0;
        }
    }

    private static int defaultLength(ColumnType columnType) {
        //TODO IGNITE-19938: Return length for other types. See SQL`16 part 2 section 6.1 syntax rule 39
        switch (columnType) {
            case BITMASK:
            case STRING:
            case BYTE_ARRAY:
                return DEFAULT_VARLEN_LENGTH;
            default:
                return Math.max(DEFAULT_LENGTH, defaultPrecision(columnType));
        }
    }

    /**
     * Creates a new version of the distribution zone descriptor based on the AlterZone command params and the previous version. If a field
     * is not set in the command params, then the value from the previous version is taken.
     *
     * <p>Features of working with auto adjust params:</p>
     * <ul>
     *     <li>If {@link AlterZoneParams#dataNodesAutoAdjust()} is <b>not</b> {@code null}, then
     *     {@link CatalogZoneDescriptor#dataNodesAutoAdjustScaleUp()} and {@link CatalogZoneDescriptor#dataNodesAutoAdjustScaleDown()} will
     *     be {@link #INFINITE_TIMER_VALUE} in the new version;</li>
     *     <li>If {@link AlterZoneParams#dataNodesAutoAdjustScaleUp()} or {@link AlterZoneParams#dataNodesAutoAdjustScaleDown()} is
     *     <b>not</b> {@code null}, then {@link CatalogZoneDescriptor#dataNodesAutoAdjust()} will be {@link #INFINITE_TIMER_VALUE} in the
     *     new version.</li>
     * </ul>
     *
     * @param params Parameters.
     * @param previous Previous version of the distribution zone descriptor.
     * @return Distribution zone descriptor.
     */
    public static CatalogZoneDescriptor fromParamsAndPreviousValue(AlterZoneParams params, CatalogZoneDescriptor previous) {
        assert previous.name().equals(params.zoneName()) : "previousZoneName=" + previous.name() + ", paramsZoneName=" + params.zoneName();

        @Nullable Integer autoAdjust = null;
        @Nullable Integer scaleUp = null;
        @Nullable Integer scaleDown = null;

        if (params.dataNodesAutoAdjust() != null) {
            autoAdjust = params.dataNodesAutoAdjust();
            scaleUp = INFINITE_TIMER_VALUE;
            scaleDown = INFINITE_TIMER_VALUE;
        } else if (params.dataNodesAutoAdjustScaleUp() != null || params.dataNodesAutoAdjustScaleDown() != null) {
            autoAdjust = INFINITE_TIMER_VALUE;
            scaleUp = params.dataNodesAutoAdjustScaleUp();
            scaleDown = params.dataNodesAutoAdjustScaleDown();
        }

        CatalogDataStorageDescriptor dataStorageDescriptor = params.dataStorage() != null
                ? fromParams(params.dataStorage()) : previous.dataStorage();

        return new CatalogZoneDescriptor(
                previous.id(),
                previous.name(),
                Objects.requireNonNullElse(params.partitions(), previous.partitions()),
                Objects.requireNonNullElse(params.replicas(), previous.replicas()),
                Objects.requireNonNullElse(autoAdjust, previous.dataNodesAutoAdjust()),
                Objects.requireNonNullElse(scaleUp, previous.dataNodesAutoAdjustScaleUp()),
                Objects.requireNonNullElse(scaleDown, previous.dataNodesAutoAdjustScaleDown()),
                Objects.requireNonNullElse(params.filter(), previous.filter()),
                dataStorageDescriptor
        );
    }
}
