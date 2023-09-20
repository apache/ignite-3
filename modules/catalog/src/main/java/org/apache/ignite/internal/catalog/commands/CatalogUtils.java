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
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.TableNotFoundValidationException;
import org.apache.ignite.internal.catalog.descriptors.CatalogDataStorageDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
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

    /** Flag indicate no precision applicability. */
    public static final int PRECISION_NOT_APPLICABLE = Integer.MIN_VALUE;

    /** Flag indicate no scale applicability. */
    public static final int SCALE_NOT_APPLICABLE = Integer.MIN_VALUE;

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
     * @param params Column description.
     * @return Column descriptor.
     */
    public static CatalogTableColumnDescriptor fromParams(ColumnParams params) {
        int precision = params.precision() == null ? PRECISION_NOT_APPLICABLE : params.precision();
        int scale = params.scale() == null ? SCALE_NOT_APPLICABLE : params.scale();
        int length = Objects.requireNonNullElse(params.length(), defaultLength(params.type(), precision));

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

    /**
     * Returns a list of schemas, replacing any schema with {@code newSchema} if its id equal to {@code newSchema.id()}.
     *
     * @param newSchema A schema.
     * @param schemas A list of schemas.
     * @return A List of schemas.
     */
    public static List<CatalogSchemaDescriptor> replaceSchema(CatalogSchemaDescriptor newSchema,
            Collection<CatalogSchemaDescriptor> schemas) {

        return schemas.stream().map(s -> {
            if (Objects.equals(s.id(), newSchema.id())) {
                return newSchema;
            } else {
                return s;
            }
        }).collect(toList());
    }

    private static int defaultLength(ColumnType columnType, int precision) {
        //TODO IGNITE-20432: Return length for other types. See SQL`16 part 2 section 6.1 syntax rule 39
        switch (columnType) {
            case BITMASK:
            case STRING:
            case BYTE_ARRAY:
                return DEFAULT_VARLEN_LENGTH;
            default:
                return Math.max(DEFAULT_LENGTH, precision);
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

    /**
     * Returns schema with given name, or throws {@link CatalogValidationException} if schema with given name not exists.
     *
     * @param catalog Catalog to look up schema in.
     * @param name Name of the schema of interest.
     * @return Schema with given name. Never null.
     * @throws CatalogValidationException If schema with given name is not exists.
     */
    static CatalogSchemaDescriptor schemaOrThrow(Catalog catalog, String name) throws CatalogValidationException {
        name = Objects.requireNonNull(name, "schemaName");

        CatalogSchemaDescriptor schema = catalog.schema(name);

        if (schema == null) {
            throw new CatalogValidationException(format("Schema with name '{}' not found", name));
        }

        return schema;
    }

    /**
     * Returns table with given name, or throws {@link TableNotFoundValidationException} if table with given name not exists.
     *
     * @param schema Schema to look up table in.
     * @param name Name of the table of interest.
     * @return Table with given name. Never null.
     * @throws TableNotFoundValidationException If table with given name is not exists.
     */
    static CatalogTableDescriptor tableOrThrow(CatalogSchemaDescriptor schema, String name) throws TableNotFoundValidationException {
        name = Objects.requireNonNull(name, "tableName");

        CatalogTableDescriptor table = schema.table(name);

        if (table == null) {
            throw new TableNotFoundValidationException(format("Table with name '{}.{}' not found", schema.name(), name));
        }

        return table;
    }

    /**
     * Returns zone with given name, or throws {@link CatalogValidationException} if zone with given name not exists.
     *
     * @param catalog Catalog to look up zone in.
     * @param name Name of the zone of interest.
     * @return Zone with given name. Never null.
     * @throws CatalogValidationException If zone with given name is not exists.
     */
    static CatalogZoneDescriptor zoneOrThrow(Catalog catalog, String name) throws CatalogValidationException {
        name = Objects.requireNonNull(name, "zoneName");

        CatalogZoneDescriptor zone = catalog.zone(name);

        if (zone == null) {
            throw new CatalogValidationException(format("Distribution zone with name '{}' not found", name));
        }

        return zone;
    }
}
