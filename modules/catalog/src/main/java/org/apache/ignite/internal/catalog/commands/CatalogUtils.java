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
import static org.apache.ignite.internal.catalog.CatalogService.SYSTEM_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.Type.FUNCTION_CALL;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.DistributionZoneNotFoundValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.TableNotFoundValidationException;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
import org.apache.ignite.internal.catalog.commands.DefaultValue.Type;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog utils.
 */
public class CatalogUtils {
    /** Default number of distribution zone partitions. */
    public static final int DEFAULT_PARTITION_COUNT = 25;

    /** Default number of distribution zone replicas. */
    public static final int DEFAULT_REPLICA_COUNT = 5;

    /**
     * Default filter of distribution zone, which is a {@link com.jayway.jsonpath.JsonPath} expression for including all attributes of
     * nodes.
     */
    public static final String DEFAULT_FILTER = "$..*";

    /** Infinite value for the distribution zone timers. */
    public static final int INFINITE_TIMER_VALUE = Integer.MAX_VALUE;

    /** Value for the distribution zone timers which means that data nodes changing will be started without waiting. */
    public static final int IMMEDIATE_TIMER_VALUE = 0;

    /** Max number of distribution zone partitions. */
    public static final int MAX_PARTITION_COUNT = 65_000;

    /** Precision if not specified. */
    public static final int DEFAULT_PRECISION = 0;

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
    public static final int MAX_TIME_PRECISION = NativeTypes.MAX_TIME_PRECISION;

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

    /**
     * Functions that are allowed to be used as columns' functional default. The set contains uppercase function names.
     */
    private static final Map<String, ColumnType> FUNCTIONAL_DEFAULT_FUNCTIONS = new HashMap<>();

    static {
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT8, EnumSet.of(ColumnType.INT8, ColumnType.INT16, ColumnType.INT32,
                ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT16, EnumSet.of(ColumnType.INT16, ColumnType.INT32, ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT32, EnumSet.of(ColumnType.INT32, ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT64, EnumSet.of(ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.FLOAT, EnumSet.of(ColumnType.FLOAT, ColumnType.DOUBLE));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.DOUBLE, EnumSet.of(ColumnType.DOUBLE));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.STRING, EnumSet.of(ColumnType.STRING));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.BYTE_ARRAY, EnumSet.of(ColumnType.BYTE_ARRAY));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.DECIMAL, EnumSet.of(ColumnType.DECIMAL));

        FUNCTIONAL_DEFAULT_FUNCTIONS.put("RAND_UUID", ColumnType.UUID);
    }

    public static final List<String> SYSTEM_SCHEMAS = List.of(SYSTEM_SCHEMA_NAME);

    /** System schema names. */
    static boolean isSystemSchema(String schemaName) {
        return SYSTEM_SCHEMAS.contains(schemaName);
    }

    /**
     * Converts StorageProfileParams to descriptor.
     *
     * @param params Parameters.
     * @return Storage profiles descriptor.
     */
    public static CatalogStorageProfilesDescriptor fromParams(List<StorageProfileParams> params) {
        return new CatalogStorageProfilesDescriptor(
                params.stream().map(p -> new CatalogStorageProfileDescriptor(p.storageProfile())).collect(toList())
        );
    }

    /**
     * Converts AlterTableAdd command columns parameters to column descriptor.
     *
     * @param params Column description.
     * @return Column descriptor.
     */
    public static CatalogTableColumnDescriptor fromParams(ColumnParams params) {
        int precision = Objects.requireNonNullElse(params.precision(), DEFAULT_PRECISION);
        int scale = Objects.requireNonNullElse(params.scale(), DEFAULT_SCALE);
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
     * Validates a column change. If something is not valid, the supplied listener is invoked with information about the exact reason.
     *
     * @param origin Original column definition.
     * @param newType New type.
     * @param newPrecision New column precision.
     * @param newScale New column scale.
     * @param newLength New column length.
     * @param listener Listener to invoke on a validation failure.
     * @return {@code true} iff the proposed change is valid.
     */
    public static boolean validateColumnChange(
            CatalogTableColumnDescriptor origin,
            @Nullable ColumnType newType,
            @Nullable Integer newPrecision,
            @Nullable Integer newScale,
            @Nullable Integer newLength,
            TypeChangeValidationListener listener
    ) {
        if (newType != null) {
            if (isSupportedColumnTypeChange(origin.type(), newType)) {
                if (origin.type().precisionAllowed() && newPrecision != null && newPrecision < origin.precision()) {
                    listener.onFailure("Decreasing the precision for column of type '{}' is not allowed", origin.type(), newType);
                    return false;
                }

                if (origin.type().scaleAllowed() && newScale != null && newScale != origin.scale()) {
                    listener.onFailure("Changing the scale for column of type '{}' is not allowed", origin.type(), newType);
                    return false;
                }

                if (origin.type().lengthAllowed() && newLength != null && newLength < origin.length()) {
                    listener.onFailure("Decreasing the length for column of type '{}' is not allowed", origin.type(), newType);
                    return false;
                }

                return true;
            }

            if (newType != origin.type()) {
                listener.onFailure("Changing the type from {} to {} is not allowed", origin.type(), newType);
                return false;
            }
        }

        if (newPrecision != null && newPrecision != origin.precision()) {
            listener.onFailure("Changing the precision for column of type '{}' is not allowed", origin.type(), newType);
            return false;
        }

        if (newScale != null && newScale != origin.scale()) {
            listener.onFailure("Changing the scale for column of type '{}' is not allowed", origin.type(), newType);
            return false;
        }

        if (newLength != null && newLength != origin.length()) {
            listener.onFailure("Changing the length for column of type '{}' is not allowed", origin.type(), newType);
            return false;
        }

        return true;
    }

    /**
     * Returns whether the proposed column type change is supported.
     *
     * @param oldColumn Original column definition.
     * @param newColumn New column definition.
     * @return {@code true} iff the proposed change is supported.
     */
    public static boolean isColumnTypeChangeSupported(CatalogTableColumnDescriptor oldColumn, CatalogTableColumnDescriptor newColumn) {
        return validateColumnChange(
                oldColumn,
                newColumn.type(),
                newColumn.precision(),
                newColumn.scale(),
                newColumn.length(),
                TypeChangeValidationListener.NO_OP
        );
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

    /**
     * Replaces the table descriptor that has the same ID as the {@code newTableDescriptor} in the given {@code schema}.
     *
     * @param schema Schema, which table descriptor needs to be replaced.
     * @param newTableDescriptor Table descriptor which will replace the descriptor with the same ID in the schema.
     * @return New schema descriptor with a replaced table descriptor.
     * @throws CatalogValidationException If the table descriptor with the same ID is not present in the schema.
     */
    public static CatalogSchemaDescriptor replaceTable(CatalogSchemaDescriptor schema, CatalogTableDescriptor newTableDescriptor) {
        CatalogTableDescriptor[] tableDescriptors = schema.tables().clone();

        for (int i = 0; i < tableDescriptors.length; i++) {
            if (tableDescriptors[i].id() == newTableDescriptor.id()) {
                tableDescriptors[i] = newTableDescriptor;

                return new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        tableDescriptors,
                        schema.indexes(),
                        schema.systemViews(),
                        newTableDescriptor.updateToken()
                );
            }
        }

        throw new CatalogValidationException(String.format(
                "Table with ID %d has not been found in schema with ID %d", newTableDescriptor.id(), newTableDescriptor.schemaId()
        ));
    }

    /**
     * Replaces the index descriptor that has the same ID as the {@code newIndexDescriptor} in the given {@code schema}.
     *
     * @param schema Schema, which index descriptor needs to be replaced.
     * @param newIndexDescriptor Index descriptor which will replace the descriptor with the same ID in the schema.
     * @return New schema descriptor with a replaced index descriptor.
     * @throws CatalogValidationException If the index descriptor with the same ID is not present in the schema.
     */
    public static CatalogSchemaDescriptor replaceIndex(CatalogSchemaDescriptor schema, CatalogIndexDescriptor newIndexDescriptor) {
        CatalogIndexDescriptor[] indexDescriptors = schema.indexes().clone();

        for (int i = 0; i < indexDescriptors.length; i++) {
            if (indexDescriptors[i].id() == newIndexDescriptor.id()) {
                indexDescriptors[i] = newIndexDescriptor;

                return new CatalogSchemaDescriptor(
                        schema.id(),
                        schema.name(),
                        schema.tables(),
                        indexDescriptors,
                        schema.systemViews(),
                        newIndexDescriptor.updateToken()
                );
            }
        }

        throw new CatalogValidationException(String.format(
                "Index with ID %d has not been found in schema with ID %d", newIndexDescriptor.id(), schema.id()
        ));
    }

    /**
     * Return default length according to supplied type.
     *
     * @param columnType Column type.
     * @param precision Type precision.
     */
    public static int defaultLength(ColumnType columnType, int precision) {
        // TODO IGNITE-20432: Return length for other types. See SQL`16 part 2 section 6.1 syntax rule 39
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
     * Returns schema with given name, or throws {@link CatalogValidationException} if schema with given name not exists.
     *
     * @param catalog Catalog to look up schema in.
     * @param name Name of the schema of interest.
     * @return Schema with given name. Never null.
     * @throws CatalogValidationException If schema with given name is not exists.
     */
    public static CatalogSchemaDescriptor schemaOrThrow(Catalog catalog, String name) throws CatalogValidationException {
        name = Objects.requireNonNull(name, "schemaName");

        CatalogSchemaDescriptor schema = catalog.schema(name);

        if (schema == null) {
            throw new CatalogValidationException(format("Schema with name '{}' not found", name));
        }

        return schema;
    }

    /**
     * Returns a schema descriptor of the schema with the a given ID.
     *
     * @param catalog Catalog to look up the schema in.
     * @param schemaId Schema ID.
     * @throws CatalogValidationException If schema does not exist.
     */
    public static CatalogSchemaDescriptor schemaOrThrow(Catalog catalog, int schemaId) throws CatalogValidationException {
        CatalogSchemaDescriptor schema = catalog.schema(schemaId);

        if (schema == null) {
            throw new CatalogValidationException(format("Schema with ID '{}' not found", schemaId));
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
    public static CatalogTableDescriptor tableOrThrow(CatalogSchemaDescriptor schema, String name) throws TableNotFoundValidationException {
        name = Objects.requireNonNull(name, "tableName");

        CatalogTableDescriptor table = schema.table(name);

        if (table == null) {
            throw new TableNotFoundValidationException(format("Table with name '{}.{}' not found", schema.name(), name));
        }

        return table;
    }

    /**
     * Returns a table descriptor of the table with the a given ID.
     *
     * @param catalog Catalog to look up the table in.
     * @param tableId Table ID.
     * @throws TableNotFoundValidationException If table does not exist.
     */
    public static CatalogTableDescriptor tableOrThrow(Catalog catalog, int tableId) throws TableNotFoundValidationException {
        CatalogTableDescriptor table = catalog.table(tableId);

        if (table == null) {
            throw new TableNotFoundValidationException(format("Table with ID '{}' not found", tableId));
        }

        return table;
    }

    /**
     * Returns zone with given name, or throws {@link CatalogValidationException} if zone with given name not exists.
     *
     * @param catalog Catalog to look up zone in.
     * @param name Name of the zone of interest.
     * @return Zone with given name. Never null.
     * @throws DistributionZoneNotFoundValidationException If zone with given name is not exists.
     */
    public static CatalogZoneDescriptor zoneOrThrow(Catalog catalog, String name) throws DistributionZoneNotFoundValidationException {
        name = Objects.requireNonNull(name, "zoneName");

        CatalogZoneDescriptor zone = catalog.zone(name);

        if (zone == null) {
            throw new DistributionZoneNotFoundValidationException(format("Distribution zone with name '{}' not found", name));
        }

        return zone;
    }

    /**
     * Returns the primary key index name for table.
     *
     * @param tableName Table name.
     */
    public static String pkIndexName(String tableName) {
        return tableName + "_PK";
    }

    /**
     * Returns index descriptor.
     *
     * @param schema Schema to look up index in.
     * @param name Name of the index of interest.
     * @throws IndexNotFoundValidationException If index does not exist.
     */
    public static CatalogIndexDescriptor indexOrThrow(CatalogSchemaDescriptor schema, String name) throws IndexNotFoundValidationException {
        CatalogIndexDescriptor index = schema.aliveIndex(name);

        if (index == null) {
            throw new IndexNotFoundValidationException(format("Index with name '{}.{}' not found", schema.name(), name));
        }

        return index;
    }

    /**
     * Returns index descriptor.
     *
     * @param catalog Catalog to look up index in.
     * @param indexId ID of the index of interest.
     * @throws IndexNotFoundValidationException If index does not exist.
     */
    public static CatalogIndexDescriptor indexOrThrow(Catalog catalog, int indexId) throws IndexNotFoundValidationException {
        CatalogIndexDescriptor index = catalog.index(indexId);

        if (index == null) {
            throw new IndexNotFoundValidationException(format("Index with ID '{}' not found", indexId));
        }

        return index;
    }

    /**
     * Returns the timestamp at which the catalog version is guaranteed to be activated on every node of the cluster. This takes into
     * account possible clock skew between nodes.
     *
     * @param catalog Catalog version of interest.
     * @param maxClockSkewMillis Max clock skew in milliseconds.
     */
    public static HybridTimestamp clusterWideEnsuredActivationTimestamp(Catalog catalog, long maxClockSkewMillis) {
        HybridTimestamp activationTs = HybridTimestamp.hybridTimestamp(catalog.time());

        return activationTs.addPhysicalTime(maxClockSkewMillis)
                // Rounding up to the closest millisecond to account for possibility of HLC.now() having different
                // logical parts on different nodes of the cluster (see IGNITE-21084).
                .roundUpToPhysicalTick();
    }

    /**
     * Returns timestamp for which we'll wait after adding a new version to a Catalog.
     *
     * @param catalog Catalog version that has been added.
     * @param partitionIdleSafeTimePropagationPeriodMsSupplier Supplies partition idle safe time propagation period in millis.
     * @param maxClockSkewMillis Max clock skew in milliseconds.
     */
    public static HybridTimestamp clusterWideEnsuredActivationTsSafeForRoReads(
            Catalog catalog,
            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier,
            long maxClockSkewMillis
    ) {
        HybridTimestamp clusterWideEnsuredActivationTs = clusterWideEnsuredActivationTimestamp(catalog, maxClockSkewMillis);
        // TODO: this addition has to be removed when IGNITE-20378 is implemented.
        return clusterWideEnsuredActivationTs.addPhysicalTime(
                partitionIdleSafeTimePropagationPeriodMsSupplier.getAsLong() + maxClockSkewMillis
        );
    }

    /** Returns id of the default zone from given catalog, or {@code null} if default zone is not exist. */
    public static @Nullable Integer defaultZoneIdOpt(Catalog catalog) {
        CatalogZoneDescriptor defaultZone = catalog.defaultZone();

        return defaultZone != null ? defaultZone.id() : null;
    }

    /**
     * Check if provided default value is a constant or a functional default of supported function, or fail otherwise.
     */
    static void ensureSupportedDefault(String columnName, ColumnType columnType, @Nullable DefaultValue defaultValue) {
        if (defaultValue == null || defaultValue.type == Type.CONSTANT) {
            return;
        }

        if (defaultValue.type == FUNCTION_CALL) {
            String functionName = ((FunctionCall) defaultValue).functionName();
            ColumnType returnType = FUNCTIONAL_DEFAULT_FUNCTIONS.get(functionName.toUpperCase());

            if (returnType == columnType) {
                return;
            }

            if (returnType != null) {
                throw new CatalogValidationException(
                        format("Functional default type mismatch: [col={}, functionName={}, expectedType={}, actualType={}]",
                                columnName, functionName, returnType, columnType));
            }

            throw new CatalogValidationException(
                    format("Functional default contains unsupported function: [col={}, functionName={}]",
                            columnName, functionName));
        }

        throw new CatalogValidationException(
                format("Default of unsupported kind: [col={}, defaultType={}]", columnName, defaultValue.type));
    }

    /**
     * Check if provided default value is a constant, or fail otherwise.
     */
    static void ensureNonFunctionalDefault(String columnName, @Nullable DefaultValue defaultValue) {
        if (defaultValue == null || defaultValue.type == Type.CONSTANT) {
            return;
        }

        if (defaultValue.type == FUNCTION_CALL) {
            throw new CatalogValidationException(
                    format("Functional defaults are not supported for non-primary key columns [col={}].", columnName));
        }

        throw new CatalogValidationException(
                format("Default of unsupported kind: [col={}, defaultType={}]", columnName, defaultValue.type));
    }

    /**
     * Check if provided column type can be persisted, or fail otherwise.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-15200
    //  Remove this after interval type support is added.
    static void ensureTypeCanBeStored(String columnName, ColumnType columnType) {
        if (columnType == ColumnType.PERIOD || columnType == ColumnType.DURATION) {
            throw new CatalogValidationException(
                    format("Column of type '{}' cannot be persisted [col={}].", columnType, columnName));
        }
    }
}
