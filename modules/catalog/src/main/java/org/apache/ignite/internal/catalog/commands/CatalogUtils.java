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

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.CatalogService.DEFINITION_SCHEMA;
import static org.apache.ignite.internal.catalog.CatalogService.INFORMATION_SCHEMA;
import static org.apache.ignite.internal.catalog.CatalogService.SYSTEM_SCHEMA_NAME;
import static org.apache.ignite.internal.catalog.commands.DefaultValue.Type.FUNCTION_CALL;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.STRONG_CONSISTENCY;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import it.unimi.dsi.fastutil.ints.IntList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogValidationException;
import org.apache.ignite.internal.catalog.IndexNotFoundValidationException;
import org.apache.ignite.internal.catalog.commands.DefaultValue.FunctionCall;
import org.apache.ignite.internal.catalog.commands.DefaultValue.Type;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfilesDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.SetDefaultZoneEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog utils.
 */
public class CatalogUtils {
    /** Default zone name. */
    public static final String DEFAULT_ZONE_NAME = "Default";

    /** Default number of distribution zone partitions. */
    public static final int DEFAULT_PARTITION_COUNT = 25;

    /** Default number of distribution zone replicas. */
    public static final int DEFAULT_REPLICA_COUNT = 1;

    /**
     * Quorum size for the default zone. Default quorum size for other zones is calculated using replicas count.
     */
    public static final int DEFAULT_ZONE_QUORUM_SIZE = 1;

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
     * Minimum precision for TIME and TIMESTAMP types.
     */
    public static final int MIN_TIME_PRECISION = 0;

    /**
     * Maximum TIME and TIMESTAMP precision is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 38
     */
    public static final int MAX_TIME_PRECISION = NativeTypes.MAX_TIME_PRECISION;

    /**
     * Unspecified precision.
     */
    public static final int UNSPECIFIED_PRECISION = -1;

    /**
     * Minimum DECIMAL precision.
     */
    public static final int MIN_DECIMAL_PRECISION = 1;

    /**
     * Max DECIMAL precision is implementation-defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 25
     */
    public static final int MAX_DECIMAL_PRECISION = Short.MAX_VALUE;

    /**
     * Minimum DECIMAL scale.
     */
    public static final int MIN_DECIMAL_SCALE = 0;

    /**
     * Unspecified scale.
     */
    public static final int UNSPECIFIED_SCALE = -1;

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
     * Default length for VARCHAR and VARBINARY is implementation defined.
     *
     * <p>SQL`16 part 2 section 6.1 syntax rule 8
     */
    public static final int DEFAULT_VARLEN_LENGTH = 2 << 15;

    /**
     * Maximum length for VARCHAR and VARBINARY types.
     */
    public static final int MAX_VARLEN_LENGTH = Integer.MAX_VALUE;

    /**
     * Minimum length for VARCHAR and VARBINARY types.
     */
    public static final int MIN_VARLEN_PRECISION = 1;

    /**
     * Unspecified length.
     */
    public static final int UNSPECIFIED_LENGTH = -1;

    /**
     * Minimum precision for interval types.
     */
    public static final int MIN_INTERVAL_TYPE_PRECISION = 1;

    /**
     * Maximum precision for interval types.
     */
    public static final int MAX_INTERVAL_TYPE_PRECISION = 10;

    public static final ConsistencyMode DEFAULT_CONSISTENCY_MODE = STRONG_CONSISTENCY;

    public static final long DEFAULT_MIN_STALE_ROWS_COUNT = 500L;
    public static final double DEFAULT_STALE_ROWS_FRACTION = 0.2d;

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

    public static final Set<String> SYSTEM_SCHEMAS = Set.of(
            SYSTEM_SCHEMA_NAME,
            DEFINITION_SCHEMA,
            INFORMATION_SCHEMA
    );

    /** System schema names. */
    public static boolean isSystemSchema(String schemaName) {
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
                    listener.onFailure("Decreasing the precision for column of type '{}' is not allowed.", origin.type(), newType);
                    return false;
                }

                if (origin.type().scaleAllowed() && newScale != null && newScale != origin.scale()) {
                    listener.onFailure("Changing the scale for column of type '{}' is not allowed.", origin.type(), newType);
                    return false;
                }

                if (origin.type().lengthAllowed() && newLength != null && newLength < origin.length()) {
                    listener.onFailure("Decreasing the length for column of type '{}' is not allowed.", origin.type(), newType);
                    return false;
                }

                return true;
            }

            if (newType != origin.type()) {
                listener.onFailure("Changing the type from {} to {} is not allowed.", origin.type(), newType);
                return false;
            }
        }

        if (newPrecision != null && newPrecision != origin.precision()) {
            listener.onFailure("Changing the precision for column of type '{}' is not allowed.", origin.type(), newType);
            return false;
        }

        if (newScale != null && newScale != origin.scale()) {
            listener.onFailure("Changing the scale for column of type '{}' is not allowed.", origin.type(), newType);
            return false;
        }

        if (newLength != null && newLength != origin.length()) {
            listener.onFailure("Changing the length for column of type '{}' is not allowed.", origin.type(), newType);
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
                        newTableDescriptor.updateTimestamp()
                );
            }
        }

        throw new CatalogValidationException("Table with ID {} has not been found in schema with ID {}.",
                newTableDescriptor.id(), newTableDescriptor.schemaId());
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
                        newIndexDescriptor.updateTimestamp()
                );
            }
        }

        throw new CatalogValidationException("Index with ID {} has not been found in schema with ID {}.",
                newIndexDescriptor.id(), schema.id());
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
        CatalogSchemaDescriptor schemaDescriptor = schema(catalog, name, true);

        assert schemaDescriptor != null;

        return schemaDescriptor;
    }

    /**
     * Returns a schema descriptor of the schema with the a given ID.
     *
     * @param catalog Catalog to look up the schema in.
     * @param schemaId Schema ID.
     * @throws CatalogValidationException If schema with given name is not exists.
     */
    public static CatalogSchemaDescriptor schemaOrThrow(Catalog catalog, int schemaId) throws CatalogValidationException {
        CatalogSchemaDescriptor schema = catalog.schema(schemaId);

        if (schema == null) {
            throw new CatalogValidationException("Schema with ID '{}' not found.", schemaId);
        }

        return schema;
    }

    /**
     * Returns schema with given name, or throws {@link CatalogValidationException} if schema with given name not exists.
     *
     * @param catalog Catalog to look up schema in.
     * @param name Name of the schema of interest.
     * @param shouldThrowIfNotExists Flag indicated should be thrown the {@code CatalogValidationException} for absent schema or just
     *         return {@code null}.
     * @return Schema with given name. Never null.
     * @throws CatalogValidationException If schema with given name is not exists and flag shouldThrowIfNotExists set to {@code true}.
     */
    public static @Nullable CatalogSchemaDescriptor schema(Catalog catalog, String name, boolean shouldThrowIfNotExists)
            throws CatalogValidationException {
        name = Objects.requireNonNull(name, "schemaName");

        CatalogSchemaDescriptor schema = catalog.schema(name);

        if (schema == null && shouldThrowIfNotExists) {
            throw new CatalogValidationException(format("Schema with name '{}' not found.", name));
        }

        return schema;
    }

    /**
     * Returns table with given name.
     *
     * @param schema Schema to look up table in.
     * @param name Name of the table of interest.
     * @param shouldThrowIfNotExists Flag indicated should be thrown the {@code CatalogValidationException} for absent table or just
     *         return {@code null}.
     * @return Table descriptor for given name or @{code null} in case table is absent and flag shouldThrowIfNotExists set to {@code false}.
     * @throws CatalogValidationException If table with given name is not exists and flag shouldThrowIfNotExists set to {@code true}.
     */
    public static @Nullable CatalogTableDescriptor table(CatalogSchemaDescriptor schema, String name, boolean shouldThrowIfNotExists)
            throws CatalogValidationException {
        name = Objects.requireNonNull(name, "tableName");

        CatalogTableDescriptor table = schema.table(name);

        if (table == null && shouldThrowIfNotExists) {
            throw new CatalogValidationException("Table with name '{}.{}' not found.", schema.name(), name);
        }

        return table;
    }

    /**
     * Returns a table descriptor of the table with the a given ID.
     *
     * @param catalog Catalog to look up the table in.
     * @param tableId Table ID.
     * @throws CatalogValidationException If table does not exist.
     */
    public static CatalogTableDescriptor tableOrThrow(Catalog catalog, int tableId) throws CatalogValidationException {
        CatalogTableDescriptor table = catalog.table(tableId);

        if (table == null) {
            throw new CatalogValidationException("Table with ID '{}' not found.", tableId);
        }

        return table;
    }

    /**
     * Returns a descriptor for given zone name or for a default zone from the given catalog.
     *
     * @param catalog Catalog to check zones' existence.
     * @param zoneName Zone name to try to find catalog descriptor for. If {@code null} then will return default zone descriptor.
     * @return Returns a descriptor for given zone name if it isn't {@code null} or for existed default zone otherwise.
     * @throws CatalogValidationException In casse if given zone name isn't {@code null}, but the given catalog hasn't zone with such name.
     * @implNote This method assumes, that {@link #shouldCreateNewDefaultZone} returns {@code false} with the same input.
     */
    public static CatalogZoneDescriptor zoneByNameOrDefaultOrThrow(
            Catalog catalog,
            @Nullable String zoneName
    ) throws CatalogValidationException {
        if (zoneName != null) {
            return zoneOrThrow(catalog, zoneName);
        }

        CatalogZoneDescriptor defaultZone = catalog.defaultZone();

        // TODO: Remove after https://issues.apache.org/jira/browse/IGNITE-26798
        if (defaultZone == null) {
            throw new CatalogValidationException("Default zone not found.");
        }

        return defaultZone;
    }

    /**
     * Returns {@code true} if the given zone name is {@code null} and in the given catalog there is no default zone. If so, then looks like
     * {@link #createDefaultZoneDescriptor} should be called then.
     *
     * @param catalog Catalog to check default zone presence.
     * @param zoneName Zone name to check if it is {@code null}.
     * @return Returns {@code true} if the given zone name is {@code null} and in the given catalog there is no default zone.
     */
    public static boolean shouldCreateNewDefaultZone(Catalog catalog, @Nullable String zoneName) {
        return zoneName == null && catalog.defaultZone() == null;
    }

    /**
     * Creates catalog descriptor for a new default zone.
     *
     * @param catalog Catalog to check that {@link #DEFAULT_ZONE_NAME} isn't used.
     * @param newDefaultZoneId Identifier for new default zone.
     * @return New default zone's catalog descriptor.
     *
     * @throws CatalogValidationException If a zone with {@link #DEFAULT_ZONE_NAME} already exists.
     */
    public static CatalogZoneDescriptor createDefaultZoneDescriptor(
            Catalog catalog,
            int newDefaultZoneId,
            Collection<UpdateEntry> updateEntries
    ) throws CatalogValidationException {
        // TODO: Remove after https://issues.apache.org/jira/browse/IGNITE-26798
        checkDuplicateDefaultZoneName(catalog);

        CatalogZoneDescriptor defaultZone = createDefaultZoneDescriptor(newDefaultZoneId);

        updateEntries.add(new NewZoneEntry(defaultZone));
        updateEntries.add(new SetDefaultZoneEntry(defaultZone.id()));

        return defaultZone;
    }

    private static CatalogZoneDescriptor createDefaultZoneDescriptor(int newDefaultZoneId) {
        return new CatalogZoneDescriptor(
                newDefaultZoneId,
                DEFAULT_ZONE_NAME,
                DEFAULT_PARTITION_COUNT,
                DEFAULT_REPLICA_COUNT,
                DEFAULT_ZONE_QUORUM_SIZE,
                IMMEDIATE_TIMER_VALUE,
                INFINITE_TIMER_VALUE,
                DEFAULT_FILTER,
                new CatalogStorageProfilesDescriptor(List.of(new CatalogStorageProfileDescriptor(DEFAULT_STORAGE_PROFILE))),
                STRONG_CONSISTENCY
        );
    }

    private static void checkDuplicateDefaultZoneName(Catalog catalog) {
        if (catalog.zone(DEFAULT_ZONE_NAME) == null) {
            return;
        }

        throw new CatalogValidationException(
                "Distribution zone with name '{}' already exists. Please specify zone name for the new table or set the zone as default",
                DEFAULT_ZONE_NAME
        );
    }

    /**
     * Returns zone with given name.
     *
     * @param catalog Catalog to look up zone in.
     * @param name Name of the zone of interest.
     * @param shouldThrowIfNotExists Flag indicated should be thrown the {@code CatalogValidationException} for
     *         absent zone or just return {@code null}.
     * @return Zone descriptor for given name or @{code null} in case zone is absent and flag shouldThrowIfNotExists set to {@code false}.
     * @throws CatalogValidationException If zone with given name is not exists and flag shouldThrowIfNotExists
     *         set to {@code true}.
     */
    public static @Nullable CatalogZoneDescriptor zone(
            Catalog catalog,
            String name,
            boolean shouldThrowIfNotExists
    ) throws CatalogValidationException {
        name = Objects.requireNonNull(name, "zoneName");

        CatalogZoneDescriptor zone = catalog.zone(name);

        if (zone == null && shouldThrowIfNotExists) {
            throw new CatalogValidationException("Distribution zone with name '{}' not found.", name);
        }

        return zone;
    }

    /**
     * Returns zone with given name.
     *
     * @param catalog Catalog to look up zone in.
     * @param name Name of the zone of interest.
     * @return Zone descriptor for given name.
     * @throws CatalogValidationException If zone with given name is not exists.
     */
    public static CatalogZoneDescriptor zoneOrThrow(Catalog catalog, String name)
            throws CatalogValidationException {
        return zone(catalog, name, true);
    }

    /**
     * Creates common exception for cases when validated distribution zone has already existed name.
     *
     * @param duplicatedZoneName Conflicting zone name.
     * @return Catalog validation exception instance with proper and common for such cases message.
     */
    public static CatalogValidationException duplicateDistributionZoneNameCatalogValidationException(String duplicatedZoneName) {
        return new CatalogValidationException("Distribution zone with name '{}' already exists.", duplicatedZoneName);
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
     * Returns index with given name.
     *
     * @param schema Schema to look up index in.
     * @param name Name of the index of interest.
     * @param shouldThrowIfNotExists Flag indicated should be thrown the {@code IndexNotFoundValidationException} for
     *         absent index or just return {@code null}.
     * @return Index descriptor for given name or @{code null} in case index is absent and flag shouldThrowIfNotExists set to {@code false}.
     * @throws IndexNotFoundValidationException If index with given name is not exists and flag shouldThrowIfNotExists set to {@code true}.
     */
    public static @Nullable CatalogIndexDescriptor index(CatalogSchemaDescriptor schema, String name, boolean shouldThrowIfNotExists)
            throws IndexNotFoundValidationException {
        CatalogIndexDescriptor index = schema.aliveIndex(name);

        if (index == null && shouldThrowIfNotExists) {
            throw new IndexNotFoundValidationException(format("Index with name '{}.{}' not found.", schema.name(), name));
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
            throw new IndexNotFoundValidationException(format("Index with ID '{}' not found.", indexId));
        }

        return index;
    }

    /**
     * Returns the timestamp at which the catalog version is guaranteed to be activated on every node of the cluster. This takes into
     * account possible clock skew between nodes.
     *
     * @param activationTs Catalog version activation timestamp of interest.
     * @param maxClockSkewMillis Max clock skew in milliseconds.
     */
    public static HybridTimestamp clusterWideEnsuredActivationTimestamp(long activationTs, long maxClockSkewMillis) {
        return HybridTimestamp.hybridTimestamp(activationTs).addPhysicalTime(maxClockSkewMillis)
                // Rounding up to the closest millisecond to account for possibility of HLC.now() having different
                // logical parts on different nodes of the cluster (see IGNITE_21084).
                .roundUpToPhysicalTick();
    }

    /** Returns id of the default zone from given catalog, or {@code null} if default zone is not exist. */
    public static @Nullable Integer defaultZoneIdOpt(Catalog catalog) {
        CatalogZoneDescriptor defaultZone = catalog.defaultZone();

        return defaultZone != null ? defaultZone.id() : null;
    }

    /**
     * Returns the maximum supported precision for given type or {@link #UNSPECIFIED_PRECISION}  if the type does not support precision.
     *
     * @param columnType Column type.
     * @return Maximum precision.
     */
    public static int getMaxPrecision(ColumnType columnType) {
        if (!columnType.precisionAllowed()) {
            return UNSPECIFIED_PRECISION;
        } else {
            switch (columnType) {
                case DECIMAL:
                    return MAX_DECIMAL_PRECISION;
                case TIME:
                case DATETIME:
                case TIMESTAMP:
                    return MAX_TIME_PRECISION;
                case DURATION:
                case PERIOD:
                    return MAX_INTERVAL_TYPE_PRECISION;
                default:
                    throw new IllegalArgumentException("Unexpected column type: " + columnType);
            }
        }
    }

    /**
     * Returns the minimum supported precision for given type or {@link #UNSPECIFIED_PRECISION} if the type does not support precision.
     *
     * @param columnType Column type.
     * @return Minimum precision.
     */
    public static int getMinPrecision(ColumnType columnType) {
        if (!columnType.precisionAllowed()) {
            return UNSPECIFIED_PRECISION;
        } else {
            switch (columnType) {
                case DECIMAL:
                    return MIN_DECIMAL_PRECISION;
                case TIME:
                case DATETIME:
                case TIMESTAMP:
                    return MIN_TIME_PRECISION;
                case DURATION:
                case PERIOD:
                    return MIN_INTERVAL_TYPE_PRECISION;
                default:
                    throw new IllegalArgumentException("Unexpected column type: " + columnType);
            }
        }
    }

    /**
     * Returns the maximum supported length for given type or {@link #UNSPECIFIED_LENGTH} if the type does not support length.
     *
     * @param columnType Column type.
     * @return Maximum length.
     */
    public static int getMaxLength(ColumnType columnType) {
        if (!columnType.lengthAllowed()) {
            return UNSPECIFIED_LENGTH;
        } else {
            switch (columnType) {
                case STRING:
                case BYTE_ARRAY:
                    return MAX_VARLEN_LENGTH;
                default:
                    throw new IllegalArgumentException("Unexpected column type: " + columnType);
            }
        }
    }

    /**
     * Returns the minimum supported length for given type or {@link #UNSPECIFIED_LENGTH} if the type does not support length.
     *
     * @param columnType Column type.
     * @return Minimum length.
     */
    public static int getMinLength(ColumnType columnType) {
        if (!columnType.lengthAllowed()) {
            return UNSPECIFIED_LENGTH;
        } else {
            switch (columnType) {
                case STRING:
                case BYTE_ARRAY:
                    return MIN_VARLEN_PRECISION;
                default:
                    throw new IllegalArgumentException("Unexpected column type: " + columnType);
            }
        }
    }

    /**
     * Returns the maximum supported scale for given type or {@link #UNSPECIFIED_SCALE} if the type does not support scale.
     *
     * @param columnType Column type.
     * @return Maximum scale.
     */
    public static int getMaxScale(ColumnType columnType) {
        if (!columnType.scaleAllowed()) {
            return UNSPECIFIED_SCALE;
        } else {
            if (columnType == ColumnType.DECIMAL) {
                return MAX_DECIMAL_SCALE;
            }
            throw new IllegalArgumentException("Unexpected column type: " + columnType);
        }
    }

    /**
     * Returns the minimum supported scale for given type or {@link #UNSPECIFIED_SCALE} if the type does not support scale.
     *
     * @param columnType Column type.
     * @return Minimum scale.
     */
    public static int getMinScale(ColumnType columnType) {
        if (!columnType.scaleAllowed()) {
            return UNSPECIFIED_SCALE;
        } else {
            if (columnType == ColumnType.DECIMAL) {
                return MIN_DECIMAL_SCALE;
            }
            throw new IllegalArgumentException("Unexpected column type: " + columnType);
        }
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
                        "Functional default type mismatch: [col={}, functionName={}, expectedType={}, actualType={}].",
                        columnName, functionName, returnType, columnType);
            }

            throw new CatalogValidationException(
                    "Functional default contains unsupported function: [col={}, functionName={}].",
                    columnName, functionName);
        }

        throw new CatalogValidationException("Default of unsupported kind: [col={}, defaultType={}].", columnName, defaultValue.type);
    }

    /**
     * Check if provided default value is a constant, or fail otherwise.
     */
    static void ensureNonFunctionalDefault(String columnName, @Nullable DefaultValue defaultValue) {
        if (defaultValue == null || defaultValue.type == Type.CONSTANT) {
            return;
        }

        if (defaultValue.type == FUNCTION_CALL) {
            throw new CatalogValidationException("Functional defaults are not supported for non-primary key columns [col={}].", columnName);
        }

        throw new CatalogValidationException("Default of unsupported kind: [col={}, defaultType={}].", columnName, defaultValue.type);
    }

    /**
     * Check if provided column type can be persisted, or fail otherwise.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-17373
    //  Remove this after interval type support is added.
    static void ensureTypeCanBeStored(String columnName, ColumnType columnType) {
        if (columnType == ColumnType.PERIOD || columnType == ColumnType.DURATION) {
            throw new CatalogValidationException("Column of type '{}' cannot be persisted [col={}].", columnType, columnName);
        }
    }

    /**
     * Calculates default quorum size based on the number of replicas.
     *
     * @param replicas Number of replicas.
     * @return Quorum size.
     */
    public static int defaultQuorumSize(int replicas) {
        if (replicas <= 4) {
            return min(replicas, 2);
        }
        return 3;
    }

    /**
     * Resolves column names from their corresponding column IDs within a catalog table.
     *
     * <p>This method takes a list of column IDs and maps each ID to its corresponding column name
     * by looking up the column descriptor in the provided table. The order of column names in the
     * returned list matches the order of column IDs in the input list.
     *
     * @param table The catalog table descriptor containing the column definitions to search within.
     * @param columnIds The list of column IDs to resolve into column names.
     * @return A list of column names corresponding to the provided column IDs, in the same order.
     * @throws IllegalArgumentException if any column ID in the list does not exist in the table
     */
    public static List<String> resolveColumnNames(CatalogTableDescriptor table, IntList columnIds) {
        List<String> names = new ArrayList<>(columnIds.size());
        for (int columnId : columnIds) {
            CatalogTableColumnDescriptor column = table.columnById(columnId);
            if (column == null) {
                throw new IllegalArgumentException("Column with id=" + columnId + " not found in table " + table);
            }

            names.add(column.name());
        }

        return names;
    }
}
