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

package org.apache.ignite.internal.catalog;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.MAX_PARTITION_COUNT;
import static org.apache.ignite.lang.IgniteStringFormatter.format;

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Predicate;
import org.apache.ignite.internal.catalog.commands.AbstractCreateIndexCommandParams;
import org.apache.ignite.internal.catalog.commands.AbstractIndexCommandParams;
import org.apache.ignite.internal.catalog.commands.AbstractTableCommandParams;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.ErrorGroups.DistributionZones;
import org.apache.ignite.lang.ErrorGroups.Index;
import org.apache.ignite.lang.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for validating catalog commands parameters.
 */
public class CatalogParamsValidationUtils {
    /**
     * Validates that given identifier string neither null nor blank.
     *
     * @param identifier Identifier to validate.
     * @param context Context to build message for exception in case validation fails.
     *      The message has the following format: `{context} can't be null or blank`.
     * @throws CatalogValidationException If the specified identifier does not meet the requirements.
     */
    public static void validateIdentifier(@Nullable String identifier, String context) throws CatalogValidationException {
        if (StringUtils.nullOrBlank(identifier)) {
            throw new CatalogValidationException(format("{} can't be null or blank", context));
        }
    }

    static void validateCreateZoneParams(CreateZoneParams params) {
        validateUpdateZoneFieldsParameters(
                params.zoneName(),
                params.partitions(),
                params.replicas(),
                params.dataNodesAutoAdjust(),
                params.dataNodesAutoAdjustScaleUp(),
                params.dataNodesAutoAdjustScaleDown(),
                params.filter()
        );
    }

    static void validateAlterZoneParams(AlterZoneParams params) {
        validateUpdateZoneFieldsParameters(
                params.zoneName(),
                params.partitions(),
                params.replicas(),
                params.dataNodesAutoAdjust(),
                params.dataNodesAutoAdjustScaleUp(),
                params.dataNodesAutoAdjustScaleDown(),
                params.filter()
        );
    }

    static void validateCreateHashIndexParams(CreateHashIndexParams params) {
        validateCommonCreateIndexParams(params);
    }

    static void validateCreateSortedIndexParams(CreateSortedIndexParams params) {
        validateCommonCreateIndexParams(params);

        validateCollectionIsNotEmpty(params.collations(), Index.INVALID_INDEX_DEFINITION_ERR, "Columns collations not specified");

        if (params.collations().size() != params.columns().size()) {
            throw new CatalogValidationException(Index.INVALID_INDEX_DEFINITION_ERR, "Columns collations doesn't match number of columns");
        }
    }

    static void validateDropIndexParams(DropIndexParams params) {
        validateCommonIndexParams(params);
    }

    static void validateDropZoneParams(DropZoneParams params) {
        validateZoneName(params.zoneName());
    }

    static void validateRenameZoneParams(RenameZoneParams params) {
        validateZoneName(params.zoneName());
        validateZoneName(params.newZoneName(), "Missing new zone name");
    }

    private static void validateUpdateZoneFieldsParameters(
            String zoneName,
            @Nullable Integer partitions,
            @Nullable Integer replicas,
            @Nullable Integer dataNodesAutoAdjust,
            @Nullable Integer dataNodesAutoAdjustScaleUp,
            @Nullable Integer dataNodesAutoAdjustScaleDown,
            @Nullable String filter
    ) {
        validateZoneName(zoneName);

        validateZonePartitions(partitions);
        validateZoneReplicas(replicas);

        validateZoneDataNodesAutoAdjust(dataNodesAutoAdjust);
        validateZoneDataNodesAutoAdjustScaleUp(dataNodesAutoAdjustScaleUp);
        validateZoneDataNodesAutoAdjustScaleDown(dataNodesAutoAdjustScaleDown);

        validateZoneDataNodesAutoAdjustParametersCompatibility(
                dataNodesAutoAdjust,
                dataNodesAutoAdjustScaleUp,
                dataNodesAutoAdjustScaleDown
        );

        validateZoneFilter(filter);
    }

    private static void validateZoneName(String zoneName) {
        validateZoneName(zoneName, "Missing zone name");
    }

    private static void validateZoneName(String zoneName, String errorMessage) {
        validateNameField(zoneName, DistributionZones.ZONE_DEFINITION_ERR, errorMessage);
    }

    private static void validateZonePartitions(@Nullable Integer partitions) {
        validateZoneField(partitions, 1, MAX_PARTITION_COUNT, "Invalid number of partitions");
    }

    private static void validateZoneReplicas(@Nullable Integer replicas) {
        validateZoneField(replicas, 1, null, "Invalid number of replicas");
    }

    private static void validateZoneDataNodesAutoAdjust(@Nullable Integer dataNodesAutoAdjust) {
        validateZoneField(dataNodesAutoAdjust, 0, null, "Invalid data nodes auto adjust");
    }

    private static void validateZoneDataNodesAutoAdjustScaleUp(@Nullable Integer dataNodesAutoAdjustScaleUp) {
        validateZoneField(dataNodesAutoAdjustScaleUp, 0, null, "Invalid data nodes auto adjust scale up");
    }

    private static void validateZoneDataNodesAutoAdjustScaleDown(@Nullable Integer dataNodesAutoAdjustScaleDown) {
        validateZoneField(dataNodesAutoAdjustScaleDown, 0, null, "Invalid data nodes auto adjust scale down");
    }

    static void validateZoneDataNodesAutoAdjustParametersCompatibility(
            @Nullable Integer autoAdjust,
            @Nullable Integer scaleUp,
            @Nullable Integer scaleDown
    ) {
        if (autoAdjust != null && (scaleUp != null || scaleDown != null)) {
            throw new CatalogValidationException(
                    DistributionZones.ZONE_DEFINITION_ERR,
                    "Not compatible parameters [dataNodesAutoAdjust={}, dataNodesAutoAdjustScaleUp={}, dataNodesAutoAdjustScaleDown={}]",
                    autoAdjust, scaleUp, scaleDown
            );
        }
    }

    private static void validateZoneFilter(@Nullable String filter) {
        if (filter == null) {
            return;
        }

        try {
            JsonPath.compile(filter);
        } catch (InvalidPathException e) {
            String error = e.getMessage() == null ? "Unknown JsonPath compilation error." : e.getMessage();

            throw new CatalogValidationException(
                    DistributionZones.ZONE_DEFINITION_ERR,
                    "Invalid filter: [value={}, error={}]",
                    e,
                    filter, error
            );
        }
    }

    private static void validateZoneField(@Nullable Integer value, int min, @Nullable Integer max, String errorPrefix) {
        if (value == null) {
            return;
        }

        if (value < min || (max != null && value > max)) {
            throw new CatalogValidationException(
                    DistributionZones.ZONE_DEFINITION_ERR,
                    "{}: [value={}, min={}" + (max == null ? ']' : ", max={}]"),
                    errorPrefix, value, min, max
            );
        }
    }

    private static void validateCommonIndexParams(AbstractIndexCommandParams params) {
        validateNameField(params.indexName(), Index.INVALID_INDEX_DEFINITION_ERR, "Missing index name");
    }

    private static void validateCommonCreateIndexParams(AbstractCreateIndexCommandParams params) {
        validateCommonIndexParams(params);

        validateNameField(params.tableName(), Index.INVALID_INDEX_DEFINITION_ERR, "Missing table name");

        validateColumns(
                params.columns(),
                Index.INVALID_INDEX_DEFINITION_ERR,
                "Columns not specified",
                "Duplicate columns are present: {}"
        );
    }

    private static void validateNameField(String name, int errorCode, String errorMessage) {
        if (StringUtils.nullOrBlank(name)) {
            throw new CatalogValidationException(errorCode, errorMessage);
        }
    }

    private static void validateCollectionIsNotEmpty(Collection<?> collection, int errorCode, String errorMessage) {
        if (CollectionUtils.nullOrEmpty(collection)) {
            throw new CatalogValidationException(errorCode, errorMessage);
        }
    }

    private static void validateColumns(
            List<String> columns,
            int errorCode,
            String emptyColumnsErrorMessage,
            String duplicateColumnsErrorMessageFormat
    ) {
        validateCollectionIsNotEmpty(columns, errorCode, emptyColumnsErrorMessage);

        List<String> duplicates = columns.stream()
                .filter(Predicate.not(new HashSet<>()::add))
                .collect(toList());

        if (!duplicates.isEmpty()) {
            throw new CatalogValidationException(errorCode, duplicateColumnsErrorMessageFormat, duplicates);
        }
    }

    private static void validateCommonTableParams(AbstractTableCommandParams params) {
        validateIdentifier(params.tableName(), "Name of the table");
    }

    /**
     * Validates given column parameters.
     *
     * @param params Parameters to validate.
     * @throws CatalogValidationException If validation has failed.
     */
    // TODO: IGNITE-19938 Add validation column length, precision and scale
    public static void validateColumnParams(ColumnParams params) {
        validateIdentifier(params.name(), "Name of the column");

        if (params.type() == null) {
            throw new CatalogValidationException("Missing column type: " + params.name());
        }
    }

    /**
     * Validates that given schema doesn't contain any relation with specified name.
     *
     * @param schema Schema to look up relation with specified name.
     * @param name Name of the relation to look up.
     * @throws CatalogValidationException If relation with specified name exists in given schema.
     */
    public static void ensureNoTableOrIndexExistsWithGivenName(CatalogSchemaDescriptor schema, String name) {
        if (schema.index(name) != null) {
            throw new CatalogValidationException(format("Index with name '{}.{}' already exists", schema.name(), name));
        }

        if (schema.table(name) != null) {
            throw new TableExistsValidationException(format("Table with name '{}.{}' already exists", schema.name(), name));
        }
    }
}
