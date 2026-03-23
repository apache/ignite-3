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

import com.jayway.jsonpath.InvalidPathException;
import com.jayway.jsonpath.JsonPath;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogStorageProfileDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.descriptors.ConsistencyMode;
import org.apache.ignite.internal.util.StringUtils;
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
            throw new CatalogValidationException("{} can't be null or blank.", context);
        }
    }

    /**
     * Validates correctness of incoming value.
     */
    public static void validateField(@Nullable Integer value, int min, @Nullable Integer max, String errorPrefix) {
        if (value == null) {
            return;
        }

        if (value < min || (max != null && value > max)) {
            throw new CatalogValidationException(
                    "{}: [value={}, min={}" + (max == null ? ']' : ", max={}]."),
                    errorPrefix, value, min, max
            );
        }
    }

    /**
     * Validates correctness of the filter.
     */
    public static void validateZoneFilter(@Nullable String filter) {
        if (filter == null) {
            return;
        }

        try {
            JsonPath.compile(filter);
        } catch (InvalidPathException e) {
            String error = e.getMessage() == null ? "Unknown JsonPath compilation error." : e.getMessage();

            throw new CatalogValidationException(
                    "Invalid filter: [value={}, error={}].",
                    e,
                    filter, error
            );
        }
    }

    /**
     * Validates correctness of the storage profiles.
     */
    public static void validateStorageProfiles(List<StorageProfileParams> storageProfiles) {
        if (storageProfiles == null) {
            throw new CatalogValidationException(
                    "Storage profile cannot be null."
            );
        }

        if (storageProfiles.isEmpty()) {
            throw new CatalogValidationException(
                    "Storage profile cannot be empty."
            );
        }
    }

    /**
     * Validates correctness of the storage profiles.
     */
    public static void validateStorageProfileNames(List<String> storageProfileNames) {
        if (storageProfileNames == null) {
            throw new CatalogValidationException(
                    "Storage profile cannot be null."
            );
        }

        if (storageProfileNames.isEmpty()) {
            throw new CatalogValidationException(
                    "Storage profile cannot be empty."
            );
        }
    }

    /**
     * Validates that given consistency mode is has only the expected values.
     *
     * @param consistencyMode Consistency mode to validate.
     */
    public static void validateConsistencyMode(@Nullable ConsistencyMode consistencyMode) {
        if (consistencyMode != null
                && !(consistencyMode == ConsistencyMode.HIGH_AVAILABILITY || consistencyMode == ConsistencyMode.STRONG_CONSISTENCY)) {
            throw new CatalogValidationException("Consistency mode is not supported: [mode={}].", consistencyMode);
        }
    }

    /**
     * Validates that given partitions number is null, meaning that altering of a partitions number is forbidden.
     *
     * @param partitions Partitions number to validate.
     */
    public static void validatePartition(@Nullable Integer partitions) {
        if (partitions != null) {
            throw new CatalogValidationException("Partitions number cannot be altered.");
        }
    }

    /**
     * Validates that given schema doesn't contain any relation with specified name.
     *
     * @param schema Schema to look up relation with specified name.
     * @param name Name of the relation to look up.
     * @throws CatalogValidationException If relation with specified name exists in given schema.
     */
    public static void ensureNoTableIndexOrSysViewExistsWithGivenName(CatalogSchemaDescriptor schema, String name) {
        if (schema.aliveIndex(name) != null) {
            throw new CatalogValidationException("Index with name '{}.{}' already exists.", schema.name(), name);
        }

        if (schema.table(name) != null) {
            throw new CatalogValidationException("Table with name '{}.{}' already exists.", schema.name(), name);
        }

        if (schema.systemView(name) != null) {
            throw new CatalogValidationException("System view with name '{}.{}' already exists.", schema.name(), name);
        }
    }

    /**
     * Validates that table's zone contains table's storage profile.
     *
     * @throws CatalogValidationException If zone does not contain table's storage profile.
     */
    public static void ensureZoneContainsTablesStorageProfile(CatalogZoneDescriptor zone, String tableStorageProfile) {
        Set<String> zonesStorageProfile = zone.storageProfiles().profiles()
                .stream().map(CatalogStorageProfileDescriptor::storageProfile)
                .collect(Collectors.toSet());

        if (!zonesStorageProfile.contains(tableStorageProfile)) {
            throw new CatalogValidationException(
                    "Zone with name '{}' does not contain table's storage profile [storageProfile='{}'].",
                    zone.name(),
                    tableStorageProfile
            );
        }
    }

    /**
     * Validates that given schema name is not a reserved system name.
     *
     * @param schemaName Schema name to check.
     * @throws CatalogValidationException If given schema name is reserved system name.
     */
    public static void ensureNonSystemSchemaUsed(String schemaName) {
        if (schemaName != null && CatalogUtils.isSystemSchema(schemaName)) {
            throw new CatalogValidationException("Operations with system schemas are not allowed, schema: {}", schemaName);
        }
    }
}
