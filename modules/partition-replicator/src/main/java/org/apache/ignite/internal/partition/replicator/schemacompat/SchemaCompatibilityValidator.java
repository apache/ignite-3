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

package org.apache.ignite.internal.partition.replicator.schemacompat;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.partition.replicator.schema.ColumnDefinitionDiff;
import org.apache.ignite.internal.partition.replicator.schema.FullTableSchema;
import org.apache.ignite.internal.partition.replicator.schema.TableDefinitionDiff;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.TransactionIds;
import org.jetbrains.annotations.Nullable;

/**
 * Validates schema compatibility.
 */
public class SchemaCompatibilityValidator {
    private final ValidationSchemasSource validationSchemasSource;
    private final CatalogService catalogService;
    private final SchemaSyncService schemaSyncService;

    // TODO: Remove entries from cache when compacting schemas in SchemaManager https://issues.apache.org/jira/browse/IGNITE-20789
    private final ConcurrentMap<TableDefinitionDiffKey, TableDefinitionDiff> diffCache = new ConcurrentHashMap<>();

    private static final List<ForwardCompatibilityValidator> FORWARD_COMPATIBILITY_VALIDATORS = List.of(
            new RenameTableValidator(),
            new AddColumnsValidator(),
            new DropColumnsValidator(),
            new ChangeColumnsValidator()
    );

    /** Constructor. */
    public SchemaCompatibilityValidator(
            ValidationSchemasSource validationSchemasSource,
            CatalogService catalogService,
            SchemaSyncService schemaSyncService
    ) {
        this.validationSchemasSource = validationSchemasSource;
        this.catalogService = catalogService;
        this.schemaSyncService = schemaSyncService;
    }

    /**
     * Performs commit validation. That is, checks that each table enlisted in the tranasction still exists at the commit timestamp,
     * and that the initial schema of the table (identified by the begin timestamp) is forward-compatible with the commit schema
     * (identified by the commit timestamp).
     *
     * @param txId ID of the transaction that gets validated.
     * @param enlistedTableIds IDs of the tables that are enlisted with the transaction.
     * @param commitTimestamp Commit timestamp.
     * @return Future of validation result.
     */
    public CompletableFuture<CompatValidationResult> validateCommit(
            UUID txId,
            Set<Integer> enlistedTableIds,
            HybridTimestamp commitTimestamp
    ) {
        HybridTimestamp beginTimestamp = TransactionIds.beginTimestamp(txId);

        // Using compareTo() instead of after()/begin() because the latter methods take clock skew into account
        // which only makes sense when comparing 'unrelated' timestamps. beginTs and commitTs have a causal relationship,
        // so we don't need to account for clock skew.
        assert commitTimestamp.compareTo(beginTimestamp) > 0;

        return schemaSyncService.waitForMetadataCompleteness(commitTimestamp)
                .thenApply(ignored -> validateCommit(enlistedTableIds, commitTimestamp, beginTimestamp));
    }

    private CompatValidationResult validateCommit(Set<Integer> tableIds, HybridTimestamp commitTimestamp, HybridTimestamp beginTimestamp) {
        for (int tableId : tableIds) {
            CompatValidationResult validationResult = validateCommit(beginTimestamp, commitTimestamp, tableId);

            if (!validationResult.isSuccessful()) {
                return validationResult;
            }
        }

        return CompatValidationResult.success();
    }

    private CompatValidationResult validateCommit(HybridTimestamp beginTimestamp, HybridTimestamp commitTimestamp, int tableId) {
        CatalogTableDescriptor tableAtCommitTs = catalogService.activeCatalog(commitTimestamp.longValue()).table(tableId);

        if (tableAtCommitTs == null) {
            CatalogTableDescriptor tableAtTxStart = catalogService.activeCatalog(beginTimestamp.longValue()).table(tableId);
            assert tableAtTxStart != null : "No table " + tableId + " at ts " + beginTimestamp;

            return CompatValidationResult.tableDropped(tableAtTxStart.name(), tableAtTxStart.schemaId());
        }

        return validateForwardSchemaCompatibility(beginTimestamp, commitTimestamp, tableId);
    }

    /**
     * Performs forward compatibility validation. That is, for the given table, checks to see whether the
     * initial schema (identified by the begin timestamp) is forward-compatible with the commit schema (identified by the commit
     * timestamp).
     *
     * @param beginTimestamp Begin timestamp of a transaction.
     * @param commitTimestamp Commit timestamp.
     * @param tableId ID of the table that is under validation.
     * @return Validation result.
     */
    private CompatValidationResult validateForwardSchemaCompatibility(
            HybridTimestamp beginTimestamp,
            HybridTimestamp commitTimestamp,
            int tableId
    ) {
        List<FullTableSchema> tableSchemas = validationSchemasSource.tableSchemaVersionsBetween(tableId, beginTimestamp, commitTimestamp);

        assert !tableSchemas.isEmpty();

        for (int i = 0; i < tableSchemas.size() - 1; i++) {
            FullTableSchema oldSchema = tableSchemas.get(i);
            FullTableSchema newSchema = tableSchemas.get(i + 1);

            ValidationResult validationResult = validateForwardSchemaCompatibility(oldSchema, newSchema);

            if (validationResult.verdict == ValidatorVerdict.INCOMPATIBLE) {
                return CompatValidationResult.incompatibleChange(
                        oldSchema.tableName(),
                        oldSchema.schemaVersion(),
                        newSchema.schemaVersion(),
                        validationResult.details()
                );
            }
        }

        return CompatValidationResult.success();
    }

    private ValidationResult validateForwardSchemaCompatibility(FullTableSchema prevSchema, FullTableSchema nextSchema) {
        TableDefinitionDiff diff = diffCache.computeIfAbsent(
                new TableDefinitionDiffKey(prevSchema.tableId(), prevSchema.schemaVersion(), nextSchema.schemaVersion()),
                key -> nextSchema.diffFrom(prevSchema)
        );

        boolean accepted = false;

        for (ForwardCompatibilityValidator validator : FORWARD_COMPATIBILITY_VALIDATORS) {
            ValidationResult validationResult = validator.compatible(diff);
            switch (validationResult.verdict) {
                case COMPATIBLE:
                    accepted = true;
                    break;
                case INCOMPATIBLE:
                    return validationResult;
                default:
                    break;
            }
        }

        assert accepted : "Table schema changed from " + prevSchema.schemaVersion()
                + " to " + nextSchema.schemaVersion()
                + ", but no schema change validator voted for any change. Some schema validator is missing.";

        return ValidationResult.COMPATIBLE;
    }

    /**
     * Performs backward compatibility validation of a tuple that was just read in the transaction.
     *
     * <ul>
     *     <li>If the tuple was written with a schema version earlier or same as the initial schema version of the transaction,
     *     the read is valid.</li>
     *     <li>If the tuple was written with a schema version later than the initial schema version of the transaction,
     *     the read is valid only if the initial schema version is backward compatible with the tuple schema version.</li>
     * </ul>
     *
     * @param tupleSchemaVersion Schema version ID of the tuple.
     * @param tableId ID of the table to which the tuple belongs.
     * @param txId ID of the transaction that gets validated.
     * @return Future of validation result.
     */
    public CompletableFuture<CompatValidationResult> validateBackwards(int tupleSchemaVersion, int tableId, UUID txId) {
        HybridTimestamp beginTimestamp = TransactionIds.beginTimestamp(txId);

        return schemaSyncService.waitForMetadataCompleteness(beginTimestamp)
                .thenCompose(ignored -> validationSchemasSource.waitForSchemaAvailability(tableId, tupleSchemaVersion))
                .thenApply(ignored -> validateBackwardSchemaCompatibility(tupleSchemaVersion, tableId, beginTimestamp));
    }

    private CompatValidationResult validateBackwardSchemaCompatibility(
            int tupleSchemaVersion,
            int tableId,
            HybridTimestamp beginTimestamp
    ) {
        List<FullTableSchema> tableSchemas = validationSchemasSource.tableSchemaVersionsBetween(
                tableId,
                beginTimestamp,
                tupleSchemaVersion
        );

        if (tableSchemas.size() < 2) {
            // The tuple was not written with a future schema.
            return CompatValidationResult.success();
        }

        FullTableSchema oldSchema = tableSchemas.get(0);
        FullTableSchema newSchema = tableSchemas.get(1);
        return CompatValidationResult.incompatibleChange(
                oldSchema.tableName(),
                oldSchema.schemaVersion(),
                newSchema.schemaVersion(),
                null
        );
    }

    /**
     * Throws an exception if schema has changed between transaction start and operation timestamp.
     *
     * @param txId ID of the transaction.
     * @param operationTimestamp Timestamp of the operation.
     * @param tableId ID of the table which the operation concerns.
     * @throws IncompatibleSchemaVersionException If schema has changed.
     */
    public void failIfSchemaChangedAfterTxStart(UUID txId, HybridTimestamp operationTimestamp, int tableId) {
        HybridTimestamp beginTs = TransactionIds.beginTimestamp(txId);
        CatalogTableDescriptor tableAtBeginTs = catalogService.activeCatalog(beginTs.longValue()).table(tableId);
        CatalogTableDescriptor tableAtOpTs = catalogService.activeCatalog(operationTimestamp.longValue()).table(tableId);

        assert tableAtBeginTs != null : "No table " + tableId + " at ts " + tableAtBeginTs;

        if (tableAtOpTs == null) {
            throw IncompatibleSchemaVersionException.tableDropped(tableAtBeginTs.name());
        }

        if (tableAtOpTs.latestSchemaVersion() != tableAtBeginTs.latestSchemaVersion()) {
            throw IncompatibleSchemaVersionException.schemaChanged(
                    tableAtBeginTs.name(),
                    tableAtBeginTs.latestSchemaVersion(),
                    tableAtOpTs.latestSchemaVersion()
            );
        }
    }

    /**
     * Throws an exception if the given table does not exist at the given operation timestamp.
     *
     * @param operationTimestamp Timestamp of the operation.
     * @param tableId ID of the table
     * @throws IncompatibleSchemaVersionException If the table doesn't exist at the timestamp.
     */
    public void failIfTableDoesNotExistAt(HybridTimestamp operationTimestamp, int tableId) {
        CatalogTableDescriptor tableAtOpTs = catalogService.activeCatalog(operationTimestamp.longValue()).table(tableId);

        if (tableAtOpTs == null) {
            throw IncompatibleSchemaVersionException.tableDropped(tableId);
        }
    }

    /**
     * Throws an {@link InternalSchemaVersionMismatchException} if the schema version passed in the request differs from the schema version
     * corresponding to the transaction timestamp.
     *
     * @param txTs Transaction timestamp.
     * @param requestSchemaVersion Schema version passed in the operation request.
     * @param tableId ID of the table.
     * @throws InternalSchemaVersionMismatchException Thrown if the schema versions are different.
     */
    public void failIfRequestSchemaDiffersFromTxTs(HybridTimestamp txTs, int requestSchemaVersion, int tableId) {
        CatalogTableDescriptor table = catalogService.activeCatalog(txTs.longValue()).table(tableId);

        assert table != null : "No table " + tableId + " at " + txTs;

        if (table.latestSchemaVersion() != requestSchemaVersion) {
            throw new InternalSchemaVersionMismatchException();
        }
    }

    private static class ValidationResult {
        private static final ValidationResult COMPATIBLE = new ValidationResult(ValidatorVerdict.COMPATIBLE, null);
        private static final ValidationResult DONT_CARE = new ValidationResult(ValidatorVerdict.DONT_CARE, null);

        private final ValidatorVerdict verdict;
        private final String details;

        ValidationResult(ValidatorVerdict verdict, @Nullable String details) {
            this.verdict = verdict;
            this.details = details;
        }

        ValidatorVerdict verdict() {
            return verdict;
        }

        @Nullable String details() {
            return details;
        }
    }

    private enum ValidatorVerdict {
        /**
         * Validator accepts a change: it's compatible.
         */
        COMPATIBLE,
        /**
         * Validator rejects a change: it's incompatible.
         */
        INCOMPATIBLE,
        /**
         * Validator does not know how to handle a change.
         */
        DONT_CARE
    }

    @SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
    private interface ForwardCompatibilityValidator {
        ValidationResult compatible(TableDefinitionDiff diff);
    }

    private static class RenameTableValidator implements ForwardCompatibilityValidator {
        private static final ValidationResult INCOMPATIBLE = new ValidationResult(
                ValidatorVerdict.INCOMPATIBLE,
                "Name of the table has been changed"
        );

        @Override
        public ValidationResult compatible(TableDefinitionDiff diff) {
            return diff.nameDiffers() ? INCOMPATIBLE : ValidationResult.DONT_CARE;
        }
    }

    private static class AddColumnsValidator implements ForwardCompatibilityValidator {

        @Override
        public ValidationResult compatible(TableDefinitionDiff diff) {
            if (diff.addedColumns().isEmpty()) {
                return ValidationResult.DONT_CARE;
            }

            return ValidationResult.COMPATIBLE;
        }
    }

    private static class DropColumnsValidator implements ForwardCompatibilityValidator {
        private static final ValidationResult INCOMPATIBLE = new ValidationResult(
                ValidatorVerdict.INCOMPATIBLE,
                "Columns were dropped"
        );

        @Override
        public ValidationResult compatible(TableDefinitionDiff diff) {
            return diff.removedColumns().isEmpty() ? ValidationResult.DONT_CARE : INCOMPATIBLE;
        }
    }

    @SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
    private interface ColumnChangeCompatibilityValidator {
        ValidationResult compatible(ColumnDefinitionDiff diff);
    }

    private static class ChangeColumnsValidator implements ForwardCompatibilityValidator {
        private static final List<ColumnChangeCompatibilityValidator> validators = List.of(
                // TODO: https://issues.apache.org/jira/browse/IGNITE-20948 - add validator that says that column rename is compatible.
                new ChangeNullabilityValidator(),
                new ChangeDefaultValueValidator(),
                new ChangeColumnTypeValidator()
        );

        @Override
        public ValidationResult compatible(TableDefinitionDiff diff) {
            if (diff.changedColumns().isEmpty()) {
                return ValidationResult.DONT_CARE;
            }

            boolean accepted = false;

            for (ColumnDefinitionDiff columnDiff : diff.changedColumns()) {
                ValidationResult validationResult = compatible(columnDiff);
                switch (validationResult.verdict()) {
                    case COMPATIBLE:
                        accepted = true;
                        break;
                    case INCOMPATIBLE:
                        return validationResult;
                    default:
                        break;
                }
            }

            assert accepted : "Table schema changed from " + diff.oldSchemaVersion() + " to "
                    + diff.newSchemaVersion() + ", but no column change validator voted for any change. Some schema validator is missing.";

            return ValidationResult.COMPATIBLE;
        }

        private static ValidationResult compatible(ColumnDefinitionDiff columnDiff) {
            boolean accepted = false;

            for (ColumnChangeCompatibilityValidator validator : validators) {
                ValidationResult validationResult = validator.compatible(columnDiff);

                switch (validationResult.verdict()) {
                    case COMPATIBLE:
                        accepted = true;
                        break;
                    case INCOMPATIBLE:
                        return validationResult;
                    default:
                        break;
                }
            }

            return accepted ? ValidationResult.COMPATIBLE : ValidationResult.DONT_CARE;
        }
    }

    private static class ChangeDefaultValueValidator implements ColumnChangeCompatibilityValidator {
        @Override
        public ValidationResult compatible(ColumnDefinitionDiff diff) {
            return diff.defaultChanged()
                    ? new ValidationResult(ValidatorVerdict.INCOMPATIBLE, "Column default value changed")
                    : ValidationResult.DONT_CARE;
        }
    }

    private static class ChangeNullabilityValidator implements ColumnChangeCompatibilityValidator {
        @Override
        public ValidationResult compatible(ColumnDefinitionDiff diff) {
            if (diff.notNullAdded()) {
                return new ValidationResult(ValidatorVerdict.INCOMPATIBLE, "Not null added");
            }

            if (diff.notNullDropped()) {
                return ValidationResult.COMPATIBLE;
            }

            assert !diff.nullabilityChanged() : diff;

            return ValidationResult.DONT_CARE;
        }
    }

    private static class ChangeColumnTypeValidator implements ColumnChangeCompatibilityValidator {
        @Override
        public ValidationResult compatible(ColumnDefinitionDiff diff) {
            if (!diff.typeChanged()) {
                return ValidationResult.DONT_CARE;
            }

            return diff.typeChangeIsSupported()
                    ? ValidationResult.COMPATIBLE
                    : new ValidationResult(ValidatorVerdict.INCOMPATIBLE, "Column type changed incompatibly");
        }
    }
}
