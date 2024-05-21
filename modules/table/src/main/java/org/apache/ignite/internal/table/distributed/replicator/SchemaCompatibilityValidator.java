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

package org.apache.ignite.internal.table.distributed.replicator;

import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.schema.ColumnDefinitionDiff;
import org.apache.ignite.internal.table.distributed.schema.FullTableSchema;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.TableDefinitionDiff;
import org.apache.ignite.internal.table.distributed.schema.ValidationSchemasSource;
import org.apache.ignite.internal.tx.TransactionIds;

/**
 * Validates schema compatibility.
 */
class SchemaCompatibilityValidator {
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
    SchemaCompatibilityValidator(
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
     * @param enlistedGroupIds IDs of the partitions that are enlisted with the transaction.
     * @param commitTimestamp Commit timestamp.
     * @return Future of validation result.
     */
    CompletableFuture<CompatValidationResult> validateCommit(
            UUID txId,
            Collection<TablePartitionId> enlistedGroupIds,
            HybridTimestamp commitTimestamp
    ) {
        HybridTimestamp beginTimestamp = TransactionIds.beginTimestamp(txId);

        Set<Integer> tableIds = enlistedGroupIds.stream()
                .map(TablePartitionId::tableId)
                .collect(toSet());

        // Using compareTo() instead of after()/begin() because the latter methods take clock skew into account
        // which only makes sense when comparing 'unrelated' timestamps. beginTs and commitTs have a causal relationship,
        // so we don't need to account for clock skew.
        assert commitTimestamp.compareTo(beginTimestamp) > 0;

        return schemaSyncService.waitForMetadataCompleteness(commitTimestamp)
                .thenApply(ignored -> validateCommit(tableIds, commitTimestamp, beginTimestamp));
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
        CatalogTableDescriptor tableAtCommitTs = catalogService.table(tableId, commitTimestamp.longValue());

        if (tableAtCommitTs == null) {
            CatalogTableDescriptor tableAtTxStart = catalogService.table(tableId, beginTimestamp.longValue());
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

            List<ForwardCompatibilityValidator> failedValidators = validate(oldSchema, newSchema);

            if (!failedValidators.isEmpty()) {
                return CompatValidationResult.incompatibleChange(
                        newSchema.tableName(),
                        oldSchema.schemaVersion(),
                        newSchema.schemaVersion(),
                        failedValidators
                );
            }
        }

        return CompatValidationResult.success();
    }

    private List<ForwardCompatibilityValidator> validate(FullTableSchema prevSchema, FullTableSchema nextSchema) {
        TableDefinitionDiff diff = diffCache.computeIfAbsent(
                new TableDefinitionDiffKey(prevSchema.tableId(), prevSchema.schemaVersion(), nextSchema.schemaVersion()),
                key -> nextSchema.diffFrom(prevSchema)
        );

        boolean accepted = false;
        List<ForwardCompatibilityValidator> failed = new ArrayList<>();

        for (ForwardCompatibilityValidator validator : FORWARD_COMPATIBILITY_VALIDATORS) {
            switch (validator.compatible(diff)) {
                case COMPATIBLE:
                    accepted = true;
                    break;
                case INCOMPATIBLE:
                    failed.add(validator);
                default:
                    break;
            }
        }

        assert accepted : "Table schema changed from " + prevSchema.schemaVersion() + " and " + nextSchema.schemaVersion()
                + ", but no schema change validator voted for any change. Some schema validator is missing.";

        return List.of();
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
    CompletableFuture<CompatValidationResult> validateBackwards(int tupleSchemaVersion, int tableId, UUID txId) {
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
                newSchema.tableName(),
                oldSchema.schemaVersion(),
                newSchema.schemaVersion(),
                ""
        );
    }

    void failIfSchemaChangedAfterTxStart(UUID txId, HybridTimestamp operationTimestamp, int tableId) {
        HybridTimestamp beginTs = TransactionIds.beginTimestamp(txId);
        CatalogTableDescriptor tableAtBeginTs = catalogService.table(tableId, beginTs.longValue());
        CatalogTableDescriptor tableAtOpTs = catalogService.table(tableId, operationTimestamp.longValue());

        assert tableAtBeginTs != null;

        if (tableAtOpTs == null) {
            throw tableWasDroppedException(tableId);
        }

        if (tableAtOpTs.tableVersion() != tableAtBeginTs.tableVersion()) {
            throw new IncompatibleSchemaException(
                    String.format(
                            "Table schema was updated after the transaction was started [table=%d, startSchema=%d, operationSchema=%d]",
                            tableId, tableAtBeginTs.tableVersion(), tableAtOpTs.tableVersion()
                    )
            );
        }
    }

    private static IncompatibleSchemaException tableWasDroppedException(int tableId) {
        return new IncompatibleSchemaException(String.format("Table was dropped [table=%d]", tableId));
    }

    void failIfTableDoesNotExistAt(HybridTimestamp operationTimestamp, int tableId) {
        CatalogTableDescriptor tableAtOpTs = catalogService.table(tableId, operationTimestamp.longValue());

        if (tableAtOpTs == null) {
            throw tableWasDroppedException(tableId);
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
    void failIfRequestSchemaDiffersFromTxTs(HybridTimestamp txTs, int requestSchemaVersion, int tableId) {
        CatalogTableDescriptor table = catalogService.table(tableId, txTs.longValue());

        assert table != null : "No table " + tableId + " at " + txTs;

        if (table.tableVersion() != requestSchemaVersion) {
            throw new InternalSchemaVersionMismatchException();
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
        ValidatorVerdict compatible(TableDefinitionDiff diff);
    }

    private static class RenameTableValidator implements ForwardCompatibilityValidator {
        @Override
        public ValidatorVerdict compatible(TableDefinitionDiff diff) {
            return diff.nameDiffers() ? ValidatorVerdict.INCOMPATIBLE : ValidatorVerdict.DONT_CARE;
        }
    }

    private static class AddColumnsValidator implements ForwardCompatibilityValidator {
        @Override
        public ValidatorVerdict compatible(TableDefinitionDiff diff) {
            if (diff.addedColumns().isEmpty()) {
                return ValidatorVerdict.DONT_CARE;
            }

            for (CatalogTableColumnDescriptor column : diff.addedColumns()) {
                if (!column.nullable() && column.defaultValue() == null) {
                    return ValidatorVerdict.INCOMPATIBLE;
                }
            }

            return ValidatorVerdict.COMPATIBLE;
        }
    }

    private static class DropColumnsValidator implements ForwardCompatibilityValidator {
        @Override
        public ValidatorVerdict compatible(TableDefinitionDiff diff) {
            return diff.removedColumns().isEmpty() ? ValidatorVerdict.DONT_CARE : ValidatorVerdict.INCOMPATIBLE;
        }
    }

    @SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
    private interface ColumnChangeCompatibilityValidator {
        ValidatorVerdict compatible(ColumnDefinitionDiff diff);
    }

    private static class ChangeColumnsValidator implements ForwardCompatibilityValidator {
        private static final List<ColumnChangeCompatibilityValidator> validators = List.of(
                // TODO: https://issues.apache.org/jira/browse/IGNITE-20948 - add validator that says that column rename is compatible.
                new ChangeNullabilityValidator(),
                new ChangeDefaultValueValidator(),
                new ChangeColumnTypeValidator()
        );

        @Override
        public ValidatorVerdict compatible(TableDefinitionDiff diff) {
            if (diff.changedColumns().isEmpty()) {
                return ValidatorVerdict.DONT_CARE;
            }

            boolean accepted = false;

            for (ColumnDefinitionDiff columnDiff : diff.changedColumns()) {
                switch (compatible(columnDiff)) {
                    case COMPATIBLE:
                        accepted = true;
                        break;
                    case INCOMPATIBLE:
                        return ValidatorVerdict.INCOMPATIBLE;
                    default:
                        break;
                }
            }

            assert accepted : "Table schema changed from " + diff.oldSchemaVersion() + " and " + diff.newSchemaVersion()
                    + ", but no column change validator voted for any change. Some schema validator is missing.";

            return ValidatorVerdict.COMPATIBLE;
        }

        private ValidatorVerdict compatible(ColumnDefinitionDiff columnDiff) {
            boolean accepted = false;

            for (ColumnChangeCompatibilityValidator validator : validators) {
                switch (validator.compatible(columnDiff)) {
                    case COMPATIBLE:
                        accepted = true;
                        break;
                    case INCOMPATIBLE:
                        return ValidatorVerdict.INCOMPATIBLE;
                    default:
                        break;
                }
            }

            return accepted ? ValidatorVerdict.COMPATIBLE : ValidatorVerdict.DONT_CARE;
        }
    }

    private static class ChangeDefaultValueValidator implements ColumnChangeCompatibilityValidator {
        @Override
        public ValidatorVerdict compatible(ColumnDefinitionDiff diff) {
            return diff.defaultChanged() ? ValidatorVerdict.INCOMPATIBLE : ValidatorVerdict.DONT_CARE;
        }
    }

    private static class ChangeNullabilityValidator implements ColumnChangeCompatibilityValidator {
        @Override
        public ValidatorVerdict compatible(ColumnDefinitionDiff diff) {
            if (diff.notNullAdded()) {
                return ValidatorVerdict.INCOMPATIBLE;
            }
            if (diff.notNullDropped()) {
                return ValidatorVerdict.COMPATIBLE;
            }

            assert !diff.nullabilityChanged() : diff;

            return ValidatorVerdict.DONT_CARE;
        }
    }

    private static class ChangeColumnTypeValidator implements ColumnChangeCompatibilityValidator {
        @Override
        public ValidatorVerdict compatible(ColumnDefinitionDiff diff) {
            if (!diff.typeChanged()) {
                return ValidatorVerdict.DONT_CARE;
            }

            return diff.typeChangeIsSupported() ? ValidatorVerdict.COMPATIBLE : ValidatorVerdict.INCOMPATIBLE;
        }
    }
}
