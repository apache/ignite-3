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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.schema.FullTableSchema;
import org.apache.ignite.internal.table.distributed.schema.Schemas;
import org.apache.ignite.internal.table.distributed.schema.TableDefinitionDiff;
import org.apache.ignite.internal.tx.TransactionIds;
import org.jetbrains.annotations.Nullable;

/**
 * Validates schema compatibility.
 */
class SchemaCompatValidator {
    private final Schemas schemas;
    private final CatalogService catalogService;

    /** Constructor. */
    SchemaCompatValidator(Schemas schemas, CatalogService catalogService) {
        this.schemas = schemas;
        this.catalogService = catalogService;
    }

    /**
     * Performs commit forward compatibility validation. That is, for each table enlisted in the transaction, checks to see whether the
     * initial schema (identified by the begin timestamp) is forward-compatible with the commit schema (identified by the commit
     * timestamp).
     *
     * @param txId ID of the transaction that gets validated.
     * @param enlistedGroupIds IDs of the partitions that are enlisted with the transaction.
     * @param commitTimestamp Commit timestamp (or {@code null} if it's an abort).
     * @return Future of validation result.
     */
    CompletableFuture<CompatValidationResult> validateForward(
            UUID txId,
            Collection<TablePartitionId> enlistedGroupIds,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        HybridTimestamp beginTimestamp = TransactionIds.beginTimestamp(txId);

        Set<Integer> tableIds = enlistedGroupIds.stream()
                .map(TablePartitionId::tableId)
                .collect(toSet());

        assert commitTimestamp != null;
        // Using compareTo() instead of after()/begin() because the latter methods take clock skew into account
        // which only makes sense when comparing 'unrelated' timestamps. beginTs and commitTs have a causal relationship,
        // so we don't need to account for clock skew.
        assert commitTimestamp.compareTo(beginTimestamp) > 0;

        return schemas.waitForSchemasAvailability(commitTimestamp)
                .thenApply(ignored -> validateForwardSchemasCompatibility(tableIds, commitTimestamp, beginTimestamp));
    }

    private CompatValidationResult validateForwardSchemasCompatibility(
            Set<Integer> tableIds,
            HybridTimestamp commitTimestamp,
            HybridTimestamp beginTimestamp
    ) {
        for (int tableId : tableIds) {
            CompatValidationResult validationResult = validateForwardSchemaCompatibility(beginTimestamp, commitTimestamp, tableId);

            if (!validationResult.isSuccessful()) {
                return validationResult;
            }
        }

        return CompatValidationResult.success();
    }

    private CompatValidationResult validateForwardSchemaCompatibility(
            HybridTimestamp beginTimestamp,
            HybridTimestamp commitTimestamp,
            int tableId
    ) {
        List<FullTableSchema> tableSchemas = schemas.tableSchemaVersionsBetween(tableId, beginTimestamp, commitTimestamp);

        assert !tableSchemas.isEmpty();

        for (int i = 0; i < tableSchemas.size() - 1; i++) {
            FullTableSchema oldSchema = tableSchemas.get(i);
            FullTableSchema newSchema = tableSchemas.get(i + 1);
            if (!isForwardCompatible(oldSchema, newSchema)) {
                return CompatValidationResult.failure(tableId, oldSchema.schemaVersion(), newSchema.schemaVersion());
            }
        }

        return CompatValidationResult.success();
    }

    private boolean isForwardCompatible(FullTableSchema prevSchema, FullTableSchema nextSchema) {
        TableDefinitionDiff diff = nextSchema.diffFrom(prevSchema);

        // TODO: IGNITE-19229 - more sophisticated logic.
        return diff.isEmpty();
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

        return schemas.waitForSchemasAvailability(beginTimestamp)
                .thenCompose(ignored -> schemas.waitForSchemaAvailability(tableId, tupleSchemaVersion))
                .thenApply(ignored -> validateBackwardSchemaCompatibility(tupleSchemaVersion, tableId, beginTimestamp));
    }

    private CompatValidationResult validateBackwardSchemaCompatibility(
            int tupleSchemaVersion,
            int tableId,
            HybridTimestamp beginTimestamp
    ) {
        List<FullTableSchema> tableSchemas = schemas.tableSchemaVersionsBetween(tableId, beginTimestamp, tupleSchemaVersion);

        if (tableSchemas.isEmpty()) {
            // The tuple was not written with a future schema.
            return CompatValidationResult.success();
        }

        for (int i = 0; i < tableSchemas.size() - 1; i++) {
            FullTableSchema oldSchema = tableSchemas.get(i);
            FullTableSchema newSchema = tableSchemas.get(i + 1);
            if (!isBackwardCompatible(oldSchema, newSchema)) {
                return CompatValidationResult.failure(tableId, oldSchema.schemaVersion(), newSchema.schemaVersion());
            }
        }

        return CompatValidationResult.success();
    }

    private boolean isBackwardCompatible(FullTableSchema oldSchema, FullTableSchema newSchema) {
        // TODO: IGNITE-19229 - is backward compatibility always symmetric with the forward compatibility?
        return isForwardCompatible(newSchema, oldSchema);
    }

    void failIfSchemaChangedAfterTxStart(UUID txId, HybridTimestamp operationTimestamp, int tableId) {
        HybridTimestamp beginTs = TransactionIds.beginTimestamp(txId);
        CatalogTableDescriptor tableAtBeginTs = catalogService.table(tableId, beginTs.longValue());
        CatalogTableDescriptor tableAtOpTs = catalogService.table(tableId, operationTimestamp.longValue());

        assert tableAtBeginTs != null;
        assert tableAtOpTs != null;

        if (tableAtOpTs.tableVersion() != tableAtBeginTs.tableVersion()) {
            throw new IncompatibleSchemaException(
                    String.format(
                            "Table schema was updated after the transaction was started [table=%d, startSchema=%d, operationSchema=%d]",
                            tableId, tableAtBeginTs.tableVersion(), tableAtOpTs.tableVersion()
                    )
            );
        }
    }
}
