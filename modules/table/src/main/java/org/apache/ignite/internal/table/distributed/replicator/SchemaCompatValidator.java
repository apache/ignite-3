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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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

    SchemaCompatValidator(Schemas schemas) {
        this.schemas = schemas;
    }

    /**
     * Performs forward compatibility validation (if needed). That is, for each table enlisted in the transaction,
     * checks to see whether the initial schema (identified by the begin timestamp) is forward-compatible with the
     * commit schema (identified by the commit timestamp).
     *
     * <p>If doing an abort (and not commit), the validation always succeeds.
     *
     * @param txId ID of the transaction that gets validated.
     * @param enlistedGroupIds IDs of the partitions that are enlisted with the transaction.
     * @param commit Whether it's a commit attempt (otherwise it's an abort).
     * @param commitTimestamp Commit timestamp (or {@code null} if it's an abort).
     * @return Future completed with validation result.
     */
    CompletableFuture<ForwardValidationResult> validateForwards(
            UUID txId,
            List<TablePartitionId> enlistedGroupIds,
            boolean commit,
            @Nullable HybridTimestamp commitTimestamp
    ) {
        if (!commit) {
            return completedFuture(ForwardValidationResult.success());
        }

        HybridTimestamp beginTimestamp = TransactionIds.beginTimestamp(txId);

        List<UUID> tableIds = enlistedGroupIds.stream()
                .map(TablePartitionId::tableId)
                .distinct()
                .collect(toList());

        assert commitTimestamp != null;
        // Using compareTo() instead of after()/begin() because the latter methods take clock skew into account
        // which only makes sense when comparing 'unrelated' timestamps. beginTs and commitTs have a causal relationship,
        // so we don't need to account for clock skew.
        assert commitTimestamp.compareTo(beginTimestamp) > 0;

        return schemas.waitForSchemasAvailability(commitTimestamp)
                .thenApply(ignored -> {
                    for (UUID tableId : tableIds) {
                        ForwardValidationResult validationResult = validateSchemaCompatibility(beginTimestamp, commitTimestamp, tableId);
                        if (!validationResult.isSuccessful()) {
                            return validationResult;
                        }
                    }

                    return ForwardValidationResult.success();
                });
    }

    private ForwardValidationResult validateSchemaCompatibility(
            HybridTimestamp beginTimestamp,
            HybridTimestamp commitTimestamp,
            UUID tableId
    ) {
        List<FullTableSchema> tableSchemas = schemas.tableSchemaVersionsBetween(tableId, beginTimestamp, commitTimestamp);

        assert !tableSchemas.isEmpty();

        for (int i = 0; i < tableSchemas.size() - 1; i++) {
            FullTableSchema from = tableSchemas.get(i);
            FullTableSchema to = tableSchemas.get(i + 1);
            if (!isCompatible(from, to)) {
                return ForwardValidationResult.failure(tableId, from.schemaVersion(), to.schemaVersion());
            }
        }

        return ForwardValidationResult.success();
    }

    private boolean isCompatible(FullTableSchema prevSchema, FullTableSchema nextSchema) {
        TableDefinitionDiff diff = nextSchema.diffFrom(prevSchema);

        // TODO: IGNITE-19229 - more sophisticated logic
        return diff.isEmpty();
    }
}
