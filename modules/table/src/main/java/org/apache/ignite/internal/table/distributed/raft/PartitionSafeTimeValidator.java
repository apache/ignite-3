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

package org.apache.ignite.internal.table.distributed.raft;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.TableAwareCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommandBase;
import org.apache.ignite.internal.partition.replicator.schema.ValidationSchemasSource;
import org.apache.ignite.internal.partition.replicator.schemacompat.CompatValidationResult;
import org.apache.ignite.internal.partition.replicator.schemacompat.SchemaCompatibilityValidator;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.raft.jraft.option.SafeTimeValidationResult;
import org.apache.ignite.raft.jraft.option.SafeTimeValidator;

/**
 * Validator for partition commands.
 *
 * <ul>
 * <li>requests a retry for full (1PC) update commands that require metadata to be available by safe time if that metadata
 * is not yet available;</li>
 * <li>rejects full (1PC) update commands whose commitTs fails schema compatibility validation (the caller is expected to retry the
 * corresponding implicit transaction).</li>
 * </ul>
 */
public class PartitionSafeTimeValidator implements SafeTimeValidator {
    private static final IgniteLogger LOG = Loggers.forClass(PartitionSafeTimeValidator.class);

    private final SchemaCompatibilityValidator schemaCompatibilityValidator;

    public PartitionSafeTimeValidator(
            ValidationSchemasSource validationSchemasSource,
            CatalogService catalogService,
            SchemaSyncService schemaSyncService
    ) {
        schemaCompatibilityValidator = new SchemaCompatibilityValidator(validationSchemasSource, catalogService, schemaSyncService);
    }

    @Override
    public boolean shouldValidateFor(WriteCommand command) {
        return command instanceof UpdateCommandBase
                && ((UpdateCommandBase) command).full()
                && command instanceof TableAwareCommand;
    }

    @Override
    public SafeTimeValidationResult validate(String groupId, WriteCommand command, HybridTimestamp safeTime) {
        UpdateCommandBase updateCommand = (UpdateCommandBase) command;

        CompletableFuture<CompatValidationResult> future = schemaCompatibilityValidator.validateCommit(
                updateCommand.txId(),
                Set.of(((TableAwareCommand) updateCommand).tableId()),
                safeTime
        );

        if (!future.isDone()) {
            // TODO: IGNITE-20298 - throttle logging.
            String format = "Metadata not yet available by safe time, rejecting ActionRequest with EBUSY [group={}, safeTs={}].";

            LOG.warn(format, groupId, safeTime);

            return SafeTimeValidationResult.forRetry(IgniteStringFormatter.format(format, groupId, safeTime));
        }

        CompatValidationResult compatibilityValidationResult = future.join();

        if (compatibilityValidationResult.isSuccessful()) {
            return SafeTimeValidationResult.forValid();
        } else {
            return SafeTimeValidationResult.forRejected(compatibilityValidationResult.validationFailedMessage());
        }
    }
}
