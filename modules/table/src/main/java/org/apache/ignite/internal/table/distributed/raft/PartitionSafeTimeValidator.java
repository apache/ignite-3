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

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommandBase;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.schema.MetadataSufficiency;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.SafeTimeValidator;

/**
 * Validator that rejects update commands that require metadata to be available by safe time if that metadata is not yet available.
 */
public class PartitionSafeTimeValidator implements SafeTimeValidator {
    private static final IgniteLogger LOG = Loggers.forClass(PartitionSafeTimeValidator.class);

    private final SchemaSyncService schemaSyncService;

    public PartitionSafeTimeValidator(SchemaSyncService schemaSyncService) {
        this.schemaSyncService = schemaSyncService;
    }

    @Override
    public boolean shouldValidateFor(WriteCommand command) {
        return command instanceof UpdateCommandBase && ((UpdateCommandBase) command).full();
    }

    @Override
    public boolean isValid(HybridTimestamp safeTime) {
        return MetadataSufficiency.isMetadataAvailableForTimestamp(safeTime, schemaSyncService);
    }

    @Override
    public void logInvalidSafeTime(String groupId, HybridTimestamp safeTime) {
        // TODO: IGNITE-20298 - throttle logging.
        LOG.warn(
                "Metadata not yet available by safe time, rejecting ActionRequest with EBUSY [group={}, requiredLevel={}].",
                groupId, safeTime
        );
    }

    @Override
    public RaftError validationFailedError() {
        return RaftError.EBUSY;
    }

    @Override
    public String validationFailedErrorMessage(String groupId, HybridTimestamp safeTime) {
        return String.format(
                "Metadata not yet available by safe time, rejecting ActionRequest with EBUSY [group=%s, safeTs=%s].",
                groupId,
                safeTime
        );
    }
}
