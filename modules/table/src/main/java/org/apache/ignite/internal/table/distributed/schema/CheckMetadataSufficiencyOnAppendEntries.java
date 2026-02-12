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

package org.apache.ignite.internal.table.distributed.schema;

import static org.apache.ignite.internal.hlc.HybridTimestamp.nullableHybridTimestamp;
import static org.apache.ignite.internal.partition.replicator.marshaller.PartitionCommandsMarshaller.NO_VERSION_REQUIRED;
import static org.apache.ignite.internal.table.distributed.schema.MetadataSufficiency.isMetadataAvailableForCatalogVersion;
import static org.apache.ignite.internal.table.distributed.schema.MetadataSufficiency.isMetadataAvailableForTimestamp;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.marshaller.PartitionCommandsMarshaller;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.entity.EnumOutter.EntryType;
import org.apache.ignite.raft.jraft.entity.RaftOutter;
import org.apache.ignite.raft.jraft.entity.RaftOutter.EntryMeta;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestInterceptor;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link AppendEntriesRequestInterceptor} that rejects requests (by returning EBUSY error code) if any of the
 * incoming commands requires catalog version that is not available locally yet or if a schema sync on the safe time
 * the command carries would require to wait to get schema for that timestamp.
 */
public class CheckMetadataSufficiencyOnAppendEntries implements AppendEntriesRequestInterceptor {
    private static final IgniteLogger LOG = Loggers.forClass(CheckMetadataSufficiencyOnAppendEntries.class);

    private final CatalogService catalogService;
    private final SchemaSyncService schemaSyncService;

    public CheckMetadataSufficiencyOnAppendEntries(CatalogService catalogService, SchemaSyncService schemaSyncService) {
        this.catalogService = catalogService;
        this.schemaSyncService = schemaSyncService;
    }

    @Override
    public @Nullable Message intercept(RaftServerService service, AppendEntriesRequest request, RpcRequestClosure done) {
        if (request.entriesList() == null || request.data() == null) {
            return null;
        }

        Node node = (Node) service;

        ByteBuffer allData = request.data().asReadOnlyBuffer().order(OptimizedMarshaller.ORDER);
        int offset = 0;

        for (RaftOutter.EntryMeta entry : request.entriesList()) {
            DataEntryMetadata entryMetadata = readDataEntryMetadata(allData, entry, node.getOptions().getCommandsMarshaller());

            if (entryMetadata.requiredCatalogVersion != NO_VERSION_REQUIRED
                    && !isMetadataAvailableForCatalogVersion(entryMetadata.requiredCatalogVersion, catalogService)) {
                // TODO: IGNITE-20298 - throttle logging.
                LOG.warn(
                        "Metadata not yet available by catalog version, rejecting AppendEntriesRequest with EBUSY "
                                + "[group={}, requiredLevel={}].",
                        request.groupId(), entryMetadata.requiredCatalogVersion
                );

                return busyResponse(
                        node,
                        "Metadata not yet available by catalog version, rejecting AppendEntriesRequest with EBUSY "
                                + "[group=%s, requiredLevel=%d].",
                        request.groupId(),
                        entryMetadata.requiredCatalogVersion
                );
            }

            if (entryMetadata.safeTime != null
                    && !isMetadataAvailableForTimestamp(entryMetadata.safeTime, schemaSyncService)) {
                // TODO: IGNITE-20298 - throttle logging.
                LOG.warn(
                        "Metadata not yet available by safe time, rejecting ActionRequest with EBUSY [group={}, safeTime={}].",
                        request.groupId(), entryMetadata.safeTime
                );

                return busyResponse(
                        node,
                        "Metadata not yet available by safe time, rejecting AppendEntriesRequest with EBUSY [group=%s, safeTime=%s].",
                        request.groupId(),
                        entryMetadata.safeTime
                );
            }

            offset += (int) entry.dataLen();
            allData.position(offset);
        }

        return null;
    }

    private static DataEntryMetadata readDataEntryMetadata(ByteBuffer allData, EntryMeta entry, Marshaller commandsMarshaller) {
        if (entry.type() != EntryType.ENTRY_TYPE_DATA) {
            return DataEntryMetadata.EMPTY;
        }

        if (!(commandsMarshaller instanceof PartitionCommandsMarshaller)) {
            return DataEntryMetadata.EMPTY;
        }

        PartitionCommandsMarshaller partitionCommandsMarshaller = (PartitionCommandsMarshaller) commandsMarshaller;

        long dataLen = entry.dataLen();
        if (dataLen > 0) {
            int requiredCatalogVersion = partitionCommandsMarshaller.readRequiredCatalogVersion(allData);
            long safeTime = partitionCommandsMarshaller.readSafeTimestamp(allData);
            return new DataEntryMetadata(requiredCatalogVersion, safeTime);
        }

        return DataEntryMetadata.EMPTY;
    }

    private static Message busyResponse(Node node, String message, Object... args) {
        return RaftRpcFactory.DEFAULT //
                .newResponse(
                        node.getRaftOptions().getRaftMessagesFactory(),
                        RaftError.EBUSY,
                        message,
                        args
                );
    }

    private static class DataEntryMetadata {
        private final int requiredCatalogVersion;
        private final @Nullable HybridTimestamp safeTime;

        private static final DataEntryMetadata EMPTY = new DataEntryMetadata(NO_VERSION_REQUIRED, 0);

        private DataEntryMetadata(int requiredCatalogVersion, long safeTime) {
            this.requiredCatalogVersion = requiredCatalogVersion;
            this.safeTime = nullableHybridTimestamp(safeTime);
        }
    }
}
