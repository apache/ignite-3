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

import static org.apache.ignite.internal.partition.replicator.marshaller.PartitionCommandsMarshaller.NO_VERSION_REQUIRED;
import static org.apache.ignite.internal.table.distributed.schema.MetadataSufficiency.isMetadataAvailableForCatalogVersion;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.marshaller.PartitionCommandsMarshaller;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
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
 * incoming commands requires catalog version that is not available locally yet.
 */
public class CheckCatalogVersionOnAppendEntries implements AppendEntriesRequestInterceptor {
    private static final IgniteLogger LOG = Loggers.forClass(CheckCatalogVersionOnAppendEntries.class);

    private final CatalogService catalogService;

    public CheckCatalogVersionOnAppendEntries(CatalogService catalogService) {
        this.catalogService = catalogService;
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
            int requiredCatalogVersion = readRequiredCatalogVersionForMeta(allData, entry, node.getOptions().getCommandsMarshaller());

            if (requiredCatalogVersion != NO_VERSION_REQUIRED
                    && !isMetadataAvailableForCatalogVersion(requiredCatalogVersion, catalogService)) {
                // TODO: IGNITE-20298 - throttle logging.
                LOG.warn(
                        "Metadata not yet available, rejecting AppendEntriesRequest with EBUSY [group={}, requiredLevel={}].",
                        request.groupId(), requiredCatalogVersion
                );

                return RaftRpcFactory.DEFAULT //
                    .newResponse(
                            node.getRaftOptions().getRaftMessagesFactory(),
                            RaftError.EBUSY,
                            "Metadata not yet available, rejecting AppendEntriesRequest with EBUSY [group=%s, requiredLevel=%d].",
                            request.groupId(), requiredCatalogVersion
                    );
            }

            offset += (int) entry.dataLen();
            allData.position(offset);
        }

        return null;
    }

    private static int readRequiredCatalogVersionForMeta(ByteBuffer allData, final EntryMeta entry, Marshaller commandsMarshaller) {
        if (entry.type() != EntryType.ENTRY_TYPE_DATA) {
            return NO_VERSION_REQUIRED;
        }

        if (!(commandsMarshaller instanceof PartitionCommandsMarshaller)) {
            return NO_VERSION_REQUIRED;
        }

        PartitionCommandsMarshaller partitionCommandsMarshaller = (PartitionCommandsMarshaller) commandsMarshaller;

        long dataLen = entry.dataLen();
        if (dataLen > 0) {
            return partitionCommandsMarshaller.readRequiredCatalogVersion(allData);
        }

        return NO_VERSION_REQUIRED;
    }
}
