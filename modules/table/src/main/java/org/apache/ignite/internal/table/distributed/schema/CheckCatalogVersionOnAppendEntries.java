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

import java.nio.ByteBuffer;
import java.util.Collection;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.table.distributed.command.CatalogLevelAware;
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
import org.apache.ignite.raft.jraft.rpc.impl.AppendEntriesRequestInterceptor;
import org.apache.ignite.raft.jraft.util.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link AppendEntriesRequestInterceptor} that rejects requests (by returning EBUSY error code) if any of the
 * incoming commands requires catalog version that is not available locally yet.
 */
public class CheckCatalogVersionOnAppendEntries implements AppendEntriesRequestInterceptor {
    private static final IgniteLogger LOG = Loggers.forClass(CheckCatalogVersionOnAppendEntries.class);

    private static final int NO_LEVEL_REQUIREMENT = Integer.MIN_VALUE;

    private final CatalogService catalogService;

    public CheckCatalogVersionOnAppendEntries(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @Override
    @Nullable
    public Message intercept(
            RaftServerService service,
            AppendEntriesRequest request,
            Marshaller commandsMarshaller,
            RpcRequestClosure done
    ) {
        if (request.entriesList() == null || request.data() == null) {
            return null;
        }

        Node node = (Node) service;

        ByteBuffer allData = request.data().asReadOnlyByteBuffer();

        final Collection<EntryMeta> entriesList = request.entriesList();
        for (RaftOutter.EntryMeta entry : entriesList) {
            int requiredCatalogVersion = readRequiredCatalogVersionForMeta(allData, entry, commandsMarshaller);

            if (requiredCatalogVersion != NO_LEVEL_REQUIREMENT && !isMetadataAvailableFor(requiredCatalogVersion)) {
                LOG.warn("Metadata not yet available, group {}, required level {}.", request.groupId(), requiredCatalogVersion);
                return RaftRpcFactory.DEFAULT //
                    .newResponse(node.getRaftOptions().getRaftMessagesFactory(), RaftError.EBUSY,
                            "Metadata not yet available, group '%s', required level %d.", request.groupId(), requiredCatalogVersion);
            }
        }

        return null;
    }

    private int readRequiredCatalogVersionForMeta(ByteBuffer allData, final EntryMeta entry, Marshaller commandsMarshaller) {
        if (entry.type() != EntryType.ENTRY_TYPE_DATA) {
            return NO_LEVEL_REQUIREMENT;
        }

        long dataLen = entry.dataLen();
        if (dataLen > 0) {
            byte[] bs = new byte[(int) dataLen];
            assert allData != null;
            allData.get(bs, 0, bs.length);
            Object command = commandsMarshaller.unmarshall(ByteBuffer.wrap(bs));

            if (command instanceof CatalogLevelAware) {
                return ((CatalogLevelAware) command).requiredCatalogVersion();
            }
        }

        return NO_LEVEL_REQUIREMENT;
    }

    private boolean isMetadataAvailableFor(int catalogVersion) {
        return catalogVersion <= catalogService.latestCatalogVersion();
    }
}
