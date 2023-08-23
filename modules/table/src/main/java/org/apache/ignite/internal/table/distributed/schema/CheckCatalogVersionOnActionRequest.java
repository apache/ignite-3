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

import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.table.distributed.command.CatalogVersionAware;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.impl.ActionRequestInterceptor;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link ActionRequestInterceptor} that rejects requests (by returning EBUSY error code) if the incoming command
 * requires catalog version that is not available locally yet.
 */
public class CheckCatalogVersionOnActionRequest implements ActionRequestInterceptor {
    private static final IgniteLogger LOG = Loggers.forClass(CheckCatalogVersionOnActionRequest.class);

    private final CatalogService catalogService;

    public CheckCatalogVersionOnActionRequest(CatalogService catalogService) {
        this.catalogService = catalogService;
    }

    @Override
    public @Nullable Message intercept(RpcContext rpcCtx, ActionRequest request) {
        Node node = rpcCtx.getNodeManager().get(request.groupId(), new PeerId(rpcCtx.getLocalConsistentId()));

        Command command = request.command();

        if (command instanceof CatalogVersionAware) {
            int requiredCatalogVersion = ((CatalogVersionAware) command).requiredCatalogVersion();

            if (!isMetadataAvailableFor(requiredCatalogVersion)) {
                // TODO: IGNITE-20298 - throttle logging.
                LOG.warn(
                        "Metadata not yet available, group {}, required level {}; rejecting ActionRequest with EBUSY.",
                        request.groupId(), requiredCatalogVersion
                );

                return RaftRpcFactory.DEFAULT //
                    .newResponse(
                            node.getRaftOptions().getRaftMessagesFactory(),
                            RaftError.EBUSY,
                            "Metadata not yet available, group '%s', required level %d; rejecting ActionRequest with EBUSY.",
                            request.groupId(), requiredCatalogVersion
                    );
            }
        }

        return null;
    }

    private boolean isMetadataAvailableFor(int requiredCatalogVersion) {
        return requiredCatalogVersion <= catalogService.latestCatalogVersion();
    }
}
