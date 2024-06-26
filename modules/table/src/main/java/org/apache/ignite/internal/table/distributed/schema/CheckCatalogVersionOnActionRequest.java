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

import static org.apache.ignite.internal.table.distributed.schema.CatalogVersionSufficiency.isMetadataAvailableFor;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.State;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
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
    public @Nullable Message intercept(RpcContext rpcCtx, ActionRequest request, Marshaller commandsMarshaller) {
        Node node = rpcCtx.getNodeManager().get(request.groupId(), new PeerId(rpcCtx.getLocalConsistentId()));

        if (node == null) {
            return Loza.FACTORY.errorResponse().errorCode(RaftError.UNKNOWN.getNumber()).build();
        }

        Message errorIfNotLeader = errorResponseIfNotLeader(node);
        if (errorIfNotLeader != null) {
            return errorIfNotLeader;
        }

        if (!(commandsMarshaller instanceof PartitionCommandsMarshaller)) {
            return null;
        }

        if (!(request instanceof WriteActionRequest)) {
            return null;
        }

        ByteBuffer command = ((WriteActionRequest) request).command();

        var partitionCommandsMarshaller = (PartitionCommandsMarshaller) commandsMarshaller;

        int requiredCatalogVersion = partitionCommandsMarshaller.readRequiredCatalogVersion(command);

        if (requiredCatalogVersion >= 0) {
            if (!isMetadataAvailableFor(requiredCatalogVersion, catalogService)) {
                // TODO: IGNITE-20298 - throttle logging.
                LOG.warn(
                        "Metadata not yet available, rejecting ActionRequest with EBUSY [group={}, requiredLevel={}].",
                        request.groupId(), requiredCatalogVersion
                );

                return RaftRpcFactory.DEFAULT //
                    .newResponse(
                            node.getRaftOptions().getRaftMessagesFactory(),
                            RaftError.EBUSY,
                            "Metadata not yet available, rejecting ActionRequest with EBUSY [group=%s, requiredLevel=%d].",
                            request.groupId(), requiredCatalogVersion
                    );
            }
        }

        return null;
    }

    /**
     * Builds an {@link org.apache.ignite.raft.jraft.rpc.RpcRequests.ErrorResponse} if the current node is not a leader.
     * It tries to mirror the behavior of {@link NodeImpl} in this regard.
     *
     * <p>NB that it's important to return leaderId for the 'not a leader and not transferring leadership' state
     * so that the caller switches to the actual leader.
     *
     * @param node The node.
     * @return ErrorResponse if this node is not a leader, {@code null} otherwise.
     */
    private static @Nullable Message errorResponseIfNotLeader(Node node) {
        State state = node.getNodeState();

        if (state == State.STATE_LEADER) {
            return null;
        }

        Status st = NodeImpl.cannotApplyBecauseNotLeaderStatus(state);

        LOG.debug("Node {} can't apply, status={}.", node.getNodeId(), st);

        PeerId leaderId = node.getLeaderId();

        // We only return leaderId in case of EPERM (which means that we're not the leader) AND we know the actual leaderId.
        boolean returnLeaderId = st.getRaftError() == RaftError.EPERM && leaderId != null;

        return RaftRpcFactory.DEFAULT.newResponse(
                returnLeaderId ? leaderId.toString() : null,
                node.getRaftOptions().getRaftMessagesFactory(),
                st.getRaftError(),
                st.getErrorMsg()
        );
    }
}
