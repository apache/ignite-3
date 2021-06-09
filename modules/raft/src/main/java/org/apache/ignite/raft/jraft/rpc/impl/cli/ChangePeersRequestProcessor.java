/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.rpc.impl.cli;

import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersResponse;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;

/**
 * Change peers request processor.
 */
public class ChangePeersRequestProcessor extends BaseCliRequestProcessor<ChangePeersRequest> {

    public ChangePeersRequestProcessor(Executor executor) {
        super(executor, ChangePeersResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final ChangePeersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final ChangePeersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final ChangePeersRequest request,
        final RpcRequestClosure done) {
        final List<PeerId> oldConf = ctx.node.listPeers();

        final Configuration conf = new Configuration();
        for (final String peerIdStr : request.getNewPeersList()) {
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                conf.addPeer(peer);
            }
            else {
                return RaftRpcFactory.DEFAULT //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }
        LOG.info("Receive ChangePeersRequest to {} from {}, new conf is {}", ctx.node.getNodeId(), done.getRpcCtx()
            .getRemoteAddress(), conf);
        ctx.node.changePeers(conf, status -> {
            if (!status.isOk()) {
                done.run(status);
            }
            else {
                ChangePeersResponse.Builder rb = ChangePeersResponse.newBuilder();
                for (final PeerId peer : oldConf) {
                    rb.addOldPeers(peer.toString());
                }
                for (final PeerId peer : conf) {
                    rb.addNewPeers(peer.toString());
                }
                done.sendResponse(rb.build());
            }
        });
        return null;
    }

    @Override
    public String interest() {
        return ChangePeersRequest.class.getName();
    }
}
