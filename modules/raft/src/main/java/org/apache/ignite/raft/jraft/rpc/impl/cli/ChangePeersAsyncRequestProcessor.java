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
package org.apache.ignite.raft.jraft.rpc.impl.cli;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAsyncRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAsyncResponse;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;

import static java.util.stream.Collectors.toList;

/**
 * Change peers request processor.
 */
public class ChangePeersAsyncRequestProcessor extends BaseCliRequestProcessor<ChangePeersAsyncRequest> {

    public ChangePeersAsyncRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final ChangePeersAsyncRequest request) {
        return request.leaderId();
    }

    @Override
    protected String getGroupId(final ChangePeersAsyncRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final ChangePeersAsyncRequest request,
            final IgniteCliRpcRequestClosure done) {
        final List<PeerId> oldPeers = ctx.node.listPeers();
        final List<PeerId> oldLearners = ctx.node.listLearners();

        final Configuration conf = new Configuration();
        for (final String peerIdStr : request.newPeersList()) {
            final PeerId peer = new PeerId();
            if (peer.parse(peerIdStr)) {
                conf.addPeer(peer);
            }
            else {
                return RaftRpcFactory.DEFAULT //
                        .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse peer id %s", peerIdStr);
            }
        }

        for (final String learnerIdStr : request.newLearnersList()) {
            final PeerId learner = new PeerId();
            if (learner.parse(learnerIdStr)) {
                conf.addLearner(learner);
            }
            else {
                return RaftRpcFactory.DEFAULT //
                        .newResponse(msgFactory(), RaftError.EINVAL, "Fail to parse learner id %s", learnerIdStr);
            }
        }

        long term = request.term();

        LOG.info("Receive ChangePeersAsyncRequest with term {} to {} from {}, new conf is {}", term, ctx.node.getNodeId(), done.getRpcCtx()
                .getRemoteAddress(), conf);

        ctx.node.changePeersAsync(conf, term, status -> {
            if (!status.isOk()) {
                done.run(status);
            }
            else {
                ChangePeersAsyncResponse resp = msgFactory().changePeersAsyncResponse()
                        .oldPeersList(toStringList(oldPeers))
                        .newPeersList(toStringList(conf.getPeers()))
                        .oldLearnersList(toStringList(oldLearners))
                        .newLearnersList(toStringList(conf.getLearners()))
                        .build();

                done.sendResponse(resp);
            }
        });
        return null;
    }

    private static List<String> toStringList(Collection<?> collection) {
        return collection.stream().map(Object::toString).collect(toList());
    }

    @Override
    public String interest() {
        return ChangePeersAsyncRequest.class.getName();
    }
}
