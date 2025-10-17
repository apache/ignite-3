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

import static java.util.stream.Collectors.toList;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersResponse;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;

/**
 * Change peers request processor.
 */
public class ChangePeersAndLearnersRequestProcessor extends BaseCliRequestProcessor<ChangePeersAndLearnersRequest> {

    public ChangePeersAndLearnersRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final ChangePeersAndLearnersRequest request) {
        return request.leaderId();
    }

    @Override
    protected String getGroupId(final ChangePeersAndLearnersRequest request) {
        return request.groupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final ChangePeersAndLearnersRequest request,
        final IgniteCliRpcRequestClosure done) {
        final List<PeerId> oldPeers = ctx.node.listPeers();
        final List<PeerId> oldLearners = ctx.node.listLearners();

        long sequenceToken = request.sequenceToken() != null ? request.sequenceToken() : Configuration.NO_SEQUENCE_TOKEN;

        final Configuration conf = new Configuration(sequenceToken);
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

        LOG.info("Receive ChangePeersAndLearnersRequest with term {} to {} from {}, new conf is {}", term, ctx.node.getNodeId(),
            done.getRpcCtx().getRemoteAddress(), conf);

        ctx.node.changePeersAndLearners(conf, term, status -> {
            if (!status.isOk()) {
                done.run(status);
            }
            else {
                ChangePeersAndLearnersResponse req = msgFactory().changePeersAndLearnersResponse()
                    .oldPeersList(toStringList(oldPeers))
                    .newPeersList(toStringList(conf.getPeers()))
                    .oldLearnersList(toStringList(oldLearners))
                    .newLearnersList(toStringList(conf.getLearners()))
                    .build();

                done.sendResponse(req);
            }
        });
        return null;
    }

    private static List<String> toStringList(Collection<?> collection) {
        return collection.stream().map(Object::toString).collect(toList());
    }

    @Override
    public String interest() {
        return ChangePeersAndLearnersRequest.class.getName();
    }
}
