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

package org.apache.ignite.raft.jraft.rpc.impl.core;

import java.util.ArrayList;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.CoalescedHeartbeatResponseBuilder;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.CoalescedHeartbeatRequest;

/**
 * Heartbeat request processor.
 */
public class HeartbeatRequestProcessor extends RpcRequestProcessor<CoalescedHeartbeatRequest> {

    /**
     * Constructor.
     *
     * @param executor Executor.
     * @param msgFactory Message factory.
     */
    public HeartbeatRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    public Message processRequest(CoalescedHeartbeatRequest request, RpcRequestClosure done) {
        CoalescedHeartbeatResponseBuilder builder = msgFactory().coalescedHeartbeatResponse();
        builder.messages(new ArrayList<>());

        for (AppendEntriesRequest message : request.messages()) {
            PeerId peerId = PeerId.parsePeer(message.peerId());

            Message msg;

            if (peerId == null) {
                msg = RaftRpcFactory.DEFAULT.newResponse(
                        msgFactory(),
                        RaftError.EINVAL,
                        "Fail to parse peerId: %s",
                        message.peerId());
            } else {
                Node node = done.getRpcCtx().getNodeManager().get(message.groupId(), peerId);

                if (node == null) {
                    msg = RaftRpcFactory.DEFAULT.newResponse(
                            msgFactory(),
                            RaftError.ENOENT, "Peer id not found: %s, group: %s", message.peerId(),
                            message.groupId());
                } else {
                    RaftServerService svc = (RaftServerService) node;

                    msg = svc.handleAppendEntriesRequest(message, null);
                }
            }

            builder.messages().add(msg);
        }

        return builder.build();
    }

    @Override
    public String interest() {
        return CoalescedHeartbeatRequest.class.getName();
    }
}
