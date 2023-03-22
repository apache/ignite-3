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

package org.apache.ignite.raft.jraft.rpc.impl;

import org.apache.ignite.internal.raft.server.impl.RaftServiceEventInterceptor;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.CliRequests.SubscriptionLeaderChangeRequest;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;

/**
 * Election notification processor.
 */
public class NotifyElectProcessor implements RpcProcessor<SubscriptionLeaderChangeRequest> {
    /** RAFT event listener. */
    private final RaftServiceEventInterceptor serviceEventInterceptor;

    /** Message factory. */
    private final RaftMessagesFactory msgFactory;

    /**
     * The constructor.
     *
     * @param msgFactory Message factory.
     * @param serviceEventInterceptor RAFT event interceptor.
     */
    public NotifyElectProcessor(RaftMessagesFactory msgFactory, RaftServiceEventInterceptor serviceEventInterceptor) {
        this.msgFactory = msgFactory;
        this.serviceEventInterceptor = serviceEventInterceptor;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, SubscriptionLeaderChangeRequest request) {
        if (request.subscribe()) {
            serviceEventInterceptor.subscribe(request.groupId(), rpcCtx.getSender(), term ->
                    rpcCtx.sendResponseAsync(msgFactory.leaderChangeNotification()
                            .groupId(request.groupId())
                            .term(term)
                            .build()
                    ));
        } else {
            serviceEventInterceptor.unsubscribe(request.groupId(), rpcCtx.getSender());
        }

        rpcCtx.sendResponse(msgFactory.subscriptionLeaderChangeRequestAcknowledge().build());
    }

    @Override
    public String interest() {
        return SubscriptionLeaderChangeRequest.class.getName();
    }
}
