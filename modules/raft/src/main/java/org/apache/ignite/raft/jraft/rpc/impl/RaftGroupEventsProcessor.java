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

import org.apache.ignite.raft.jraft.rpc.CliRequests.LeaderChangeNotification;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;

public class RaftGroupEventsProcessor implements RpcProcessor<LeaderChangeNotification> {
    private final RaftGroupEventsClientListener eventsClientListener;

    public RaftGroupEventsProcessor(RaftGroupEventsClientListener eventsClientListener) {
        this.eventsClientListener = eventsClientListener;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, LeaderChangeNotification request) {
        eventsClientListener.onLeaderElected(request.groupId(), rpcCtx.getSender(), request.term());
    }

    @Override
    public String interest() {
        return LeaderChangeNotification.class.getName();
    }
}
