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

import static java.util.concurrent.CompletableFuture.runAsync;
import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.rpc.CliRequests.GetLeaderRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosureAdapter;
import org.apache.ignite.raft.jraft.rpc.impl.core.NodeRequestProcessor;

/**
 * Process get leader request.
 */
public class GetLeaderRequestProcessor extends NodeRequestProcessor<GetLeaderRequest> {

    public GetLeaderRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        super(executor, msgFactory);
    }

    @Override
    protected String getPeerId(final GetLeaderRequest request) {
        return request.peerId();
    }

    @Override
    protected String getGroupId(final GetLeaderRequest request) {
        return request.groupId();
    }

    @Override
    public Message processRequest0(final RaftServerService service, final GetLeaderRequest request,
        final RpcRequestClosure done) {

        service.handleGetLeaderAndTermRequest(request, new RpcResponseClosureAdapter<>() {
            @Override
            public void run(final Status status) {
                if (getResponse() != null) {
                    runAsync(() -> done.sendResponse(getResponse()), executor());
                }
                else {
                    runAsync(() -> done.run(status), executor());
                }
            }

        });

        return null;
    }

    @Override
    public String interest() {
        return GetLeaderRequest.class.getName();
    }
}
