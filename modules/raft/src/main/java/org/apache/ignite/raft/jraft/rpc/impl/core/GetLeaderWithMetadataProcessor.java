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

package org.apache.ignite.raft.jraft.rpc.impl.core;

import java.util.concurrent.Executor;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.GetLeaderWithMetaRequest;
import org.apache.ignite.raft.jraft.rpc.RpcResponseClosureAdapter;

/**
 * The processor handles {@link GetLeaderWithMetaRequest}.
 */
public class GetLeaderWithMetadataProcessor extends NodeRequestProcessor<GetLeaderWithMetaRequest> {
    /**
     * The constructor.
     *
     * @param executor Executor.
     * @param msgFactory Raft message factory.
     */
    public GetLeaderWithMetadataProcessor(
            Executor executor,
            RaftMessagesFactory msgFactory
    ) {
        super(executor, msgFactory);
    }

    @Override
    public String interest() {
        return GetLeaderWithMetaRequest.class.getName();
    }

    @Override
    protected Message processRequest0(
            RaftServerService serviceService,
            GetLeaderWithMetaRequest request,
            RpcRequestClosure done
    ) {
        serviceService.handleReadLeaderIndexRequest(
                request,
                new RpcResponseClosureAdapter<>() {
                    @Override
                    public void run(Status status) {
                        if (getResponse() != null) {
                            done.sendResponse(getResponse());
                        } else {
                            done.run(status);
                        }
                    }
                }
        );

        return null;
    }

    @Override
    protected String getPeerId(GetLeaderWithMetaRequest request) {
        return request.peerId();
    }

    @Override
    protected String getGroupId(GetLeaderWithMetaRequest request) {
        return request.groupId();
    }
}
