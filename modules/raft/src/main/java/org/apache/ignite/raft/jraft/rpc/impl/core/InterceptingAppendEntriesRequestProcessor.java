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
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.rpc.RpcRequestClosure;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.AppendEntriesRequest;

/**
 * Extension of the standard {@link AppendEntriesRequestProcessor} that allows to add some interception logic.
 *
 * @see AppendEntriesRequestInterceptor
 */
public class InterceptingAppendEntriesRequestProcessor extends AppendEntriesRequestProcessor {
    private final AppendEntriesRequestInterceptor interceptor;

    /**
     * Constructor.
     */
    public InterceptingAppendEntriesRequestProcessor(Executor executor, RaftMessagesFactory msgFactory,
            AppendEntriesRequestInterceptor interceptor) {
        super(executor, msgFactory);

        this.interceptor = interceptor;
    }

    @Override
    public Message processRequest0(RaftServerService service, AppendEntriesRequest request, RpcRequestClosure done) {
        Message interceptionResult = interceptor.intercept(service, request,  done);

        if (interceptionResult != null) {
            return interceptionResult;
        }

        return super.processRequest0(service, request, done);
    }
}
