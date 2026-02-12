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

import java.util.concurrent.Executor;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcContext;

/**
 * Extension of the standard {@link ActionRequestProcessor} that allows to add some interception logic.
 *
 * @see ActionRequestInterceptor
 */
public class InterceptingActionRequestProcessor extends ActionRequestProcessor {
    private final ActionRequestInterceptor interceptor;

    /** Constructor. */
    public InterceptingActionRequestProcessor(Executor executor, RaftMessagesFactory msgFactory, ActionRequestInterceptor interceptor) {
        super(executor, msgFactory);

        this.interceptor = interceptor;
    }

    @Override
    protected void handleRequestInternal(RpcContext rpcCtx, Node node, ActionRequest request, Marshaller commandsMarshaller) {
        Message interceptionResult = interceptor.intercept(rpcCtx, request, commandsMarshaller, node);

        if (interceptionResult != null) {
            rpcCtx.sendResponse(interceptionResult);
        } else {
            super.handleRequestInternal(rpcCtx, node, request, commandsMarshaller);
        }
    }
}
