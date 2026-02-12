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

import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.jetbrains.annotations.Nullable;

/**
 * Intercepts {@link ActionRequest}s as they come in. It might be used to handle such a request in a non-standard
 * way (like returning EBUSY under special circumstances instead of the standard behavior).
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface ActionRequestInterceptor {
    /**
     * Intercepts handling of an incoming request. If non-null message is returned, the standard handling is omitted.
     *
     * @param rpcCtx RPC context.
     * @param request Request in question.
     * @param commandsMarshaller Marshaller that can be used to deserialize command from the request, if necessary.
     * @param node JRaft node.
     * @return A message to return to the caller, or {@code null} if standard handling should be used.
     */
    @Nullable Message intercept(RpcContext rpcCtx, ActionRequest request, Marshaller commandsMarshaller, Node node);
}
