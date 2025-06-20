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
package org.apache.ignite.raft.jraft.rpc;

import java.util.concurrent.Executor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.core.NotLeaderException;

/**
 * Abstract AsyncUserProcessor for RPC processors.
 *
 * @param <T> Message
 */
public abstract class RpcRequestProcessor<T extends Message> implements RpcProcessor<T> {
    /**
     * The logger.
     */
    protected static final IgniteLogger LOG = Loggers.forClass(RpcRequestProcessor.class);

    private final Executor executor;

    private final RaftMessagesFactory msgFactory;

    public abstract Message processRequest(final T request, final RpcRequestClosure done);

    public RpcRequestProcessor(Executor executor, RaftMessagesFactory msgFactory) {
        this.executor = executor;
        this.msgFactory = msgFactory;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final T request) {
        try {
            final Message msg = processRequest(request, new RpcRequestClosure(rpcCtx, msgFactory));

            if (msg != null) {
                rpcCtx.sendResponse(msg);
            }
        }
        catch (final Throwable t) {
            if (isIgnorable(t)) {
                LOG.debug("handleRequest {} failed", t, request);
            } else {
                LOG.error("handleRequest {} failed", t, request);
            }
            rpcCtx.sendResponse(RaftRpcFactory.DEFAULT //
                .newResponse(msgFactory, -1, "handleRequest internal error"));
        }
    }

    private static boolean isIgnorable(Throwable t) {
        // It is ok if we lost leadership while a request to us was in flight, there is no need to clutter up the log.
        return t instanceof NotLeaderException;
    }

    @Override
    public Executor executor() {
        return this.executor;
    }

    public RaftMessagesFactory msgFactory() {
        return msgFactory;
    }
}
