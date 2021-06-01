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
package org.apache.ignite.raft.jraft.counter.rpc;

import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.counter.CounterClosure;
import org.apache.ignite.raft.jraft.counter.CounterService;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;

/**
 * IncrementAndGetRequest processor.
 */
public class IncrementAndGetRequestProcessor implements RpcProcessor<IncrementAndGetRequest> {

    private final CounterService counterService;

    public IncrementAndGetRequestProcessor(CounterService counterService) {
        super();
        this.counterService = counterService;
    }

    @Override
    public void handleRequest(final RpcContext rpcCtx, final IncrementAndGetRequest request) {
        final CounterClosure closure = new CounterClosure() {
            @Override
            public void run(Status status) {
                rpcCtx.sendResponse(getValueResponse());
            }
        };

        this.counterService.incrementAndGet(request.getDelta(), closure);
    }

    @Override
    public String interest() {
        return IncrementAndGetRequest.class.getName();
    }
}
