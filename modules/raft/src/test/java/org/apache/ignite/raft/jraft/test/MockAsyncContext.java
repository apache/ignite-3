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
package org.apache.ignite.raft.jraft.test;

import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcContext;

/**
 * mock alipay remoting async context
 */
public class MockAsyncContext implements RpcContext {
    private Object responseObject;
    private NodeManager nodeManager = new NodeManager();

    public Object getResponseObject() {
        return this.responseObject;
    }

    public <T extends Message> T as(Class<T> t) {
        return (T) this.responseObject;
    }

    public void setResponseObject(Object responseObject) {
        this.responseObject = responseObject;
    }

    @Override public NodeManager getNodeManager() {
        return nodeManager;
    }

    @Override public void sendResponse(Object responseObject) {
        this.responseObject = responseObject;
    }

    @Override public String getRemoteAddress() {
        return "localhost:12345";
    }

    @Override public String getLocalAddress() {
        return "localhost:8081";
    }
}
