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
package org.apache.ignite.raft.jraft.test;

import java.util.UUID;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcContext;

/**
 * Mocked async context.
 */
public class MockAsyncContext implements RpcContext {
    private Object responseObject;
    private NodeManager nodeManager = new NodeManager(null);

    private InternalClusterNode sender = new ClusterNodeImpl(
            UUID.randomUUID(),
            "node0",
            new NetworkAddress("localhost", 12345));

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

    @Override
    public void sendResponseAsync(Object responseObj) {
        IgniteTestUtils.runAsync(() -> sendResponse(responseObject));
    }

    @Override public NetworkAddress getRemoteAddress() {
        return sender.address();
    }

    @Override
    public InternalClusterNode getSender() {
        return sender;
    }

    @Override public String getLocalConsistentId() {
        return "localhost-8081";
    }
}
