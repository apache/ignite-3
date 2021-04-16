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
package org.apache.ignite.raft.jraft.rpc.impl.core;

import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftServerService;
import org.apache.ignite.raft.jraft.test.MockAsyncContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(value = MockitoJUnitRunner.class)
public abstract class BaseNodeRequestProcessorTest<T extends Message> {
    @Mock(extraInterfaces = { RaftServerService.class })
    private Node               node;
    protected final String     groupId   = "test";
    protected final String     peerIdStr = "localhost:8081";
    protected MockAsyncContext asyncContext;
    protected NodeManager nodeManager = new NodeManager();

    public abstract T createRequest(String groupId, PeerId peerId);

    public abstract NodeRequestProcessor<T> newProcessor();

    public abstract void verify(String interest, RaftServerService service, NodeRequestProcessor<T> processor);

    @Before
    public void setup() {
        Mockito.lenient().when(node.getRaftOptions()).thenReturn(new RaftOptions());
    }

    @After
    public void teardown() {
        nodeManager.clear();
    }

    @Test
    public void testHandleRequest() {
        final PeerId peerId = mockNode();

        final NodeRequestProcessor<T> processor = newProcessor();
        processor.handleRequest(asyncContext, createRequest(groupId, peerId));
        verify(processor.interest(), (RaftServerService) this.node, processor);
    }

    protected PeerId mockNode() {
        Mockito.when(node.getGroupId()).thenReturn(this.groupId);
        final PeerId peerId = new PeerId();
        peerId.parse(this.peerIdStr);
        Mockito.when(node.getNodeId()).thenReturn(new NodeId(groupId, peerId));
        nodeManager.add(node);
        return peerId;
    }
}
