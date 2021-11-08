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

package org.apache.ignite.raft.jraft.rpc.impl.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;

import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.AddPeerResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class AddPeerRequestProcessorTest extends AbstractCliRequestProcessorTest<AddPeerRequest> {

    @Override
    public AddPeerRequest createRequest(String groupId, PeerId peerId) {
        return msgFactory.addPeerRequest()
                .groupId(groupId)
                .leaderId(peerId.toString())
                .peerId("test:8181")
                .build();
    }

    @Override
    public BaseCliRequestProcessor<AddPeerRequest> newProcessor() {
        return new AddPeerRequestProcessor(null, msgFactory);
    }

    @Override
    public void verify(String interest, Node node, ArgumentCaptor<Closure> doneArg) {
        assertEquals(AddPeerRequest.class.getName(), interest);
        Mockito.verify(node).addPeer(eq(new PeerId("test", 8181)), doneArg.capture());
        Closure done = doneArg.getValue();
        assertNotNull(done);
        done.run(Status.OK());
        assertNotNull(this.asyncContext.getResponseObject());
        assertEquals("[localhost:8081, localhost:8082, localhost:8083]", this.asyncContext.as(AddPeerResponse.class)
                .oldPeersList().toString());
        assertEquals("[localhost:8081, localhost:8082, localhost:8083, test:8181]",
                this.asyncContext.as(AddPeerResponse.class).newPeersList().toString());
    }

}
