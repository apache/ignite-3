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
package org.apache.ignite.raft.jraft.rpc.impl.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;

import java.util.List;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersAsyncRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersAsyncResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ChangePeersAndLearnersAsyncRequestProcessorTest extends AbstractCliRequestProcessorTest<ChangePeersAndLearnersAsyncRequest>{
    private static final List<String> PEERS = List.of("follower1", "follower2");
    private static final List<String> LEARNERS = List.of("learner1", "learner2", "learner3");

    @Override
    public ChangePeersAndLearnersAsyncRequest createRequest(String groupId, PeerId peerId) {
        return msgFactory.changePeersAndLearnersAsyncRequest()
                .groupId(groupId)
                .leaderId(peerId.toString())
                .newPeersList(PEERS)
                .newLearnersList(LEARNERS)
                .term(1L)
                .build();
    }

    @Override
    public BaseCliRequestProcessor<ChangePeersAndLearnersAsyncRequest> newProcessor() {
        return new ChangePeersAndLearnersAsyncRequestProcessor(null, msgFactory);
    }

    @Override
    public void verify(String interest, Node node, ArgumentCaptor<Closure> doneArg) {
        assertEquals(ChangePeersAndLearnersAsyncRequest.class.getName(), interest);

        Configuration expectedConf = new Configuration();
        PEERS.stream().map(PeerId::parsePeer).forEach(expectedConf::addPeer);
        LEARNERS.stream().map(PeerId::parsePeer).forEach(expectedConf::addLearner);

        Mockito.verify(node).changePeersAndLearnersAsync(eq(expectedConf), eq(1L), doneArg.capture());
        Closure done = doneArg.getValue();
        assertNotNull(done);
        done.run(Status.OK());
        assertNotNull(this.asyncContext.getResponseObject());

        ChangePeersAndLearnersAsyncResponse response = this.asyncContext.as(ChangePeersAndLearnersAsyncResponse.class);

        assertEquals(List.of("localhost:8081", "localhost:8082", "localhost:8083"), response.oldPeersList());
        assertEquals(PEERS, response.newPeersList());
        assertEquals(List.of("learner:8081", "learner:8082", "learner:8083"), response.oldLearnersList());
        assertEquals(LEARNERS, response.newLearnersList());
    }
}
