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
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests.ChangePeersAndLearnersResponse;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

public class ChangePeersAndLearnersRequestProcessorTest extends AbstractCliRequestProcessorTest<ChangePeersAndLearnersRequest> {
    private static final long CURRENT_TERM = 1L;

    @Override
    public ChangePeersAndLearnersRequest createRequest(String groupId, PeerId peerId) {
        return msgFactory.changePeersAndLearnersRequest()
            .groupId(groupId)
            .leaderId(peerId.toString())
            .newPeersList(List.of("localhost:8084", "localhost:8085"))
            .newLearnersList(List.of("localhost:8086", "localhost:8087"))
            .term(CURRENT_TERM)
            .sequenceToken(111L)
            .build();
    }

    @Override
    public BaseCliRequestProcessor<ChangePeersAndLearnersRequest> newProcessor() {
        return new ChangePeersAndLearnersRequestProcessor(null, msgFactory);
    }

    @Override
    public void verify(String interest, Node node, ArgumentCaptor<Closure> doneArg) {
        assertEquals(ChangePeersAndLearnersRequest.class.getName(), interest);

        Configuration configuration = JRaftUtils.getConfiguration(
                "localhost:8084,localhost:8085,localhost:8086/learner,localhost:8087/learner");
        configuration.updateSequenceToken(111L);

        Mockito.verify(node).changePeersAndLearners(
                eq(configuration),
                eq(CURRENT_TERM),
                doneArg.capture()
        );

        Closure done = doneArg.getValue();
        assertNotNull(done);
        done.run(Status.OK());
        assertNotNull(this.asyncContext.getResponseObject());
        assertEquals("[localhost:8081, localhost:8082, localhost:8083]", this.asyncContext
            .as(ChangePeersAndLearnersResponse.class).oldPeersList().toString());
        assertEquals("[localhost:8084, localhost:8085]", this.asyncContext.as(ChangePeersAndLearnersResponse.class)
            .newPeersList().toString());
        assertEquals("[learner:8081, learner:8082, learner:8083]", this.asyncContext
                .as(ChangePeersAndLearnersResponse.class).oldLearnersList().toString());
        assertEquals("[localhost:8086, localhost:8087]", this.asyncContext.as(ChangePeersAndLearnersResponse.class)
                .newLearnersList().toString());
    }

}
