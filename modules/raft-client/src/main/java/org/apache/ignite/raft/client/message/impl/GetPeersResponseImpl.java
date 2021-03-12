/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.raft.client.message.impl;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.raft.client.PeerId;
import org.apache.ignite.raft.client.message.GetPeersResponse;

class GetPeersResponseImpl implements GetPeersResponse, GetPeersResponse.Builder {
    private List<PeerId> peersList = new ArrayList<>();

    private List<PeerId> learnersList = new ArrayList<>();

    @Override public List<PeerId> getPeersList() {
        return peersList;
    }

    @Override public List<PeerId> getLearnersList() {
        return learnersList;
    }

    @Override public Builder addPeers(PeerId peerId) {
        peersList.add(peerId);

        return this;
    }

    @Override public Builder addLearners(PeerId learnerId) {
        learnersList.add(learnerId);

        return this;
    }

    @Override public GetPeersResponse build() {
        return this;
    }
}
