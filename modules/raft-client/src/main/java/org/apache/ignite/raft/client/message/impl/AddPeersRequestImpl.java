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
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.AddPeersRequest;

class AddPeersRequestImpl implements AddPeersRequest, AddPeersRequest.Builder {
    private String groupId;

    private List<Peer> peersList = new ArrayList<>();

    @Override public String getGroupId() {
        return groupId;
    }

    @Override public List<Peer> getPeersList() {
        return peersList;
    }

    @Override public Builder setGroupId(String groupId) {
        this.groupId = groupId;

        return this;
    }

    @Override public Builder addPeer(Peer peer) {
        if (!this.peersList.contains(peer))
            this.peersList.add(peer);

        return this;
    }

    @Override public AddPeersRequest build() {
        return this;
    }
}
