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

package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.PeerId;

/**
 * Raft client messages.
 */
public final class RaftClientMessages {
    private RaftClientMessages() {
    }

    public interface AddPeerRequest {
        PeerId getPeerId();

        interface Builder {
            Builder setGroupId(String groupId);

            Builder setPeerId(PeerId peerId);

            AddPeerRequest build();
        }
    }

    public interface AddPeerResponse {
        List<PeerId> getOldPeersList();

        List<PeerId> getNewPeersList();

        public interface Builder {
            Builder addOldPeers(PeerId oldPeersId);

            Builder addNewPeers(PeerId newPeersId);

            AddPeerResponse build();
        }
    }

    public interface RemovePeerRequest {
        PeerId getPeerId();

        interface Builder {
            Builder setGroupId(String groupId);

            Builder setPeerId(PeerId peerId);

            RemovePeerRequest build();
        }
    }

    public interface RemovePeerResponse {
        List<PeerId> getOldPeersList();

        List<PeerId> getNewPeersList();

        public interface Builder {
            Builder addOldPeers(PeerId oldPeerId);

            Builder addNewPeers(PeerId newPeerId);

            RemovePeerResponse build();
        }
    }

    public interface ChangePeersRequest {
        List<PeerId> getNewPeersList();

        public interface Builder {
            Builder setGroupId(String groupId);

            Builder addNewPeers(PeerId peerId);

            ChangePeersRequest build();
        }
    }

    public interface ChangePeersResponse {
        List<PeerId> getOldPeersList();

        List<PeerId> getNewPeersList();

        public interface Builder {
            Builder addOldPeers(PeerId oldPeerId);

            Builder addNewPeers(PeerId newPeerId);

            ChangePeersResponse build();
        }
    }

    public interface SnapshotRequest {
        String getGroupId();

        public interface Builder {
            Builder setGroupId(String groupId);

            SnapshotRequest build();
        }
    }

    public interface ResetPeerRequest {
        List<PeerId> getNewPeersList();

        public interface Builder {
            Builder setGroupId(String groupId);

            Builder addNewPeers(PeerId peerId);

            ResetPeerRequest build();
        }
    }

    public interface TransferLeaderRequest {
        String getGroupId();

        PeerId getPeerId();

        public interface Builder {
            Builder setGroupId(String groupId);

            TransferLeaderRequest build();
        }
    }

    public interface GetLeaderRequest {
        String getGroupId();

        public interface Builder {
            Builder setGroupId(String groupId);

            GetLeaderRequest build();
        }
    }

    public interface GetLeaderResponse {
        PeerId getLeaderId();

        public interface Builder {
            GetLeaderResponse build();

            Builder setLeaderId(PeerId leaderId);
        }
    }

    public interface GetPeersRequest {
        boolean getOnlyAlive();

        public interface Builder {
            Builder setGroupId(String groupId);

            Builder setOnlyAlive(boolean onlyGetAlive);

            GetPeersRequest build();
        }
    }

    public interface GetPeersResponse {
        List<PeerId> getPeersList();

        List<PeerId> getLearnersList();

        public interface Builder {
            Builder addPeers(PeerId peerId);

            Builder addLearners(PeerId learnerId);

            GetPeersResponse build();
        }
    }

    public interface AddLearnersRequest {
        List<PeerId> getLearnersList();

        public interface Builder {
            Builder setGroupId(String groupId);

            Builder addLearners(PeerId learnerId);

            AddLearnersRequest build();
        }
    }

    public interface RemoveLearnersRequest {
        List<PeerId> getLearnersList();

        public interface Builder {
            Builder setGroupId(String groupId);

            Builder addLearners(PeerId leaderId);

            RemoveLearnersRequest build();
        }
    }

    public interface ResetLearnersRequest {
        List<PeerId> getLearnersList();

        public interface Builder {
            Builder setGroupId(String groupId);

            Builder addLearners(PeerId learnerId);

            ResetLearnersRequest build();
        }
    }

    public interface LearnersOpResponse {
        List<PeerId> getOldLearnersList();

        List<PeerId> getNewLearnersList();

        public interface Builder {
            Builder addOldLearners(PeerId oldLearnersId);

            Builder addNewLearners(PeerId newLearnersId);

            LearnersOpResponse build();
        }
    }

    public interface UserRequest {
        Object request();

        String getGroupId();

        public interface Builder {
            Builder setRequest(Object request);

            Builder setGroupId(String groupId);

            UserRequest build();
        }
    }

    public interface UserResponse<T> {
        T response();

        public interface Builder<T> {
            Builder setResponse(T response);

            UserResponse<T> build();
        }
    }
}
