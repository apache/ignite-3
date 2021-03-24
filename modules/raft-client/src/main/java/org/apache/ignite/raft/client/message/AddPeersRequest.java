package org.apache.ignite.raft.client.message;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.raft.client.Peer;

public interface AddPeersRequest {
    String groupId();

    Collection<Peer> peers();

    interface Builder {
        Builder setGroupId(String groupId);

        Builder setPeers(Collection<Peer> peers);

        AddPeersRequest build();
    }
}
