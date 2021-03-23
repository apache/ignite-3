package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;

public interface NewLeaderHint {
    /**
     * @return A new leader, because old is obsolete.
     */
    Peer newLeaderId();
}
