package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

/**
 *
 */
public interface GetPeersResponse {
    /**
     * @return Current peers.
     */
    List<Peer> peers();

    /**
     * @return Current leaners.
     */
    List<Peer> learners();

    /** */
    public interface Builder {
        /**
         * @param peers Current peers.
         * @return The builder.
         */
        Builder peers(List<Peer> peers);

        /**
         * @param learners Current learners.
         * @return The builder.
         */
        Builder learners(List<Peer> learners);

        GetPeersResponse build();
    }
}
