package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

/**
 * Change peers result.
 */
public interface ChangePeersResponse {
    /**
     * @return Old peers.
     */
    List<Peer> oldPeers();

    /**
     * @return New peers.
     */
    List<Peer> newPeers();

    public interface Builder {
        /**
         * @param oldPeers Old peers.
         * @return The builder.
         */
        Builder oldPeers(List<Peer> oldPeers);

        /**
         * @param newPeers New peers.
         * @return The builder.
         */
        Builder newPeers(List<Peer> newPeers);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        ChangePeersResponse build();
    }
}
