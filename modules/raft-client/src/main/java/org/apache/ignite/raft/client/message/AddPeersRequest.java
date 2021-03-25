package org.apache.ignite.raft.client.message;

import java.util.List;
import org.apache.ignite.raft.client.Peer;

/**
 * Add peers.
 */
public interface AddPeersRequest {
    /**
     * @return Group id.
     */
    String groupId();

    /**
     * @return Peers.
     */
    List<Peer> peers();

    /** */
    interface Builder {
        /**
         * @param groupId Group id.
         * @return The builder.
         */
        Builder groupId(String groupId);

        /**
         * @param peers Peers.
         * @return The builder.
         */
        Builder peers(List<Peer> peers);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        AddPeersRequest build();
    }
}
