package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.jetbrains.annotations.Nullable;

/**
 * Raft error response. Also used as a default response when errorCode == {@link RaftErrorCode#SUCCESS}
 */
public interface RaftErrorResponse {
    /**
     * @return Error code.
     */
    public RaftErrorCode errorCode();

    /**
     * @return The new leader if a current leader is obsolete or null if not applicable.
     */
    public @Nullable Peer newLeader();

    /** */
    public interface Builder {
        /**
         * @param errorCode Error code.
         * @return The builder.
         */
        Builder errorCode(RaftErrorCode errorCode);

        /**
         * @param newLeader New leader.
         * @return The builder.
         */
        Builder newLeader(Peer newLeader);

        /**
         * @return The complete message.
         * @throws IllegalStateException If the message is not in valid state.
         */
        RaftErrorResponse build();
    }
}
