package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.Peer;

public interface UserResponse<T> extends NewLeaderHint {
    /**
     * @return A response for this request.
     */
    T response();

    public interface Builder<T> {
        Builder setResponse(T response);

        Builder setNewLeaderId(Peer peer);

        UserResponse<T> build();
    }
}
