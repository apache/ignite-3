package org.apache.ignite.raft.client.message;

import org.apache.ignite.raft.client.PeerId;

public interface UserResponse<T> {
    /**
     * @return A response for this request.
     */
    T response();

    /**
     * @return A new leader, because old has stepped down.
     */
    PeerId newLeaderId();

    public interface Builder<T> {
        Builder setResponse(T response);

        Builder setNewLeaderId(PeerId peerId);

        UserResponse<T> build();
    }
}
