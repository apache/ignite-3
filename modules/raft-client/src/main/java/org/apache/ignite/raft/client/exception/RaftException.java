package org.apache.ignite.raft.client.exception;

import org.apache.ignite.raft.client.RaftErrorCode;

/**
 * A raft exception containing code and description.
 */
public class RaftException extends RuntimeException {
    private final RaftErrorCode code;

    /**
     * @param errCode Error code.
     */
    public RaftException(RaftErrorCode errCode) {
        this.code = errCode;
    }

    /**
     * @return Error code.
     */
    public RaftErrorCode errorCode() {
        return code;
    }
}
