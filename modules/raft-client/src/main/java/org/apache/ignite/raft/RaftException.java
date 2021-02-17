package org.apache.ignite.raft;

/** */
public class RaftException extends Exception {
    private final int statusCode;

    public RaftException(int statusCode, String statusMsg) {
        super(statusMsg);

        this.statusCode = statusCode;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
