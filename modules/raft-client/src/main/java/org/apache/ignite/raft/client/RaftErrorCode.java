package org.apache.ignite.raft.client;

/**
 * Error codes for raft protocol.
 */
public enum RaftErrorCode {
    /** */
    SUCCESS(1000, "Successful"),

    /** */
    NO_LEADER(1001, "No leader found within a timeout"),

    /** */
    LEADER_CHANGED(1002, "A peer is no longer a leader");

    /** */
    private final int code;

    /** */
    private final String desc;

    /**
     * @param code The code.
     * @param desc The desctiption.
     */
    RaftErrorCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    /**
     * @return The code.
     */
    public int code() {
        return code;
    }

    /**
     * @return The description.
     */
    public String description() {
        return desc;
    }
}
