package org.apache.ignite.raft.client;

/**
 * Error codes for raft protocol.
 */
public enum RaftErrorCode {
    NO_LEADER(0, "No leader found within a timeout"),

    LEADER_CHANGED(1, "A peer is no longer a leader");

    /** */
    private final int code;

    /** */
    private final String desc;

    /**
     * @param code Code.
     * @param desc Desctiption.
     */
    RaftErrorCode(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    /**
     * @return Code.
     */
    public int code() {
        return code;
    }

    /**
     * @return Description.
     */
    public String description() {
        return desc;
    }
}
