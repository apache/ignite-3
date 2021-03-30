package org.apache.ignite.raft.jraft.rpc;

import java.io.Serializable;

/**
 * Base message. Temporary extends Serializable for compatibility with JDK serialization.
 * TODO asch message haven't to be Serializable.
 */
public interface Message extends Serializable {
}
