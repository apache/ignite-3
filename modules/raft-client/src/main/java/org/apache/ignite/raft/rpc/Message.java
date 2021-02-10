package org.apache.ignite.raft.rpc;

import java.io.Serializable;

/**
 * The base message.
 * <p>
 * Extends Serializable for compatibility with JDK serialization.
 */
public interface Message extends Serializable {
}
