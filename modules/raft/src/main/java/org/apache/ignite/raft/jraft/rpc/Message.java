package org.apache.ignite.raft.jraft.rpc;

import java.io.Serializable;
import org.apache.ignite.network.NetworkMessage;

/**
 * Base message. Temporary extends Serializable for compatibility with JDK serialization. TODO asch message haven't to
 * be Serializable.
 */
public interface Message extends NetworkMessage, Serializable {
    default @Override short directType() {
        return 1;
    }
}
