package org.apache.ignite.raft.rpc;

import java.io.Serializable;

public interface Node extends Serializable {
    String id();
}
