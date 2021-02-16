package org.apache.ignite.raft.rpc;

import java.io.Serializable;

/**
 * TODO FIXME asch must be elsewhere.
 */
public interface Node extends Serializable {
    String id();
}
