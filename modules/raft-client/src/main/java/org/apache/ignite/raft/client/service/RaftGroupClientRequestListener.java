package org.apache.ignite.raft.client.service;

import java.util.Iterator;

/**
 * A listener for raft group clien requests.
 */
public interface RaftGroupClientRequestListener {
    void onReads(Iterator iterator);

    void onWrites(Iterator iterator);
}
