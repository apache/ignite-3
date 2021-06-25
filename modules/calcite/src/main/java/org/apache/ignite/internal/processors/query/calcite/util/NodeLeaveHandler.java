package org.apache.ignite.internal.processors.query.calcite.util;

import java.util.function.Consumer;

import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyEventHandler;

public class NodeLeaveHandler implements TopologyEventHandler {
    private final Consumer<ClusterNode> onDisappeared;

    public NodeLeaveHandler(Consumer<ClusterNode> onDisappeared) {
        this.onDisappeared = onDisappeared;
    }

    /** {@inheritDoc} */
    @Override public void onAppeared(ClusterNode member) {
        // NO-OP
    }

    /** {@inheritDoc} */
    @Override public void onDisappeared(ClusterNode member) {
        onDisappeared.accept(member);
    }
}
