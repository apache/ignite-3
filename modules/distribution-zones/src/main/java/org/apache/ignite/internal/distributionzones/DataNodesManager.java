package org.apache.ignite.internal.distributionzones;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.MetaStorageManager;

public class DataNodesManager implements LogicalTopologyEventListener {
    private final MetaStorageManager metaStorageManager;

    public DataNodesManager(MetaStorageManager metaStorageManager) {
        this.metaStorageManager = metaStorageManager;
    }

    @Override
    public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {

    }

    @Override
    public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {

    }

    @Override
    public void onTopologyLeap(LogicalTopologySnapshot newTopology) {

    }

    public void onFilterChanged() {

    }

    public void onAutoAdjustAlteration() {

    }

    public CompletableFuture<Set<String>> dataNodes(int zoneId, HybridTimestamp timestamp) {

    }
}
