package org.apache.ignite.client.fakes;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.TopologyEventHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

public class FakeTopologyService implements TopologyService {
    private final InternalClusterNode node;

    public FakeTopologyService(InternalClusterNode node) {
        this.node = node;
    }

    @Override
    public InternalClusterNode localMember() {
        return node;
    }

    @Override
    public Collection<InternalClusterNode> allMembers() {
        return List.of(node);
    }

    @Override
    public Collection<InternalClusterNode> logicalTopologyMembers() {
        return List.of(node);
    }

    @Override
    public void addEventHandler(TopologyEventHandler handler) {

    }

    @Override
    public void removeEventHandler(TopologyEventHandler handler) {

    }

    @Override
    public @Nullable InternalClusterNode getByAddress(NetworkAddress addr) {
        return null;
    }

    @Override
    public @Nullable InternalClusterNode getByConsistentId(String consistentId) {
        return null;
    }

    @Override
    public @Nullable InternalClusterNode getById(UUID id) {
        return null;
    }

    @Override
    public void onJoined(InternalClusterNode node) {

    }

    @Override
    public void onLeft(InternalClusterNode node) {

    }
}
