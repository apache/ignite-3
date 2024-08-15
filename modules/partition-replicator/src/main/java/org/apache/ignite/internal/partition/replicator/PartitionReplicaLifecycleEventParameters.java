package org.apache.ignite.internal.partition.replicator;

import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.event.CausalEventParameters;

public class PartitionReplicaLifecycleEventParameters extends CausalEventParameters {

    private final CatalogZoneDescriptor zoneDescriptor;

    private final int partitionId;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     */
    public PartitionReplicaLifecycleEventParameters(long causalityToken, CatalogZoneDescriptor zoneDescriptor, int partitionId) {
        super(causalityToken);
        this.zoneDescriptor = zoneDescriptor;
        this.partitionId = partitionId;
    }

    public CatalogZoneDescriptor zoneDescriptor() {
        return zoneDescriptor;
    }

    public int partitionId() {
        return partitionId;
    }
}

