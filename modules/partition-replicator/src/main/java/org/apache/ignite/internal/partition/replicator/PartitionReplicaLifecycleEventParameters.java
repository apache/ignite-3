package org.apache.ignite.internal.partition.replicator;

import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.event.CausalEventParameters;

public class PartitionReplicaLifecycleEventParameters extends CausalEventParameters {

    private final CatalogZoneDescriptor zoneDescriptor;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     */
    public PartitionReplicaLifecycleEventParameters(long causalityToken, CatalogZoneDescriptor zoneDescriptor) {
        super(causalityToken);
        this.zoneDescriptor = zoneDescriptor;
    }

    public CatalogZoneDescriptor zoneDescriptor() {
        return zoneDescriptor;
    }

    public int partitionId() {
        throw new UnsupportedOperationException();
    }
}

