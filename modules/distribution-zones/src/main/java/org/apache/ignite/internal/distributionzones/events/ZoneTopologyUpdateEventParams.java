package org.apache.ignite.internal.distributionzones.events;

import org.apache.ignite.internal.event.CausalEventParameters;

public class ZoneTopologyUpdateEventParams extends CausalEventParameters {

    private final int zoneId;

    private final long timestamp;

    /**
     * Constructor.
     *
     * @param causalityToken Causality token.
     */
    public ZoneTopologyUpdateEventParams(long causalityToken, int zoneId, long timestamp) {
        super(causalityToken);
        this.zoneId = zoneId;
        this.timestamp = timestamp;
    }

    public int zoneId() {
        return zoneId;
    }

    public long timestamp() {
        return timestamp;
    }
}
