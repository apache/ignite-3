package org.apache.ignite.internal.placementdriver.message;

import org.apache.ignite.network.annotations.Transferable;

/**
 * The message is sent to placement driver when a leaseholder does not want to hold lease anymore.
 */
@Transferable(PlacementDriverMessageGroup.STOP_LEASE_PROLONGATION)
public interface StopLeaseProlongationMessage extends PlacementDriverActorMessage {
}
