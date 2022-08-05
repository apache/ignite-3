package org.apache.ignite.internal.table.distributed.replicator.action;

import java.util.UUID;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.distributed.replicator.action.container.SingleEntryContainer;

public class PartitionTwoAction extends PartitionSingleAction {
    /**
     * Entry container.
     */
    private final SingleEntryContainer secondContainer;

    /**
     * The constructor.
     *
     * @param type       Action type.
     * @param txId       Transaction id.
     * @param timestamp  Transaction timestamp.
     * @param indexToUse Index id.
     * @param container  Entry container.
     */
    public PartitionTwoAction(
            RequestType type,
            UUID txId,
            HybridTimestamp timestamp,
            UUID indexToUse,
            SingleEntryContainer container,
            SingleEntryContainer secondContainer) {
        super(type, txId, timestamp, indexToUse, container);

        this.secondContainer = secondContainer;
    }

    /**
     * Gets a second entry container.
     *
     * @return Entry container.
     */
    public SingleEntryContainer secondContainer() {
        return secondContainer;
    }
}
