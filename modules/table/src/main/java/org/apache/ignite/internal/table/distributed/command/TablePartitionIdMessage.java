package org.apache.ignite.internal.table.distributed.command;

import java.util.UUID;
import org.apache.ignite.internal.table.distributed.TableMessageGroup.Commands;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;

@Transferable(Commands.TABLE_PARTITION_ID)
public interface TablePartitionIdMessage extends NetworkMessage {
    UUID tableId();

    int partitionId();
}
