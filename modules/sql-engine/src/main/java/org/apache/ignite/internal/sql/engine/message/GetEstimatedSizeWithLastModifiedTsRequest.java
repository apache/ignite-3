package org.apache.ignite.internal.sql.engine.message;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

@Transferable(SqlQueryMessageGroup.GET_ESTIMATED_SIZE_WITH_MODIFIED_TS_MESSAGE)
public interface GetEstimatedSizeWithLastModifiedTsRequest extends NetworkMessage {
    /** ID of the table. */
    int tableId();
}
