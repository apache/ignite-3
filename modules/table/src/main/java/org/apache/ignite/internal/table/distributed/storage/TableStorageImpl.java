package org.apache.ignite.internal.table.distributed.storage;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.storage.TableStorage;
import org.apache.ignite.internal.table.TableRow;
import org.apache.ignite.internal.table.distributed.TableManagerImpl;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.PutCommand;
import org.apache.ignite.internal.table.distributed.command.response.TableRowResponse;
import org.apache.ignite.lang.LogWrapper;
import org.apache.ignite.raft.client.service.RaftGroupService;

/**
 * Storage of table rows.
 */
public class TableStorageImpl implements TableStorage {
    /** Internal prefix for the metasorage. */
    public static final String INTERNAL_PREFIX = "internal.tables.";

    /** Logger. */
    private final LogWrapper log = new LogWrapper(TableManagerImpl.class);

    /** Table id. */
    private final UUID tblId;

    /** Partition map. */
    private Map<Integer, RaftGroupService> partitionMap;

    /** Partitions. */
    private int partitions;

    /**
     * @param tableId Table id.
     * @param partMap Map partition id to raft group.
     * @param partitions Partitions.
     */
    public TableStorageImpl(
        UUID tableId,
        Map<Integer, RaftGroupService> partMap,
        int partitions
    ) {
        this.tblId = tableId;
        this.partitionMap = partMap;
        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    public TableRow put(TableRow row) {
        try {
            return partitionMap.get(row.keyChunk().hashCode() % partitions).<TableRowResponse>run(new PutCommand(row)).get()
                .getValue();
        }
        catch (InterruptedException | ExecutionException e) {
            log.error("Failed to put some value [tblId={}, row={}]", tblId, row);
        }

        //TODO: Throw exception.
        return null;
    }

    /** {@inheritDoc} */
    public TableRow get(TableRow keyRow) {
        try {
            return partitionMap.get(keyRow.keyChunk().hashCode() % partitions).<TableRowResponse>run(new GetCommand(keyRow)).get()
                .getValue();
        }
        catch (InterruptedException | ExecutionException e) {
            log.error("Failed to get some value [tblId={}, keyRow={}]", tblId, keyRow);
        }

        //TODO: Throw exception.
        return null;
    }
}
