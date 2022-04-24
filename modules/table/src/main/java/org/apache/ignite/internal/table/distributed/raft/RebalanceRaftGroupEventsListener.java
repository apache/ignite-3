package org.apache.ignite.internal.table.distributed.raft;

import static org.apache.ignite.internal.metastorage.client.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.client.Conditions.revision;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.metastorage.client.Operations.put;
import static org.apache.ignite.internal.metastorage.client.Operations.remove;
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsPendingKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsPlannedKey;
import static org.apache.ignite.internal.utils.RebalanceUtil.partAssignmentsStableKey;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.configuration.schema.ExtendedTableChange;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.raft.server.RaftGroupEventsListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.entity.PeerId;

/**
 * Listener for the raft group events, which must provide correct error handling of rebalance process
 * and start new rebalance after the current one finished.
 */
public class RebalanceRaftGroupEventsListener implements RaftGroupEventsListener {

    /** Meta storage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Table configuration instance. */
    private final TableConfiguration tblConfiguration;

    /** Unique partition id. */
    private final String partId;

    /** Partition number. */
    private final int partNum;

    /**
     * Constructs new listener.
     *
     * @param metaStorageMgr Meta storage manager.
     * @param tblConfiguration Table configuration.
     * @param partId Partition id.
     * @param partNum Partition number.
     */
    public RebalanceRaftGroupEventsListener(MetaStorageManager metaStorageMgr, TableConfiguration tblConfiguration, String partId,
            int partNum) {
        this.metaStorageMgr = metaStorageMgr;
        this.tblConfiguration = tblConfiguration;
        this.partId = partId;
        this.partNum = partNum;
    }

    /** {@inheritDoc} */
    @Override
    public void onLeaderElected() {
    }

    /** {@inheritDoc} */
    @Override
    public void onNewPeersConfigurationApplied(List<PeerId> peers) {
        Map<ByteArray, Entry> keys = metaStorageMgr.getAll(
                Set.of(partAssignmentsPlannedKey(partId), partAssignmentsPendingKey(partId))).join();

        Entry plannedEntry = keys.get(partAssignmentsPlannedKey(partId));
        Entry pendingEntry = keys.get(partAssignmentsPendingKey(partId));

        tblConfiguration.change(ch -> {
            List<List<ClusterNode>> assignments =
                    (List<List<ClusterNode>>) ByteUtils.fromBytes(((ExtendedTableChange) ch).assignments());
            assignments.set(partNum, ((List<ClusterNode>) ByteUtils.fromBytes(pendingEntry.value())));
            ((ExtendedTableChange) ch).changeAssignments(ByteUtils.toBytes(assignments));
        });

        if (plannedEntry.value() != null) {
            if (!metaStorageMgr.invoke(If.iif(
                    revision(partAssignmentsPlannedKey(partId)).eq(plannedEntry.revision()),
                    ops(
                            put(partAssignmentsStableKey(partId), pendingEntry.value()),
                            put(partAssignmentsPendingKey(partId), plannedEntry.value()),
                            remove(partAssignmentsPlannedKey(partId)))
                            .yield(true),
                    ops().yield(false))).join().getAsBoolean()) {
                onNewPeersConfigurationApplied(peers);
            }
        } else {
            if (!metaStorageMgr.invoke(If.iif(
                    notExists(partAssignmentsPlannedKey(partId)),
                    ops(put(partAssignmentsStableKey(partId), pendingEntry.value()),
                            remove(partAssignmentsPendingKey(partId))).yield(true),
                    ops().yield(false))).join().getAsBoolean()) {
                onNewPeersConfigurationApplied(peers);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public void onReconfigurationError(Status status) {}
}
