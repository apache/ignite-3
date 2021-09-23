package org.apache.ignite.internal.table;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.Peer;

public class PartitionAssignmentsUpdater implements Supplier<List<Peer>> {

    private MetaStorageManager metaStorageManager;

    private IgniteUuid tblId;

    private int partition;

    public PartitionAssignmentsUpdater(MetaStorageManager mgr, IgniteUuid tblId, int partition) {
        this.metaStorageManager = mgr;
        this.tblId = tblId;
        this.partition = partition;
    }

    @Override public List<Peer> get() {
        byte[] assignment = null;

        try (Cursor<Entry> cursor = metaStorageManager.prefix(ByteArray.fromString("dst-cfg.table.tables."))) {
            while (cursor.hasNext()) {
                Entry entry = cursor.next();

                List<String> keySplit = ConfigurationUtil.split(entry.key().toString());

                if (keySplit.size() == 5 && "id".equals(keySplit.get(4)) &&
                    entry.value() != null && Arrays.equals(ByteUtils.toBytes(tblId), entry.value()) &&
                    assignment != null)
                        return ((List<List<ClusterNode>>) ByteUtils.fromBytes(assignment))
                            .get(partition).stream().map(n -> new Peer(n.address())).collect(Collectors.toList());

                if (keySplit.size() == 5 && "assignments".equals(keySplit.get(4)))
                    assignment = entry.value();
            }
        }
        catch (Exception e) {
            throw new IgniteInternalException("Can't receive partition " + partition + " assignments for table " + tblId, e);
        }
        throw new IgniteInternalException("Assignments for table " + tblId + " were not found in metastore");
    }
}
