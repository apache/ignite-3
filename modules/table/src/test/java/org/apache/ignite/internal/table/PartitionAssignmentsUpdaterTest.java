package org.apache.ignite.internal.table;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PartitionAssignmentsUpdaterTest {

    private static final String PUBLIC_PREFIX = "dst-cfg.table.tables.";

    @Test
    public void testLastAssignmentsReceive() throws NodeStoppingException, ExecutionException, InterruptedException {
        var provider = new PartitionAssignmentsUpdater(mockMetastorageManager(),
            IgniteUuid.fromString("1-458ea9e2-4394-490b-aba3-3e932f980b8a"), 1);

        assertEquals(Collections.singletonList(new Peer(NetworkAddress.from("localhost:1001"))), provider.get());
    }

    private MetaStorageManager mockMetastorageManager() throws NodeStoppingException {
        var mm = mock(MetaStorageManager.class);

        when(mm.prefix(ByteArray.fromString(PUBLIC_PREFIX))).thenReturn(new Cursor<Entry>() {
            private Iterator<Entry> innerIterator = new LinkedHashMap<String, byte[]>() {{
                put("dst-cfg.table.tables.7d53398d7c934f978cfd8a1ff21305fd.assignments",
                    ByteUtils.toBytes(Arrays.asList(
                        Arrays.asList(new ClusterNode("node3", "node3", NetworkAddress.from("localhost:2000"))),
                        Arrays.asList(new ClusterNode("node4", "node4", NetworkAddress.from("localhost:2001")))
                    )));
                put("dst-cfg.table.tables.7d53398d7c934f978cfd8a1ff21305fd.id",
                    ByteUtils.toBytes(IgniteUuid.fromString("1-39f56d1d-3d25-4c61-80d3-091f5fab96fc")));

                put("dst-cfg.table.tables.b965ef210c9f439594d06210de71da64.assignments",
                    ByteUtils.toBytes(Arrays.asList(
                        Arrays.asList(new ClusterNode("node1", "node1", NetworkAddress.from("localhost:1000"))),
                        Arrays.asList(new ClusterNode("node2", "node2", NetworkAddress.from("localhost:1001")))
                    )));
                put("dst-cfg.table.tables.b965ef210c9f439594d06210de71da64.id",
                    ByteUtils.toBytes(IgniteUuid.fromString("1-458ea9e2-4394-490b-aba3-3e932f980b8a")));
            }}.entrySet().stream().map(e -> entry(e.getKey(), e.getValue())).iterator();

            @Override public void close() throws Exception {}

            @NotNull @Override public Iterator<Entry> iterator() {
                return innerIterator;
            }

            @Override public boolean hasNext() {
                return innerIterator.hasNext();
            }

            @Override public Entry next() {
                return innerIterator.next();
            }
        });

        return mm;
    }

    private Entry entry(String key, byte[] value) {
       return new Entry() {
           @Override public @NotNull ByteArray key() {
               return ByteArray.fromString(key);
           }

           @Override public @Nullable byte[] value() {
               return value;
           }

           @Override public long revision() {
               return 0;
           }

           @Override public long updateCounter() {
               return 0;
           }

           @Override public boolean empty() {
               return false;
           }

           @Override public boolean tombstone() {
               return false;
           }
       };
    }

}
