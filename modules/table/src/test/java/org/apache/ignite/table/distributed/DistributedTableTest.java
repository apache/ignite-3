/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.table.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.affinity.RendezvousAffinityFunction;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.TableSchemaView;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.KVGetResponse;
import org.apache.ignite.internal.table.distributed.command.response.KVInsertResponse;
import org.apache.ignite.internal.table.distributed.raft.PartitionCommandListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.scalecube.ScaleCubeMemberResolver;
import org.apache.ignite.network.scalecube.ScaleCubeNetworkClusterFactory;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.apache.ignite.raft.server.RaftServer;
import org.apache.ignite.raft.server.impl.RaftServerImpl;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DistributedTableTest {

    public static final int NODE_PORT_BASE = 20_000;
    public static final int NODES = 5;
    public static final int PARTS = 10;
    private static RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();
    private NetworkCluster client;

    public static SchemaDescriptor SCHEMA = new SchemaDescriptor(1, new Column[] {
        new Column("key", NativeType.LONG, false)
    }, new Column[] {
        new Column("value", NativeType.LONG, false)
    });

    /** Logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(DistributedTableTest.class);

    private ArrayList<NetworkCluster> cluster = new ArrayList<>();

    @BeforeEach
    public void beforeTest() {
        for (int i = 0; i < NODES; i++) {
            cluster.add(startClient(
                "node_" + i,
                NODE_PORT_BASE + i,
                IntStream.range(NODE_PORT_BASE, NODE_PORT_BASE + NODES).boxed().map((port) -> "localhost:" + port).collect(Collectors.toList())
            ));
        }

        for (NetworkCluster node : cluster)
            assertTrue(waitForTopology(node, NODES, 1000));

        LOG.info("Cluster started.");

        client = startClient(
            "client",
            NODE_PORT_BASE + NODES,
            IntStream.range(NODE_PORT_BASE, NODE_PORT_BASE + NODES).boxed().map((port) -> "localhost:" + port).collect(Collectors.toList())
        );

        assertTrue(waitForTopology(client, NODES + 1, 1000));

        LOG.info("Client started.");
    }

    @AfterEach
    public void afterTest() throws Exception {
        for (NetworkCluster node : cluster) {
            node.shutdown();
        }

        client.shutdown();
    }

    @Test
    public void partitionListener() throws Exception {
        String grpId = "part";

        RaftServer partSrv = new RaftServerImpl(
            cluster.get(0),
            FACTORY,
            1000,
            Map.of(grpId, new PartitionCommandListener())
        );

        RaftGroupService partRaftGrp = new RaftGroupServiceImpl(grpId, client, FACTORY, 10_000,
            List.of(new Peer(cluster.get(0).localMember())), true, 200);

        Row testRow = getTestRow();

        CompletableFuture<KVInsertResponse> insertFur = partRaftGrp.run(new InsertCommand(testRow));

        assertTrue(insertFur.get().isResult());

//        Row keyChunk = new Row(SCHEMA, new ByteBufferRow(testRow.keySlice()));
        Row keyChunk = getTestKey();

        CompletableFuture<KVGetResponse> getFur = partRaftGrp.run(new GetCommand(keyChunk));

        assertNotNull(getFur.get().getValue());

        assertEquals(testRow.longValue(1), new Row(SCHEMA, getFur.get().getValue()).longValue(1));
    }

    @NotNull private Row getTestKey() {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 4096, 0, 0);

        rowBuilder.appendLong(1L);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.build()));
    }

    @NotNull private Row getTestRow() {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 4096, 0, 0);

        rowBuilder.appendLong(1L);
        rowBuilder.appendLong(10L);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.build()));
    }

    @Test
    public void partitionedTable() {
        HashMap<NetworkMember, RaftServer> raftServers = new HashMap<>(NODES);

        for (int i = 0; i < NODES; i++) {
            raftServers.put(cluster.get(i).localMember(), new RaftServerImpl(
                cluster.get(i),
                FACTORY,
                1000,
                Map.of()
            ));
        }

        List<List<NetworkMember>> assignment = RendezvousAffinityFunction.assignPartitions(
            cluster.stream().map(node -> node.localMember()).collect(Collectors.toList()),
            PARTS,
            1,
            false,
            null
        );

        int p = 0;

        Map<Integer, RaftGroupService> partMap = new HashMap<>();

        for (List<NetworkMember> partMembers : assignment) {
            RaftServer rs = raftServers.get(partMembers.get(0));

            String grpId = "part-" + p;

            rs.setListener(grpId, new PartitionCommandListener());

            partMap.put(p, new RaftGroupServiceImpl(grpId, client, FACTORY, 10_000,
                List.of(new Peer(partMembers.get(0))), true, 200));

            p++;
        }

        Table tbl = new TableImpl(new InternalTableImpl(
            UUID.randomUUID(),
            partMap,
            PARTS
        ), new TableSchemaView() {
            @Override public SchemaDescriptor schema() {
                return SCHEMA;
            }

            @Override public SchemaDescriptor schema(int ver) {
                return SCHEMA;
            }
        });

        for (int i = 0; i < PARTS * 10; i++) {
            tbl.kvView().putIfAbsent(
                tbl.kvView().tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build(),
                tbl.kvView().tupleBuilder()
                    .set("value", Long.valueOf(i + 2))
                    .build());
        }

        for (int i = 0; i < PARTS * 10; i++) {
            Tuple entry = tbl.kvView().get(
                tbl.kvView().tupleBuilder()
                    .set("key", Long.valueOf(i))
                    .build());

            LOG.info("The result is [key=" + i + ", tuple=" + entry + ']');

            assertEquals(Long.valueOf(i + 2), entry.longValue("value"));
        }

        //Nothing to do until marshaller will be implemented.
//        tbl.kvView().get(new TestTableRowImpl("Hello!".getBytes()));
    }

    /**
     * @param name Node name.
     * @param port Local port.
     * @param servers Server nodes of the cluster.
     * @return The client cluster view.
     */
    private NetworkCluster startClient(String name, int port, List<String> servers) {
        Network network = new Network(
            new ScaleCubeNetworkClusterFactory(name, port, servers, new ScaleCubeMemberResolver())
        );

//        network.registerMessageMapper((short)1000, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1001, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1005, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1006, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1009, new DefaultMessageMapperProvider());

        return network.start();
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    private boolean waitForTopology(NetworkCluster cluster, int expected, int timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cluster.allMembers().size() >= expected)
                return true;

            try {
                Thread.sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
