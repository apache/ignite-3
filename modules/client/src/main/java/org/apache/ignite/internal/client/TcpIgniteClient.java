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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.client.ClientUtils.sync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.client.compute.ClientCompute;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.table.ClientTables;
import org.apache.ignite.internal.client.tx.ClientTransactions;
import org.apache.ignite.internal.jdbc.proto.ClientMessage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * Implementation of {@link IgniteClient} over TCP protocol.
 */
public class TcpIgniteClient implements IgniteClient {
    /** Configuration. */
    private final IgniteClientConfiguration cfg;

    /** Channel. */
    private final ReliableChannel ch;

    /** Tables. */
    private final ClientTables tables;

    /** Transactions. */
    private final ClientTransactions transactions;

    /** Compute. */
    private final ClientCompute compute;

    /** Compute. */
    private final ClientSql sql;

    /**
     * Constructor.
     *
     * @param cfg Config.
     */
    private TcpIgniteClient(IgniteClientConfiguration cfg) {
        this(TcpClientChannel::new, cfg);
    }

    /**
     * Constructor with custom channel factory.
     *
     * @param chFactory Channel factory.
     * @param cfg Config.
     */
    private TcpIgniteClient(
            BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory,
            IgniteClientConfiguration cfg
    ) {
        assert chFactory != null;
        assert cfg != null;

        this.cfg = cfg;

        ch = new ReliableChannel(chFactory, cfg);
        tables = new ClientTables(ch);
        transactions = new ClientTransactions(ch);
        compute = new ClientCompute(ch, tables);
        sql = new ClientSql(ch);
    }

    /**
     * Initializes the connection.
     *
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> initAsync() {
        return ch.channelsInitAsync();
    }

    /**
     * Initializes new instance of {@link IgniteClient} and establishes the connection.
     *
     * @param cfg Thin client configuration.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<IgniteClient> startAsync(IgniteClientConfiguration cfg) throws IgniteClientException {
        var client = new TcpIgniteClient(cfg);

        return client.initAsync().thenApply(x -> client);
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTables tables() {
        return tables;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteTransactions transactions() {
        return transactions;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteSql sql() {
        return sql;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteCompute compute() {
        return compute;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNode> clusterNodes() {
        return sync(clusterNodesAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<ClusterNode>> clusterNodesAsync() {
        return ch.serviceAsync(ClientOp.CLUSTER_GET_NODES, r -> {
            int cnt = r.in().unpackArrayHeader();
            List<ClusterNode> res = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; i++) {
                res.add(new ClusterNode(
                        r.in().unpackString(),
                        r.in().unpackString(),
                        new NetworkAddress(r.in().unpackString(), r.in().unpackInt())));
            }

            return res;
        });
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        ch.close();
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return "thin-client";
    }

    /** {@inheritDoc} */
    @Override
    public IgniteClientConfiguration configuration() {
        return cfg;
    }

    /** {@inheritDoc} */
    @Override
    public List<ClusterNode> connections() {
        return ch.connections();
    }

    /**
     * Sends ClientMessage request to server side asynchronously and returns result future.
     *
     * @param opCode Operation code.
     * @param req ClientMessage request.
     * @param res ClientMessage result.
     * @return Response future.
     */
    public <T extends ClientMessage> CompletableFuture<T> sendRequestAsync(int opCode, ClientMessage req, T res) {
        return ch.serviceAsync(opCode, w -> req.writeBinary(w.out()), p -> {
            res.readBinary(p.in());
            return res;
        });
    }
}
