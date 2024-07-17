/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.catalog.sql.IgniteCatalogSqlImpl;
import org.apache.ignite.internal.client.compute.ClientCompute;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.table.ClientTables;
import org.apache.ignite.internal.client.tx.ClientTransactions;
import org.apache.ignite.internal.jdbc.proto.ClientMessage;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.exporters.jmx.JmxExporter;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.tx.IgniteTransactions;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

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

    /** Metric manager. */
    private final @Nullable MetricManager metricManager;

    /** Metrics. */
    private final ClientMetricSource metrics;

    /** Marshallers provider. */
    private final ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

    /**
     * Cluster name.
     */
    private String clusterName;

    /**
     * Constructor.
     *
     * @param cfg Config.
     */
    private TcpIgniteClient(IgniteClientConfiguration cfg) {
        this(TcpClientChannel::createAsync, cfg);
    }

    /**
     * Constructor with custom channel factory.
     *
     * @param chFactory Channel factory.
     * @param cfg Config.
     */
    private TcpIgniteClient(ClientChannelFactory chFactory, IgniteClientConfiguration cfg) {
        assert chFactory != null;
        assert cfg != null;

        this.cfg = cfg;

        metrics = new ClientMetricSource();
        ch = new ReliableChannel(chFactory, cfg, metrics);
        tables = new ClientTables(ch, marshallers);
        transactions = new ClientTransactions(ch);
        compute = new ClientCompute(ch, tables);
        sql = new ClientSql(ch, marshallers);
        metricManager = initMetricManager(cfg);
    }

    @Nullable
    private MetricManager initMetricManager(IgniteClientConfiguration cfg) {
        if (!cfg.metricsEnabled()) {
            return null;
        }

        var metricManager = new MetricManagerImpl(ClientUtils.logger(cfg, MetricManagerImpl.class));
        metricManager.start(List.of(new JmxExporter(ClientUtils.logger(cfg, JmxExporter.class))));

        metricManager.registerSource(metrics);
        metrics.enable();

        return metricManager;
    }

    /**
     * Initializes the connection.
     *
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<ClientChannel> initAsync() {
        return ch.channelsInitAsync().whenComplete((channel, throwable) -> {
            if (throwable == null) {
                clusterName = channel.protocolContext().clusterName();
            }
        });
    }

    /**
     * Initializes new instance of {@link IgniteClient} and establishes the connection.
     *
     * @param cfg Thin client configuration.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<IgniteClient> startAsync(IgniteClientConfiguration cfg) {
        ErrorGroups.initialize();

        //noinspection resource: returned from method
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
            int cnt = r.in().unpackInt();
            List<ClusterNode> res = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; i++) {
                ClusterNode clusterNode = unpackClusterNode(r);

                res.add(clusterNode);
            }

            return res;
        });
    }

    @Override
    public IgniteCatalog catalog() {
        return new IgniteCatalogSqlImpl(sql(), tables);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        ch.close();

        if (metricManager != null) {
            metricManager.stopAsync(new ComponentContext()).join();
        }
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
     * Returns the name of the cluster to which this client is connected to.
     *
     * @return Cluster name.
     */
    public String clusterName() {
        return clusterName;
    }

    @TestOnly
    public ClientMetricSource metrics() {
        return metrics;
    }

    /**
     * Sends ClientMessage request to server side asynchronously and returns result future.
     *
     * @param opCode Operation code.
     * @param writer Payload writer.
     * @param reader Payload reader.
     * @return Response future.
     */
    public <T extends ClientMessage> CompletableFuture<T> sendRequestAsync(int opCode, PayloadWriter writer, PayloadReader<T> reader) {
        return ch.serviceAsync(opCode, writer, reader);
    }

    /**
     * Tries to unpack {@link ClusterNode} instance from input channel.
     *
     * @param r Payload input channel.
     * @return Cluster node or {@code null} if message doesn't contain cluster node.
     */
    public static ClusterNode unpackClusterNode(PayloadInputChannel r) {
        ClientMessageUnpacker in = r.in();

        int fieldCnt = r.in().unpackInt();
        assert fieldCnt == 4;

        return new ClientClusterNode(
                in.unpackString(),
                in.unpackString(),
                new NetworkAddress(in.unpackString(), in.unpackInt()));
    }
}
