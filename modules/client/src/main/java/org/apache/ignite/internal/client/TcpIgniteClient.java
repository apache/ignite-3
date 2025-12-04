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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.catalog.IgniteCatalog;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.catalog.sql.IgniteCatalogSqlImpl;
import org.apache.ignite.internal.client.compute.ClientCompute;
import org.apache.ignite.internal.client.network.ClientCluster;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.sql.ClientSql;
import org.apache.ignite.internal.client.table.ClientTables;
import org.apache.ignite.internal.client.tx.ClientTransactions;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.apache.ignite.internal.jdbc.proto.ClientMessage;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.metrics.MetricManagerImpl;
import org.apache.ignite.internal.metrics.exporters.jmx.JmxExporter;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.IgniteCluster;
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
    private static final AtomicLong GLOBAL_CONN_ID_GEN = new AtomicLong();

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

    /** Cluster. */
    private final ClientCluster cluster;

    /** Cluster name. */
    private String clusterName;

    private final String clientName;

    /**
     * Constructor.
     *
     * @param cfg Config.
     * @param observableTimeTracker Tracker of the latest time observed by client.
     * @param channelValidator A validator that is called when a connection to a node is established,
     *                         if it throws an exception, the network channel to that node will be closed.
     */
    private TcpIgniteClient(IgniteClientConfigurationImpl cfg, HybridTimestampTracker observableTimeTracker,
            @Nullable ChannelValidator channelValidator) {
        this(TcpClientChannel::createAsync, cfg, observableTimeTracker, channelValidator);
    }

    /**
     * Constructor with custom channel factory.
     *
     * @param chFactory Channel factory.
     * @param cfg Config.
     * @param observableTimeTracker Tracker of the latest time observed by client.
     * @param channelValidator A validator that is called when a connection to a node is established,
     *                         if it throws an exception, the network channel to that node will be closed.
     */
    private TcpIgniteClient(
            ClientChannelFactory chFactory,
            IgniteClientConfigurationImpl cfg,
            HybridTimestampTracker observableTimeTracker,
            @Nullable ChannelValidator channelValidator) {
        assert chFactory != null;
        assert cfg != null;

        this.cfg = cfg;

        String cfgName = cfg.name();
        clientName = cfgName != null
                ? cfgName
                : "client_" + GLOBAL_CONN_ID_GEN.incrementAndGet(); // Use underscores for JMX compat.

        metrics = new ClientMetricSource();
        ch = new ReliableChannel(chFactory, cfg, metrics, observableTimeTracker, channelValidator);
        transactions = new ClientTransactions(ch);
        tables = new ClientTables(ch, marshallers, cfg.sqlPartitionAwarenessMetadataCacheSize());
        compute = new ClientCompute(ch, tables);
        sql = new ClientSql(ch, marshallers, cfg.sqlPartitionAwarenessMetadataCacheSize());
        metricManager = initMetricManager(cfg);
        cluster = new ClientCluster(ch);
    }

    @Nullable
    private MetricManager initMetricManager(IgniteClientConfiguration cfg) {
        if (!cfg.metricsEnabled()) {
            return null;
        }

        var metricManager = new MetricManagerImpl(ClientUtils.logger(cfg, MetricManagerImpl.class), clientName);

        metricManager.registerSource(metrics);
        metricManager.enable(metrics);
        metricManager.start(List.of(new JmxExporter(ClientUtils.logger(cfg, JmxExporter.class))));

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
    public static CompletableFuture<IgniteClient> startAsync(IgniteClientConfigurationImpl cfg) {
        return startAsync(cfg, HybridTimestampTracker.atomicTracker(null), null);
    }

    /**
     * Initializes new instance of {@link IgniteClient} and establishes the connection.
     *
     * @param cfg Thin client configuration.
     * @param observableTimeTracker Tracker of the latest time observed by client.
     * @param channelValidator A validator that is called when a connection to a node is established,
     *                         if it throws an exception, the network channel to that node will be closed.
     * @return Future representing pending completion of the operation.
     */
    public static CompletableFuture<IgniteClient> startAsync(
            IgniteClientConfigurationImpl cfg,
            HybridTimestampTracker observableTimeTracker,
            @Nullable ChannelValidator channelValidator) {
        ErrorGroups.initialize();

        try {
            //noinspection resource: returned from method
            var client = new TcpIgniteClient(cfg, observableTimeTracker, channelValidator);

            return client.initAsync().thenApply(x -> client);
        } catch (IgniteException e) {
            return failedFuture(e);
        }
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

    @Override
    public IgniteCatalog catalog() {
        return new IgniteCatalogSqlImpl(sql, tables);
    }

    @Override
    public IgniteCluster cluster() {
        return cluster;
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        try {
            ch.close();
        } catch (Exception e) {
            throw new IgniteInternalException(CONNECTION_ERR, "Error occurred while closing the channel", e);
        }

        if (metricManager != null) {
            metricManager.beforeNodeStop();
            metricManager.stopAsync(new ComponentContext()).join();
        }
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return clientName;
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

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(TcpIgniteClient.class.getSimpleName(), "name", clientName, "clusterName", clusterName);
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
     * Returns the underlying channel.
     *
     * @return Channel.
     */
    public ReliableChannel channel() {
        return ch;
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
                in.unpackUuid(),
                in.unpackString(),
                new NetworkAddress(in.unpackString(), in.unpackInt()));
    }
}
