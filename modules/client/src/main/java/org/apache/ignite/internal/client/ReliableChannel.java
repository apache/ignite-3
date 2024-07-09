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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.tracing.Instrumentation.measure;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Client.CLUSTER_ID_MISMATCH_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.CONFIGURATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.client.ClientOperationType;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.client.RetryPolicy;
import org.apache.ignite.client.RetryPolicyContext;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.netty.NettyClientConnectionMultiplexer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Communication channel with failover and partition awareness.
 */
public final class ReliableChannel implements AutoCloseable {
    /** Channel factory. */
    private final ClientChannelFactory chFactory;

    /** Metrics. */
    private final ClientMetricSource metrics;

    /** Client channel holders for each configured address. */
    private volatile List<ClientChannelHolder> channels;

    /** Index of the default channel. */
    private volatile int defaultChIdx = -1;

    /** Index of the current channel (for round-robin balancing). */
    private final AtomicInteger curChIdx = new AtomicInteger();

    /** Client configuration. */
    private final IgniteClientConfiguration clientCfg;

    /** Node channels by name (consistent id). */
    private final Map<String, ClientChannelHolder> nodeChannelsByName = new ConcurrentHashMap<>();

    /** Channels reinit was scheduled. */
    private final AtomicBoolean scheduledChannelsReinit = new AtomicBoolean();

    /** Channel is closed. */
    private volatile boolean closed;

    /** Fail (disconnect) listeners. */
    private final ArrayList<Runnable> chFailLsnrs = new ArrayList<>();

    /** Guard channels and curChIdx together. */
    private final ReadWriteLock curChannelsGuard = new ReentrantReadWriteLock();

    /** Connection manager. */
    private final ClientConnectionMultiplexer connMgr;

    private final IgniteLogger log;

    /** Cache addresses returned by {@code ThinClientAddressFinder}. */
    private volatile String[] prevHostAddrs;

    /** Latest known partition assignment timestamp (for any table). */
    private final AtomicLong partitionAssignmentTimestamp = new AtomicLong();

    /** Observable timestamp, or causality token. Sent by the server with every response, and required by some requests. */
    private final AtomicLong observableTimestamp = new AtomicLong();

    /** Cluster id from the first handshake. */
    private final AtomicReference<UUID> clusterId = new AtomicReference<>();

    /** Scheduled executor for streamer flush. */
    @Nullable
    private ScheduledExecutorService streamerFlushExecutor;

    /**
     * Constructor.
     *
     * @param chFactory Channel factory.
     * @param clientCfg Client config.
     */
    ReliableChannel(
            ClientChannelFactory chFactory,
            IgniteClientConfiguration clientCfg,
            ClientMetricSource metrics) {
        this.clientCfg = Objects.requireNonNull(clientCfg, "clientCfg");
        this.chFactory = Objects.requireNonNull(chFactory, "chFactory");
        this.log = ClientUtils.logger(clientCfg, ReliableChannel.class);
        this.metrics = metrics;

        connMgr = new NettyClientConnectionMultiplexer(metrics);
        connMgr.start(clientCfg);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void close() throws Exception {
        closed = true;

        List<ClientChannelHolder> holders = channels;

        IgniteUtils.closeAllManually(
                () -> {
                    if (holders != null) {
                        for (ClientChannelHolder hld : holders) {
                            hld.close();
                        }
                    }
                },
                connMgr::stop,
                () -> shutdownAndAwaitTermination(streamerFlushExecutor, 10, TimeUnit.SECONDS));
    }

    /**
     * Gets the metrics.
     *
     * @return Metrics.
     */
    public ClientMetricSource metrics() {
        return metrics;
    }

    /**
     * Gets active client connections.
     *
     * @return List of connected cluster nodes.
     */
    public List<ClusterNode> connections() {
        List<ClusterNode> res = new ArrayList<>(channels.size());

        for (var holder : nodeChannelsByName.values()) {
            var chFut = holder.chFut;

            if (chFut != null) {
                var ch = ClientFutureUtils.getNowSafe(chFut);

                if (ch != null && !ch.closed()) {
                    res.add(ch.protocolContext().clusterNode());
                }
            }
        }

        return res;
    }

    public IgniteClientConfiguration configuration() {
        return clientCfg;
    }

    public long observableTimestamp() {
        return observableTimestamp.get();
    }

    /**
     * Sends request and handles response asynchronously.
     *
     * @param opCode        Operation code.
     * @param payloadWriter Payload writer.
     * @param payloadReader Payload reader.
     * @param <T>           response type.
     * @param preferredNodeName Unique name (consistent id) of the preferred target node. When a connection to the specified node exists,
     *                          it will be used to handle the request; otherwise, default connection will be used.
     * @param retryPolicyOverride Retry policy override.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader,
            @Nullable String preferredNodeName,
            @Nullable RetryPolicy retryPolicyOverride,
            boolean expectNotifications
    ) {
        return ClientFutureUtils.doWithRetryAsync(() -> measure(() -> getChannelAsync(preferredNodeName), "getChannelAsync")
                        .thenCompose(ch -> serviceAsyncInternal(opCode, payloadWriter, payloadReader, expectNotifications, ch)),
                null,
                ctx -> shouldRetry(opCode, ctx, retryPolicyOverride)
        );
    }

    /**
     * Sends request and handles response asynchronously.
     *
     * @param opCode        Operation code.
     * @param payloadWriter Payload writer.
     * @param payloadReader Payload reader.
     * @param <T>           response type.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader
    ) {
        return serviceAsync(opCode, payloadWriter, payloadReader, null, null, false);
    }

    /**
     * Sends request without payload and handles response asynchronously.
     *
     * @param opCode        Operation code.
     * @param payloadReader Payload reader.
     * @param <T>           Response type.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(int opCode, PayloadReader<T> payloadReader) {
        return serviceAsync(opCode, null, payloadReader, null, null, false);
    }

    private <T> CompletableFuture<T> serviceAsyncInternal(
            int opCode,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader,
            boolean expectNotifications,
            ClientChannel ch) {
        return ch.serviceAsync(opCode, payloadWriter, payloadReader, expectNotifications).whenComplete((res, err) -> {
            if (err != null && unwrapConnectionException(err) != null) {
                onChannelFailure(ch);
            }
        });
    }

    private CompletableFuture<ClientChannel> getChannelAsync(@Nullable String preferredNodeName) {
        // 1. Preferred node connection.
        if (preferredNodeName != null) {
            ClientChannelHolder holder = nodeChannelsByName.get(preferredNodeName);

            if (holder != null) {
                return holder.getOrCreateChannelAsync().thenCompose(ch -> {
                    if (ch != null) {
                        return completedFuture(ch);
                    } else {
                        return getDefaultChannelAsync();
                    }
                });
            }
        }

        // 2. Round-robin connection.
        ClientChannel nextCh = getNextChannelWithoutReconnect();

        if (nextCh != null) {
            return completedFuture(nextCh);
        }

        // 3. Default connection (with reconnect if necessary).
        return getDefaultChannelAsync();
    }

    /**
     * Returns host:port_range address lines parsed as {@link InetSocketAddress} as a key. Value is the amount of appearences of an address
     *      in {@code addrs} parameter.
     *
     * @return host:port_range address lines parsed as {@link InetSocketAddress} as a key. Value is the amount of appearences of an address
     *      in {@code addrs} parameter.
     */
    private static Map<InetSocketAddress, Integer> parsedAddresses(String[] addrs) {
        if (addrs == null || addrs.length == 0) {
            throw new IgniteException(CONFIGURATION_ERR, "Empty addresses");
        }

        Collection<HostAndPort> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            ranges.add(HostAndPort.parse(a, IgniteClientConfiguration.DFLT_PORT, "Failed to parse Ignite server address"));
        }

        return ranges.stream()
                .map(p -> InetSocketAddress.createUnresolved(p.host(), p.port()))
                .collect(Collectors.toMap(a -> a, a -> 1, Integer::sum));
    }

    /**
     * Roll current default channel if specified holder equals to it.
     */
    private void rollCurrentChannel(ClientChannelHolder hld) {
        curChannelsGuard.writeLock().lock();

        try {
            int idx = defaultChIdx;
            List<ClientChannelHolder> holders = channels;

            ClientChannelHolder dfltHld = holders.get(idx);

            if (dfltHld == hld) {
                idx += 1;

                if (idx >= holders.size()) {
                    defaultChIdx = 0;
                } else {
                    defaultChIdx = idx;
                }
            }
        } finally {
            curChannelsGuard.writeLock().unlock();
        }
    }

    /**
     * On current channel failure.
     */
    private void onChannelFailure(ClientChannel ch) {
        // There is nothing wrong if defaultChIdx was concurrently changed, since channel was closed by another thread
        // when current index was changed and no other wrong channel will be closed by current thread because
        // onChannelFailure checks channel binded to the holder before closing it.
        onChannelFailure(channels.get(defaultChIdx), ch);
    }

    /**
     * On channel of the specified holder failure.
     */
    private void onChannelFailure(ClientChannelHolder hld, @Nullable ClientChannel ch) {
        chFailLsnrs.forEach(Runnable::run);

        // Roll current channel even if a topology changes. To help find working channel faster.
        rollCurrentChannel(hld);

        if (scheduledChannelsReinit.get()) {
            channelsInitAsync();
        }
    }

    /**
     * Adds listener for the channel fail (disconnect).
     *
     * @param chFailLsnr Listener for the channel fail (disconnect).
     */
    public void addChannelFailListener(Runnable chFailLsnr) {
        chFailLsnrs.add(chFailLsnr);
    }

    /**
     * Should the channel initialization be stopped.
     */
    private boolean shouldStopChannelsReinit() {
        return scheduledChannelsReinit.get() || closed;
    }

    /**
     * Init channel holders to all nodes.
     *
     * @return boolean wheter channels was reinited.
     */
    private synchronized boolean initChannelHolders() {
        List<ClientChannelHolder> holders = channels;

        // Enable parallel threads to schedule new init of channel holders.
        scheduledChannelsReinit.set(false);

        Map<InetSocketAddress, Integer> newAddrs = null;

        if (clientCfg.addressesFinder() != null) {
            String[] hostAddrs = clientCfg.addressesFinder().getAddresses();

            if (hostAddrs.length == 0) {
                throw new IgniteException(CONFIGURATION_ERR, "Empty addresses");
            }

            if (!Arrays.equals(hostAddrs, prevHostAddrs)) {
                newAddrs = parsedAddresses(hostAddrs);
                prevHostAddrs = hostAddrs;
            }
        } else if (holders == null) {
            newAddrs = parsedAddresses(clientCfg.addresses());
        }

        if (newAddrs == null) {
            return true;
        }

        Map<InetSocketAddress, ClientChannelHolder> curAddrs = new HashMap<>();
        Set<InetSocketAddress> allAddrs = new HashSet<>(newAddrs.keySet());

        if (holders != null) {
            for (int i = 0; i < holders.size(); i++) {
                ClientChannelHolder h = holders.get(i);

                curAddrs.put(h.chCfg.getAddress(), h);
                allAddrs.add(h.chCfg.getAddress());
            }
        }

        List<ClientChannelHolder> reinitHolders = new ArrayList<>();

        // The variable holds a new index of default channel after topology change.
        // Suppose that reuse of the channel is better than open new connection.
        int dfltChannelIdx = -1;

        ClientChannelHolder currDfltHolder = null;

        int idx = defaultChIdx;

        if (idx != -1) {
            currDfltHolder = holders.get(idx);
        }

        for (InetSocketAddress addr : allAddrs) {
            if (shouldStopChannelsReinit()) {
                return false;
            }

            // Obsolete addr, to be removed.
            if (!newAddrs.containsKey(addr)) {
                curAddrs.get(addr).close();

                continue;
            }

            // Create new holders for new addrs.
            if (!curAddrs.containsKey(addr)) {
                ClientChannelHolder hld = new ClientChannelHolder(new ClientChannelConfiguration(clientCfg, addr));

                for (int i = 0; i < newAddrs.get(addr); i++) {
                    reinitHolders.add(hld);
                }

                continue;
            }

            // This holder is up to date.
            ClientChannelHolder hld = curAddrs.get(addr);

            for (int i = 0; i < newAddrs.get(addr); i++) {
                reinitHolders.add(hld);
            }

            if (hld == currDfltHolder) {
                dfltChannelIdx = reinitHolders.size() - 1;
            }
        }

        if (dfltChannelIdx == -1) {
            dfltChannelIdx = 0;
        }

        curChannelsGuard.writeLock().lock();

        try {
            channels = reinitHolders;
            defaultChIdx = dfltChannelIdx;
        } finally {
            curChannelsGuard.writeLock().unlock();
        }

        return true;
    }

    /**
     * Init channel holders, establish connection to default channel.
     */
    CompletableFuture<ClientChannel> channelsInitAsync() {
        // Do not establish connections if interrupted.
        if (!initChannelHolders()) {
            return nullCompletedFuture();
        }

        // Establish default channel connection.
        var fut = getDefaultChannelAsync();

        // Establish secondary connections in the background.
        fut.thenAccept(unused -> ForkJoinPool.commonPool().submit(this::initAllChannelsAsync));

        return fut;
    }

    private @Nullable ClientChannel getNextChannelWithoutReconnect() {
        curChannelsGuard.readLock().lock();

        try {
            int startIdx = curChIdx.incrementAndGet();

            for (int i = 0; i < channels.size(); i++) {
                int nextIdx = Math.abs(startIdx + i) % channels.size();

                ClientChannelHolder hld = channels.get(nextIdx);
                ClientChannel ch = hld == null ? null : hld.getNow();

                if (ch != null) {
                    return ch;
                }
            }
        } finally {
            curChannelsGuard.readLock().unlock();
        }

        return null;
    }

    /**
     * Gets the default channel, reconnecting if necessary.
     */
    private CompletableFuture<ClientChannel> getDefaultChannelAsync() {
        return ClientFutureUtils.doWithRetryAsync(
                () -> {
                    curChannelsGuard.readLock().lock();

                    ClientChannelHolder hld;

                    try {
                        hld = channels.get(defaultChIdx);
                    } finally {
                        curChannelsGuard.readLock().unlock();
                    }

                    return hld.getOrCreateChannelAsync();
                },
                Objects::nonNull,
                ctx -> shouldRetry(ClientOperationType.CHANNEL_CONNECT, ctx, null));
    }

    private CompletableFuture<ClientChannel> getCurChannelAsync() {
        if (closed) {
            return CompletableFuture.failedFuture(new IgniteClientConnectionException(CONNECTION_ERR, "ReliableChannel is closed"));
        }

        curChannelsGuard.readLock().lock();

        try {
            var hld = channels.get(defaultChIdx);

            if (hld == null) {
                return nullCompletedFuture();
            }

            CompletableFuture<ClientChannel> fut = hld.getOrCreateChannelAsync();
            return fut == null ? nullCompletedFuture() : fut;
        } finally {
            curChannelsGuard.readLock().unlock();
        }
    }

    /** Determines whether specified operation should be retried. */
    private boolean shouldRetry(int opCode, ClientFutureUtils.RetryContext ctx, @Nullable RetryPolicy retryPolicyOverride) {
        ClientOperationType opType = ClientUtils.opCodeToClientOperationType(opCode);

        boolean res = shouldRetry(opType, ctx, retryPolicyOverride);

        if (log.isDebugEnabled()) {
            if (res) {
                log.debug("Retrying operation [opCode=" + opCode + ", opType=" + opType + ", attempt=" + ctx.attempt
                        + ", lastError=" + ctx.lastError() + ']');
            } else {
                log.debug("Not retrying operation [opCode=" + opCode + ", opType=" + opType + ", attempt=" + ctx.attempt
                        + ", lastError=" + ctx.lastError() + ']');
            }
        }

        if (res) {
            metrics.requestsRetriedIncrement();
        }

        return res;
    }

    /** Determines whether specified operation should be retried. */
    private boolean shouldRetry(
            @Nullable ClientOperationType opType,
            ClientFutureUtils.RetryContext ctx,
            @Nullable RetryPolicy retryPolicyOverride) {
        var err = ctx.lastError();

        if (err == null) {
            // Closed channel situation - no error, but connection should be retried.
            return opType == ClientOperationType.CHANNEL_CONNECT && ctx.attempt < RetryLimitPolicy.DFLT_RETRY_LIMIT;
        }

        IgniteClientConnectionException exception = unwrapConnectionException(err);

        if (exception == null) {
            return false;
        }

        if (exception.code() == CLUSTER_ID_MISMATCH_ERR) {
            return false;
        }

        if (opType == null) {
            // System operation.
            return ctx.attempt < RetryLimitPolicy.DFLT_RETRY_LIMIT;
        }

        RetryPolicy plc = retryPolicyOverride != null ? retryPolicyOverride : clientCfg.retryPolicy();

        if (plc == null) {
            return false;
        }

        RetryPolicyContext retryPolicyContext = new RetryPolicyContextImpl(clientCfg, opType, ctx.attempt, exception);

        // Exception in shouldRetry will be handled by ClientFutureUtils.doWithRetryAsync
        return plc.shouldRetry(retryPolicyContext);
    }

    /**
     * Establish or repair connections to all configured servers.
     */
    private void initAllChannelsAsync() {
        List<ClientChannelHolder> holders = channels;
        List<CompletableFuture<ClientChannel>> futs = new ArrayList<>(holders.size());

        for (ClientChannelHolder hld : holders) {
            if (closed) {
                return;
            }

            try {
                futs.add(hld.getOrCreateChannelAsync(true));
            } catch (Exception e) {
                log.warn("Failed to establish connection to " + hld.chCfg.getAddress() + ": " + e.getMessage(), e);
            }
        }

        long interval = clientCfg.reconnectInterval();

        if (interval > 0 && !closed) {
            // After current round of connection attempts is finished, schedule the next one with a configured delay.
            CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new))
                    .whenCompleteAsync(
                            (res, err) -> initAllChannelsAsync(),
                            CompletableFuture.delayedExecutor(interval, TimeUnit.MILLISECONDS));
        }
    }

    private void onObservableTimestampReceived(long newTs) {
        observableTimestamp.updateAndGet(curTs -> Math.max(curTs, newTs));
    }

    private void onPartitionAssignmentChanged(long timestamp) {
        partitionAssignmentTimestamp.updateAndGet(curTs -> Math.max(curTs, timestamp));
    }

    /**
     * Gets the last known primary replica start time (for any table).
     *
     * @return Primary replica max start time.
     */
    public long partitionAssignmentTimestamp() {
        return partitionAssignmentTimestamp.get();
    }

    /**
     * Gets the data streamer flush scheduled executor.
     *
     * @return Streamer flush executor.
     */
    public synchronized ScheduledExecutorService streamerFlushExecutor() {
        if (streamerFlushExecutor == null) {
            streamerFlushExecutor = Executors.newSingleThreadScheduledExecutor(
                    new NamedThreadFactory(
                            "client-data-streamer-flush-" + hashCode(),
                            ClientUtils.logger(clientCfg, ReliableChannel.class)));
        }

        return streamerFlushExecutor;
    }

    @Nullable
    private static IgniteClientConnectionException unwrapConnectionException(Throwable err) {
        while (err instanceof CompletionException) {
            err = err.getCause();
        }

        if (!(err instanceof IgniteClientConnectionException)) {
            return null;
        }

        return (IgniteClientConnectionException) err;
    }

    /**
     * Channels holder.
     */
    @SuppressWarnings("PackageVisibleInnerClass") // Visible for tests.
    class ClientChannelHolder {
        /** Channel configuration. */
        private final ClientChannelConfiguration chCfg;

        /** Channel. */
        private volatile @Nullable CompletableFuture<ClientChannel> chFut;

        /** The last server node that channel is or was connected to. */
        private volatile ClusterNode serverNode;

        /** Address that holder is bind to (chCfg.addr) is not in use now. So close the holder. */
        private volatile boolean close;

        /** Timestamps of reconnect retries. */
        private final long[] reconnectRetries;

        /**
         * Constructor.
         *
         * @param chCfg Channel config.
         */
        private ClientChannelHolder(ClientChannelConfiguration chCfg) {
            this.chCfg = chCfg;

            reconnectRetries = chCfg.clientConfiguration().reconnectThrottlingRetries() > 0
                    && chCfg.clientConfiguration().reconnectThrottlingPeriod() > 0L
                    ? new long[chCfg.clientConfiguration().reconnectThrottlingRetries()]
                    : null;
        }

        /**
         * Returns whether reconnect throttling should be applied.
         *
         * @return Whether reconnect throttling should be applied.
         */
        private boolean applyReconnectionThrottling() {
            if (reconnectRetries == null) {
                return false;
            }

            long ts = System.currentTimeMillis();

            for (int i = 0; i < reconnectRetries.length; i++) {
                if (ts - reconnectRetries[i] >= chCfg.clientConfiguration().reconnectThrottlingPeriod()) {
                    reconnectRetries[i] = ts;

                    return false;
                }
            }

            return true;
        }

        /**
         * Get or create channel.
         */
        private CompletableFuture<ClientChannel> getOrCreateChannelAsync() {
            return getOrCreateChannelAsync(false);
        }

        /**
         * Get or create channel.
         */
        private CompletableFuture<ClientChannel> getOrCreateChannelAsync(boolean ignoreThrottling) {
            if (close) {
                return nullCompletedFuture();
            }

            var chFut0 = chFut;

            if (isFutureInProgressOrDoneAndChannelOpen(chFut0)) {
                return chFut0;
            }

            synchronized (this) {
                if (close) {
                    return nullCompletedFuture();
                }

                chFut0 = chFut;

                if (isFutureInProgressOrDoneAndChannelOpen(chFut0)) {
                    return chFut0;
                }

                if (!ignoreThrottling && applyReconnectionThrottling()) {
                    return CompletableFuture.failedFuture(
                            new IgniteClientConnectionException(CONNECTION_ERR, "Reconnect is not allowed due to applied throttling"));
                }

                CompletableFuture<ClientChannel> createFut = chFactory.create(
                        chCfg,
                        connMgr,
                        metrics,
                        ReliableChannel.this::onPartitionAssignmentChanged,
                        ReliableChannel.this::onObservableTimestampReceived);

                chFut0 = createFut.thenApply(ch -> {
                    var oldClusterId = clusterId.compareAndExchange(null, ch.protocolContext().clusterId());

                    if (oldClusterId != null && !oldClusterId.equals(ch.protocolContext().clusterId())) {
                        try {
                            ch.close();
                        } catch (Exception ignored) {
                            // Ignore
                        }

                        throw new IgniteClientConnectionException(
                                CLUSTER_ID_MISMATCH_ERR,
                                "Cluster ID mismatch: expected=" + oldClusterId + ", actual=" + ch.protocolContext().clusterId());
                    }

                    ClusterNode newNode = ch.protocolContext().clusterNode();

                    // There could be multiple holders map to the same serverNodeId if user provide the same
                    // address multiple times in configuration.
                    nodeChannelsByName.put(newNode.name(), this);

                    var oldServerNode = serverNode;
                    if (oldServerNode != null && !oldServerNode.id().equals(newNode.id())) {
                        // New node on the old address.
                        nodeChannelsByName.remove(oldServerNode.name(), this);
                    }

                    serverNode = newNode;

                    return ch;
                });

                chFut0.exceptionally(err -> {
                    closeChannel();
                    onChannelFailure(this, null);

                    log.warn("Failed to establish connection to " + chCfg.getAddress() + ": " + err.getMessage(), err);

                    return null;
                });

                chFut = chFut0;

                return chFut0;
            }
        }

        /**
         * Get channel if connected, or null otherwise.
         */
        private @Nullable ClientChannel getNow() {
            if (close) {
                return null;
            }

            var f = chFut;

            if (f == null) {
                return null;
            }

            var ch = ClientFutureUtils.getNowSafe(f);

            if (ch == null || ch.closed()) {
                return null;
            }

            return ch;
        }

        /**
         * Close channel.
         */
        private synchronized void closeChannel() {
            CompletableFuture<ClientChannel> ch0 = chFut;

            if (ch0 != null) {
                ch0.thenAccept(c -> {
                    try {
                        c.close();
                    } catch (Exception ignored) {
                        // No-op.
                    }
                });

                var oldServerNode = serverNode;

                if (oldServerNode != null) {
                    nodeChannelsByName.remove(oldServerNode.name(), this);
                }

                chFut = null;
            }
        }

        /**
         * Close holder.
         */
        void close() {
            close = true;

            var oldServerNode = serverNode;

            if (oldServerNode != null) {
                nodeChannelsByName.remove(oldServerNode.name(), this);
            }

            closeChannel();
        }

        private boolean isFutureInProgressOrDoneAndChannelOpen(@Nullable CompletableFuture<ClientChannel> f) {
            if (f == null || f.isCompletedExceptionally()) {
                return false;
            }

            if (!f.isDone()) {
                return true;
            }

            var ch = ClientFutureUtils.getNowSafe(f);

            return ch != null && !ch.closed();
        }
    }
}
