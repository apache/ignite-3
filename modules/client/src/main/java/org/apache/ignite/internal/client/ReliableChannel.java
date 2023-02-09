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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.ClientOperationType;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.client.RetryPolicy;
import org.apache.ignite.client.RetryPolicyContext;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.netty.NettyClientConnectionMultiplexer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Communication channel with failover and partition awareness.
 */
public final class ReliableChannel implements AutoCloseable {
    /** Channel factory. */
    private final BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, CompletableFuture<ClientChannel>> chFactory;

    /** Client channel holders for each configured address. */
    private volatile List<ClientChannelHolder> channels;

    /** Index of the current channel. */
    private volatile int curChIdx = -1;

    /** Client configuration. */
    private final IgniteClientConfiguration clientCfg;

    /** Node channels. */
    private final Map<String, ClientChannelHolder> nodeChannelsByName = new ConcurrentHashMap<>();

    /** Node channels. */
    private final Map<String, ClientChannelHolder> nodeChannelsById = new ConcurrentHashMap<>();

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

    /** Local topology assignment version. Instead of using event handlers to notify all tables about assignment change,
     * the table will compare its version with channel version to detect an update. */
    private final AtomicLong assignmentVersion = new AtomicLong();

    /** Cluster id from the first handshake. */
    private final AtomicReference<UUID> clusterId = new AtomicReference<>();

    /**
     * Constructor.
     *
     * @param chFactory Channel factory.
     * @param clientCfg Client config.
     */
    ReliableChannel(BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, CompletableFuture<ClientChannel>> chFactory,
            IgniteClientConfiguration clientCfg) {
        this.clientCfg = Objects.requireNonNull(clientCfg, "clientCfg");
        this.chFactory = Objects.requireNonNull(chFactory, "chFactory");
        this.log = ClientUtils.logger(clientCfg, ReliableChannel.class);

        connMgr = new NettyClientConnectionMultiplexer();
        connMgr.start(clientCfg);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void close() {
        closed = true;

        connMgr.stop();

        List<ClientChannelHolder> holders = channels;

        if (holders != null) {
            for (ClientChannelHolder hld : holders) {
                hld.close();
            }
        }
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

                if (ch != null) {
                    res.add(ch.protocolContext().clusterNode());
                }
            }
        }

        return res;
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
     * @param preferredNodeId   ID of the preferred target node. When a connection to the specified node exists,
     *                          it will be used to handle the request; otherwise, default connection will be used.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            PayloadWriter payloadWriter,
            PayloadReader<T> payloadReader,
            @Nullable String preferredNodeName,
            @Nullable String preferredNodeId
    ) {
        return ClientFutureUtils.doWithRetryAsync(
                () -> getChannelAsync(preferredNodeName, preferredNodeId)
                        .thenCompose(ch -> serviceAsyncInternal(opCode, payloadWriter, payloadReader, ch)),
                null,
                ctx -> shouldRetry(opCode, ctx));
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
            PayloadReader<T> payloadReader
    ) {
        return serviceAsync(opCode, payloadWriter, payloadReader, null, null);
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
        return serviceAsync(opCode, null, payloadReader, null, null);
    }

    private <T> CompletableFuture<T> serviceAsyncInternal(
            int opCode,
            PayloadWriter payloadWriter,
            PayloadReader<T> payloadReader,
            ClientChannel ch) {
        return ch.serviceAsync(opCode, payloadWriter, payloadReader).whenComplete((res, err) -> {
            if (err != null && unwrapConnectionException(err) != null) {
                onChannelFailure(ch);
            }
        });
    }

    private CompletableFuture<ClientChannel> getChannelAsync(@Nullable String preferredNodeName, @Nullable String preferredNodeId) {
        ClientChannelHolder holder = null;

        if (preferredNodeName != null) {
            holder = nodeChannelsByName.get(preferredNodeName);
        } else if (preferredNodeId != null) {
            holder = nodeChannelsById.get(preferredNodeId);
        }

        if (holder != null) {
            return holder.getOrCreateChannelAsync().thenCompose(ch -> {
                if (ch != null)
                    return CompletableFuture.completedFuture(ch);
                else
                    return getDefaultChannelAsync();
            });
        }

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

        Collection<HostAndPortRange> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            ranges.add(HostAndPortRange.parse(
                    a,
                    IgniteClientConfiguration.DFLT_PORT,
                    IgniteClientConfiguration.DFLT_PORT + IgniteClientConfiguration.DFLT_PORT_RANGE,
                    "Failed to parse Ignite server address"
            ));
        }

        return ranges.stream()
                .flatMap(r -> IntStream
                        .rangeClosed(r.portFrom(), r.portTo()).boxed()
                        .map(p -> InetSocketAddress.createUnresolved(r.host(), p))
                )
                .collect(Collectors.toMap(a -> a, a -> 1, Integer::sum));
    }

    /**
     * Roll current default channel if specified holder equals to it.
     */
    private void rollCurrentChannel(ClientChannelHolder hld) {
        curChannelsGuard.writeLock().lock();

        try {
            int idx = curChIdx;
            List<ClientChannelHolder> holders = channels;

            ClientChannelHolder dfltHld = holders.get(idx);

            if (dfltHld == hld) {
                idx += 1;

                if (idx >= holders.size()) {
                    curChIdx = 0;
                } else {
                    curChIdx = idx;
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
        // There is nothing wrong if curChIdx was concurrently changed, since channel was closed by another thread
        // when current index was changed and no other wrong channel will be closed by current thread because
        // onChannelFailure checks channel binded to the holder before closing it.
        onChannelFailure(channels.get(curChIdx), ch);
    }

    /**
     * On channel of the specified holder failure.
     */
    private void onChannelFailure(ClientChannelHolder hld, @Nullable ClientChannel ch) {
        if (ch != null && ch == hld.chFut) {
            hld.closeChannel();
        }

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
                //noinspection NonPrivateFieldAccessedInSynchronizedContext
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

        int idx = curChIdx;

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
            curChIdx = dfltChannelIdx;
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
            return CompletableFuture.completedFuture(null);
        }

        // Establish default channel connection.
        var fut = getDefaultChannelAsync();

        // Establish secondary connections in the background.
        fut.thenAccept(unused -> initAllChannelsAsync());

        return fut;
    }

    /**
     * Gets the default channel, reconnecting if necessary.
     */
    private CompletableFuture<ClientChannel> getDefaultChannelAsync() {
        return ClientFutureUtils.doWithRetryAsync(() -> {
                    curChannelsGuard.readLock().lock();

                    ClientChannelHolder hld;

                    try {
                        hld = channels.get(curChIdx);
                    } finally {
                        curChannelsGuard.readLock().unlock();
                    }

                    return hld.getOrCreateChannelAsync();
                },
                Objects::nonNull,
                ctx -> shouldRetry(ClientOperationType.CHANNEL_CONNECT, ctx));
    }

    private CompletableFuture<ClientChannel> getCurChannelAsync() {
        if (closed) {
            return CompletableFuture.failedFuture(new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed"));
        }

        curChannelsGuard.readLock().lock();

        try {
            var hld = channels.get(curChIdx);

            if (hld == null) {
                return CompletableFuture.completedFuture(null);
            }

            CompletableFuture<ClientChannel> fut = hld.getOrCreateChannelAsync();
            return fut == null ? CompletableFuture.completedFuture(null) : fut;
        } finally {
            curChannelsGuard.readLock().unlock();
        }
    }

    /** Determines whether specified operation should be retried. */
    private boolean shouldRetry(int opCode, ClientFutureUtils.RetryContext ctx) {
        ClientOperationType opType = ClientUtils.opCodeToClientOperationType(opCode);

        return shouldRetry(opType, ctx);
    }

    /** Determines whether specified operation should be retried. */
    private boolean shouldRetry(@Nullable ClientOperationType opType, ClientFutureUtils.RetryContext ctx) {
        var err = ctx.lastError();

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

        RetryPolicy plc = clientCfg.retryPolicy();

        if (plc == null) {
            return false;
        }

        RetryPolicyContext retryPolicyContext = new RetryPolicyContextImpl(clientCfg, opType, ctx.attempt, exception);

        // Exception in shouldRetry will be handled by ClientFutureUtils.doWithRetryAsync
        boolean shouldRetry = plc.shouldRetry(retryPolicyContext);

        if (shouldRetry) {
            log.debug("Going to retry operation because of error [op={}, currentAttempt={}, errMsg={}]",
                    exception, opType, ctx.attempt, exception.getMessage());
        }

        return shouldRetry;
    }

    /**
     * Asynchronously try to establish a connection to all configured servers.
     */
    private void initAllChannelsAsync() {
        ForkJoinPool.commonPool().submit(
                () -> {
                    List<ClientChannelHolder> holders = channels;

                    for (ClientChannelHolder hld : holders) {
                        if (closed) {
                            return; // New reinit task scheduled or channel is closed.
                        }

                        try {
                            hld.getOrCreateChannelAsync(true);
                        } catch (Exception e) {
                            log.warn("Failed to establish connection to " + hld.chCfg.getAddress() + ": " + e.getMessage(), e);
                        }
                    }
                }
        );
    }

    private void onTopologyAssignmentChanged(ClientChannel clientChannel) {
        // NOTE: Multiple channels will send the same update to us, resulting in multiple cache invalidations.
        // This could be solved with a cluster-wide AssignmentVersion, but we don't have that.
        // So we only react to updates from the default channel. When no user-initiated operations are performed on the default
        // channel, heartbeat messages will trigger updates.
        CompletableFuture<ClientChannel> ch = channels.get(curChIdx).chFut;

        if (ch != null && clientChannel == ClientFutureUtils.getNowSafe(ch)) {
            assignmentVersion.incrementAndGet();
        }
    }

    /**
     * Gets the local partition assignment version.
     *
     * @return Assignment version.
     */
    public long partitionAssignmentVersion() {
        return assignmentVersion.get();
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
                return CompletableFuture.completedFuture(null);
            }

            var chFut0 = chFut;

            if (isDoneAndOpenOrInProgress(chFut0)) {
                return chFut0;
            }

            synchronized (this) {
                if (close) {
                    return CompletableFuture.completedFuture(null);
                }

                chFut0 = chFut;

                if (isDoneAndOpenOrInProgress(chFut0)) {
                    return chFut0;
                }

                if (!ignoreThrottling && applyReconnectionThrottling()) {
                    return CompletableFuture.failedFuture(
                            new IgniteClientConnectionException(CONNECTION_ERR, "Reconnect is not allowed due to applied throttling"));
                }

                chFut0 = chFactory.apply(chCfg, connMgr).thenApply(ch -> {
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

                    ch.addTopologyAssignmentChangeListener(ReliableChannel.this::onTopologyAssignmentChanged);

                    ClusterNode newNode = ch.protocolContext().clusterNode();

                    // There could be multiple holders map to the same serverNodeId if user provide the same
                    // address multiple times in configuration.
                    nodeChannelsByName.put(newNode.name(), this);
                    nodeChannelsById.put(newNode.id(), this);

                    var oldServerNode = serverNode;
                    if (oldServerNode != null && !oldServerNode.id().equals(newNode.id())) {
                        // New node on the old address.
                        nodeChannelsByName.remove(oldServerNode.name(), this);
                        nodeChannelsById.remove(oldServerNode.id(), this);
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
                    nodeChannelsById.remove(oldServerNode.id(), this);
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
                nodeChannelsById.remove(oldServerNode.id(), this);
            }

            closeChannel();
        }

        private boolean isDoneAndOpenOrInProgress(@Nullable CompletableFuture<ClientChannel> f) {
            if (f == null || f.isCompletedExceptionally()) {
                return false;
            }

            if (!f.isDone()) {
                return true;
            }

            var ch = f.getNow(null);

            return ch != null && !ch.closed();
        }
    }
}
