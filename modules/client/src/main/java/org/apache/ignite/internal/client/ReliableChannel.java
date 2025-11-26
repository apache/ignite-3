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

import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.delayedExecutor;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCauseOrSuppressed;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Client.CLUSTER_ID_MISMATCH_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.CONFIGURATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Client.CONNECTION_ERR;

import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
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
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.apache.ignite.client.ClientOperationType;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.RetryLimitPolicy;
import org.apache.ignite.client.RetryPolicy;
import org.apache.ignite.client.RetryPolicyContext;
import org.apache.ignite.internal.client.io.ClientConnectionMultiplexer;
import org.apache.ignite.internal.client.io.netty.NettyClientConnectionMultiplexer;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.HybridTimestampTracker;
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
    private final IgniteClientConfigurationImpl clientCfg;

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
    private final HybridTimestampTracker observableTimeTracker;

    /** Cluster id from the first handshake. */
    private final AtomicReference<UUID> clusterId = new AtomicReference<>();

    /** Scheduled executor for streamer flush. */
    @Nullable
    private ScheduledExecutorService streamerFlushExecutor;

    /** Inflights. */
    private final ClientTransactionInflights inflights;

    /** Address resolver. */
    private final InetAddressResolver addressResolver;

    /**
     * A validator that is called when a connection to a node is established,
     * if it throws an exception, the network channel to that node will be closed.
     */
    private final @Nullable ChannelValidator channelValidator;

    /**
     * Constructor.
     *
     * @param chFactory Channel factory.
     * @param clientCfg Client config.
     * @param metrics Client metrics.
     * @param observableTimeTracker Tracker of the latest time observed by client.
     * @param channelValidator A validator that is called when a connection to a node is established,
     *                         if it throws an exception, the network channel to that node will be closed.
     */
    ReliableChannel(
            ClientChannelFactory chFactory,
            IgniteClientConfigurationImpl clientCfg,
            ClientMetricSource metrics,
            HybridTimestampTracker observableTimeTracker,
            @Nullable ChannelValidator channelValidator
    ) {
        this.clientCfg = Objects.requireNonNull(clientCfg, "clientCfg");
        this.chFactory = Objects.requireNonNull(chFactory, "chFactory");
        this.log = ClientUtils.logger(clientCfg, ReliableChannel.class);
        this.metrics = metrics;
        this.observableTimeTracker = Objects.requireNonNull(observableTimeTracker, "observableTime");
        this.channelValidator = channelValidator;

        connMgr = new NettyClientConnectionMultiplexer(metrics);
        connMgr.start(clientCfg);

        inflights = new ClientTransactionInflights();

        addressResolver = requireNonNullElse(clientCfg.addressResolver(), InetAddressResolver.DEFAULT);
    }

    /** {@inheritDoc} */
    @Override
    public synchronized void close() throws Exception {
        closed = true;

        List<ClientChannelHolder> holders = channels;

        List<ManuallyCloseable> closeables = new ArrayList<>();
        if (holders != null) {
            for (ClientChannelHolder hld : holders) {
                closeables.add(hld::close);
            }
        }
        closeables.add(connMgr::stop);
        closeables.add(() -> shutdownAndAwaitTermination(streamerFlushExecutor, 10, TimeUnit.SECONDS));
        IgniteUtils.closeAllManually(closeables);
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

    public HybridTimestampTracker observableTimestamp() {
        return observableTimeTracker;
    }

    public UUID clusterId() {
        return clusterId.get();
    }

    /**
     * Gets active client channels.
     *
     * @return List of connected channels.
     */
    public List<ClientChannel> channels() {
        List<ClientChannel> res = new ArrayList<>(channels.size());

        for (var holder : nodeChannelsByName.values()) {
            var chFut = holder.chFut;

            if (chFut != null) {
                ClientChannel ch = ClientFutureUtils.getNowSafe(chFut);

                if (ch != null && !ch.closed()) {
                    res.add(ch);
                }
            }
        }

        return res;
    }

    /**
     * Sends request and handles response asynchronously.
     *
     * @param <T> response type.
     * @param opCode Operation code.
     * @param payloadWriter Payload writer.
     * @param payloadReader Payload reader.
     * @param preferredNodeName Unique name (consistent id) of the preferred target node. When a connection to the specified node
     *         exists, it will be used to handle the request; otherwise, default connection will be used.
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
        return ClientFutureUtils.doWithRetryAsync(
                () -> getChannelAsync(preferredNodeName)
                        .thenCompose(ch -> serviceAsyncInternal(opCode, payloadWriter, payloadReader, expectNotifications, ch)),
                ctx -> shouldRetry(opCode, ctx, retryPolicyOverride));
    }

    /**
     * Sends request and handles response asynchronously.
     *
     * @param opCode Operation code.
     * @param payloadWriter Payload writer.
     * @param payloadReader Payload reader.
     * @param <T> response type.
     * @param channelResolver Channel resolver.
     * @param retryPolicyOverride Retry policy override.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader,
            Supplier<CompletableFuture<ClientChannel>> channelResolver,
            @Nullable RetryPolicy retryPolicyOverride,
            boolean expectNotifications
    ) {
        return ClientFutureUtils.doWithRetryAsync(
                () -> channelResolver.get()
                        .thenCompose(ch -> serviceAsyncInternal(opCode, payloadWriter, payloadReader, expectNotifications, ch)),
                ctx -> shouldRetry(opCode, ctx, retryPolicyOverride));
    }

    /**
     * Sends request and handles response asynchronously.
     *
     * @param opCodeFunc Function that returns opCode.
     * @param retryOpType OpCode to use in retry.
     * @param payloadWriter Payload writer.
     * @param payloadReader Payload reader.
     * @param <T> response type.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(
            ToIntFunction<ClientChannel> opCodeFunc,
            int retryOpType,
            @Nullable PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader
    ) {
        return ClientFutureUtils.doWithRetryAsync(
                () -> getChannelAsync(null)
                        .thenCompose(ch -> {
                            int opCode = opCodeFunc.applyAsInt(ch);
                            return serviceAsyncInternal(opCode, payloadWriter, payloadReader, false, ch);
                        }),
                ctx -> shouldRetry(retryOpType, ctx, null));
    }

    /**
     * Sends request and handles response asynchronously.
     *
     * @param opCode Operation code.
     * @param payloadWriter Payload writer.
     * @param payloadReader Payload reader.
     * @param <T> response type.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(
            int opCode,
            PayloadWriter payloadWriter,
            @Nullable PayloadReader<T> payloadReader
    ) {
        return serviceAsync(opCode, payloadWriter, payloadReader, (String) null, null, false);
    }

    /**
     * Sends request without payload and handles response asynchronously.
     *
     * @param opCode Operation code.
     * @param payloadReader Payload reader.
     * @param <T> Response type.
     * @return Future for the operation.
     */
    public <T> CompletableFuture<T> serviceAsync(int opCode, PayloadReader<T> payloadReader) {
        return serviceAsync(opCode, null, payloadReader, (String) null, null, false);
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

    /**
     * Get the channel.
     *
     * @param preferredNodeName Preferred node name.
     *
     * @return The future.
     */
    public CompletableFuture<ClientChannel> getChannelAsync(@Nullable String preferredNodeName) {
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
     * in {@code addrs} parameter.
     *
     * @return host:port_range address lines parsed as {@link InetSocketAddress} as a key. Value is the amount of appearences of an address
     *         in {@code addrs} parameter.
     */
    private static Map<InetSocketAddress, Integer> parsedAddresses(InetAddressResolver addressResolver, String[] addrs) {
        if (addrs == null || addrs.length == 0) {
            throw new IgniteException(CONFIGURATION_ERR, "Empty addresses");
        }

        Collection<HostAndPort> parsedAddrs = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            parsedAddrs.add(HostAndPort.parse(a, IgniteClientConfiguration.DFLT_PORT, "Failed to parse Ignite server address"));
        }

        var map = new HashMap<InetSocketAddress, Integer>(parsedAddrs.size());

        for (HostAndPort addr : parsedAddrs) {
            try {
                // Special handling for "localhost" to avoid unnecessary DNS resolution.
                if ("localhost".equalsIgnoreCase(addr.host())) {
                    map.merge(InetSocketAddress.createUnresolved(addr.host(), addr.port()), 1, Integer::sum);

                    continue;
                }

                for (InetAddress inetAddr : addressResolver.getAllByName(addr.host())) {
                    // Preserve unresolved address if the resolved address equals to the original host string.
                    if (Objects.equals(addr.host(), inetAddr.getHostAddress())) {
                        map.merge(InetSocketAddress.createUnresolved(addr.host(), addr.port()), 1, Integer::sum);

                        continue;
                    }

                    var sockAddr = new InetSocketAddress(inetAddr, addr.port());
                    map.merge(sockAddr, 1, Integer::sum);
                }
            } catch (UnknownHostException e) {
                var sockAddr = InetSocketAddress.createUnresolved(addr.host(), addr.port());
                map.merge(sockAddr, 1, Integer::sum);
            }
        }

        return map;
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

        if (scheduledChannelsReinit.compareAndSet(false, true)) {
            // Refresh addresses and reinit channels.
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
     * Get inflights instance.
     *
     * @return The instance.
     */
    public ClientTransactionInflights inflights() {
        return inflights;
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
     * @return boolean whether channels were reinitialized.
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
                newAddrs = parsedAddresses(addressResolver, hostAddrs);
                prevHostAddrs = hostAddrs;
            }
        } else {
            // Re-resolve DNS.
            newAddrs = parsedAddresses(addressResolver, clientCfg.addresses());
        }

        if (newAddrs == null) {
            return true;
        }

        Map<InetSocketAddress, ClientChannelHolder> curAddrs = new HashMap<>();
        Set<InetSocketAddress> allAddrs = new HashSet<>(newAddrs.keySet());

        if (holders != null) {
            for (ClientChannelHolder h : holders) {
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

        // Schedule the background re-resolve of addresses.
        if (clientCfg.backgroundReResolveAddressesInterval() > 0L) {
            supplyAsync(
                    this::initChannelHolders,
                    delayedExecutor(clientCfg.backgroundReResolveAddressesInterval(), TimeUnit.MILLISECONDS)
            );
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
                ctx -> shouldRetry(ClientOperationType.CHANNEL_CONNECT, ctx, null));
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
                futs.add(hld.getOrCreateChannelAsync());
            } catch (Exception e) {
                logFailedEstablishConnection(hld, e);
            }
        }

        long interval = clientCfg.backgroundReconnectInterval();

        if (interval > 0 && !closed) {
            // After current round of connection attempts is finished, schedule the next one with a configured delay.
            CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new))
                    .whenCompleteAsync(
                            (res, err) -> initAllChannelsAsync(),
                            delayedExecutor(interval, TimeUnit.MILLISECONDS));
        }
    }

    private void onObservableTimestampReceived(long newTs) {
        observableTimeTracker.update(HybridTimestamp.nullableHybridTimestamp(newTs));
    }

    private void onPartitionAssignmentChanged(long timestamp) {
        var old = partitionAssignmentTimestamp.getAndUpdate(curTs -> Math.max(curTs, timestamp));

        if (timestamp > old) {
            // New assignment timestamp, topology change possible.
            if (scheduledChannelsReinit.compareAndSet(false, true)) {
                channelsInitAsync();
            }
        }
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
        err = unwrapCause(err);

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

        /**
         * Constructor.
         *
         * @param chCfg Channel config.
         */
        private ClientChannelHolder(ClientChannelConfiguration chCfg) {
            this.chCfg = chCfg;
        }

        /**
         * Get or create channel.
         */
        private CompletableFuture<ClientChannel> getOrCreateChannelAsync() {
            if (close) {
                return failedFuture(
                        new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed", chCfg.getAddress().toString()));
            }

            var chFut0 = chFut;

            if (isFutureInProgressOrDoneAndChannelOpen(chFut0)) {
                return chFut0;
            }

            synchronized (this) {
                if (close) {
                    return failedFuture(
                            new IgniteClientConnectionException(CONNECTION_ERR, "Channel is closed", chCfg.getAddress().toString()));
                }

                chFut0 = chFut;

                if (isFutureInProgressOrDoneAndChannelOpen(chFut0)) {
                    return chFut0;
                }

                CompletableFuture<ClientChannel> createFut = chFactory.create(
                        chCfg,
                        connMgr,
                        metrics,
                        ReliableChannel.this::onPartitionAssignmentChanged,
                        ReliableChannel.this::onObservableTimestampReceived,
                        inflights);

                chFut0 = createFut.thenApply(ch -> {
                    if (channelValidator != null) {
                        channelValidator.validate(ch.protocolContext());
                    }

                    UUID currentClusterId = ch.protocolContext().clusterId();
                    UUID oldClusterId = clusterId.compareAndExchange(null, currentClusterId);
                    List<UUID> validClusterIds = ch.protocolContext().clusterIds();

                    if (oldClusterId != null && !validClusterIds.contains(oldClusterId)) {
                        try {
                            ch.close();
                        } catch (Exception ignored) {
                            // Ignore
                        }

                        String clusterIdsString = validClusterIds.stream()
                                .map(UUID::toString)
                                .collect(Collectors.joining(", "));

                        throw new IgniteClientConnectionException(
                                CLUSTER_ID_MISMATCH_ERR,
                                "Cluster ID mismatch: expected=" + oldClusterId + ", actual=" + clusterIdsString,
                                ch.endpoint());
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

                    logFailedEstablishConnection(this, err);

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

        private void rollNodeChannelsByName() {
            List<ClientChannelHolder> holders = channels;

            for (ClientChannelHolder h : holders) {
                if (h != this && h.serverNode != null && Objects.equals(serverNode.id(), h.serverNode.id())) {
                    nodeChannelsByName.put(h.serverNode.name(), h);

                    return;
                }
            }

            nodeChannelsByName.remove(serverNode.name(), this);
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
                    rollNodeChannelsByName();
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
                rollNodeChannelsByName();
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

    private void logFailedEstablishConnection(ClientChannelHolder ch, Throwable err) {
        String logMessage = "Failed to establish connection to {}: {}";

        if (isLogFailedEstablishConnectionExceptionStackTrace(err)) {
            log.warn(logMessage, err, ch.chCfg.getAddress(), err.getMessage());
        } else {
            log.info(logMessage, ch.chCfg.getAddress(), err.getMessage());
        }
    }

    /**
     * Returns {@code true} if need to log the stack trace of the error, since the error is unexpected and will need to be dealt with later,
     * otherwise {@code false} and means that the exception is expected and there is not need to log its stack trace so as not to worry when
     * analyzing the log.
     */
    private static boolean isLogFailedEstablishConnectionExceptionStackTrace(Throwable err) {
        // May occur when nodes are restarted, which is expected.
        return !hasCauseOrSuppressed(err, "Connection refused", ConnectException.class);
    }
}
