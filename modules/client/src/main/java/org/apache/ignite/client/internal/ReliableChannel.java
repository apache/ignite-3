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

package org.apache.ignite.client.internal;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.ignite.client.ClientOp;
import org.apache.ignite.client.IgniteClientAuthenticationException;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.IgniteClientException;
import org.apache.ignite.client.internal.io.ClientConnectionMultiplexer;
import org.apache.ignite.client.internal.io.netty.NettyClientConnectionMultiplexer;
import org.apache.ignite.configuration.schemas.clientconnector.ClientConnectorConfiguration;

/**
 * Communication channel with failover and partition awareness.
 */
final class ReliableChannel implements AutoCloseable {
    /** Do nothing helper function. */
    private static final Consumer<Integer> DO_NOTHING = (v) -> {};

    /** Channel factory. */
    private final BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory;

    /** Client channel holders for each configured address. */
    private volatile List<ClientChannelHolder> channels;

    /** Index of the current channel. */
    private volatile int curChIdx = -1;

    /** Client configuration. */
    private final IgniteClientConfiguration clientCfg;

    /** Node channels. */
    private final Map<UUID, ClientChannelHolder> nodeChannels = new ConcurrentHashMap<>();

    /** Channels reinit was scheduled. */
    private final AtomicBoolean scheduledChannelsReinit = new AtomicBoolean();

    /** Timestamp of start of channels reinitialization. */
    private volatile long startChannelsReInit;

    /** Timestamp of finish of channels reinitialization. */
    private volatile long finishChannelsReInit;

    /** Affinity map update is in progress. */
    private final AtomicBoolean affinityUpdateInProgress = new AtomicBoolean();

    /** Channel is closed. */
    private volatile boolean closed;

    /** Fail (disconnect) listeners. */
    private final ArrayList<Runnable> chFailLsnrs = new ArrayList<>();

    /** Guard channels and curChIdx together. */
    private final ReadWriteLock curChannelsGuard = new ReentrantReadWriteLock();

    /** Connection manager. */
    private final ClientConnectionMultiplexer connMgr;

    /** Cache addresses returned by {@code ThinClientAddressFinder}. */
    private volatile String[] prevHostAddrs;

    /**
     * Constructor.
     */
    ReliableChannel(
            BiFunction<ClientChannelConfiguration, ClientConnectionMultiplexer, ClientChannel> chFactory,
            IgniteClientConfiguration clientCfg
    ) {
        if (chFactory == null)
            throw new NullPointerException("chFactory");

        if (clientCfg == null)
            throw new NullPointerException("clientCfg");

        this.clientCfg = clientCfg;
        this.chFactory = chFactory;

        connMgr = new NettyClientConnectionMultiplexer(clientCfg);
        connMgr.start();
    }

    /** {@inheritDoc} */
    @Override public synchronized void close() {
        closed = true;

        connMgr.stop();

        List<ClientChannelHolder> holders = channels;

        if (holders != null) {
            for (ClientChannelHolder hld: holders)
                hld.close();
        }
    }

    /**
     * Send request and handle response.
     *
     * @throws IgniteClientException Thrown by {@code payloadWriter} or {@code payloadReader}.
     * @throws IgniteClientAuthenticationException When user name or password is invalid.
     */
    public <T> T service(
            ClientOp op,
            Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader
    ) throws IgniteClientException {
        return applyOnDefaultChannel(channel ->
                channel.service(op, payloadWriter, payloadReader)
        );
    }

    /**
     * Send request and handle response asynchronously.
     */
    public <T> CompletableFuture<T> serviceAsync(
            ClientOp op,
            Consumer<PayloadOutputChannel> payloadWriter,
            Function<PayloadInputChannel, T> payloadReader
    ) throws IgniteClientException {
        CompletableFuture<T> fut = new CompletableFuture<>();

        // Use the only one attempt to avoid blocking async method.
        handleServiceAsync(fut, op, payloadWriter, payloadReader, 1, null);

        return fut;
    }

    /**
     * Handles serviceAsync results and retries as needed.
     */
    private <T> void handleServiceAsync(final CompletableFuture<T> fut,
                                        ClientOp op,
                                        Consumer<PayloadOutputChannel> payloadWriter,
                                        Function<PayloadInputChannel, T> payloadReader,
                                        int attemptsLimit,
                                        IgniteClientConnectionException failure) {
        ClientChannel ch;
        // Workaround to store used attempts value within lambda body.
        int attemptsCnt[] = new int[1];

        try {
            ch = applyOnDefaultChannel(channel -> channel, attemptsLimit, v -> attemptsCnt[0] = v );
        } catch (Throwable ex) {
            if (failure != null) {
                failure.addSuppressed(ex);

                fut.completeExceptionally(failure);

                return;
            }

            fut.completeExceptionally(ex);

            return;
        }

        ch
                .serviceAsync(op, payloadWriter, payloadReader)
                .handle((res, err) -> {
                    if (err == null) {
                        fut.complete(res);

                        return null;
                    }

                    IgniteClientConnectionException failure0 = failure;

                    if (err instanceof IgniteClientConnectionException) {
                        try {
                            // Will try to reinit channels if topology changed.
                            onChannelFailure(ch);
                        }
                        catch (Throwable ex) {
                            fut.completeExceptionally(ex);

                            return null;
                        }

                        if (failure0 == null)
                            failure0 = (IgniteClientConnectionException)err;
                        else
                            failure0.addSuppressed(err);

                        int leftAttempts = attemptsLimit - attemptsCnt[0];

                        // If it is a first retry then reset attempts (as for initialization we use only 1 attempt).
                        if (failure == null)
                            leftAttempts = getRetryLimit() - 1;

                        if (leftAttempts > 0) {
                            handleServiceAsync(fut, op, payloadWriter, payloadReader, leftAttempts, failure0);

                            return null;
                        }
                    }
                    else {
                        fut.completeExceptionally(err instanceof IgniteClientException
                                ? err
                                : new IgniteClientException(err.getMessage(), err));

                        return null;
                    }

                    fut.completeExceptionally(failure0);

                    return null;
                });
    }

    /**
     * Send request without payload and handle response.
     */
    public <T> T service(ClientOp op, Function<PayloadInputChannel, T> payloadReader)
            throws IgniteClientException {
        return service(op, null, payloadReader);
    }

    /**
     * Send request without payload and handle response asynchronously.
     */
    public <T> CompletableFuture<T> serviceAsync(ClientOp op, Function<PayloadInputChannel, T> payloadReader)
            throws IgniteClientException {
        return serviceAsync(op, null, payloadReader);
    }

    /**
     * Send request and handle response without payload.
     */
    public void request(ClientOp op, Consumer<PayloadOutputChannel> payloadWriter)
            throws IgniteClientException {
        service(op, payloadWriter, null);
    }

    /**
     * Send request and handle response without payload.
     */
    public CompletableFuture<Void> requestAsync(ClientOp op, Consumer<PayloadOutputChannel> payloadWriter)
            throws IgniteClientException {
        return serviceAsync(op, payloadWriter, null);
    }

    /**
     * @return host:port_range address lines parsed as {@link InetSocketAddress} as a key. Value is the amount of
     * appearences of an address in {@code addrs} parameter.
     */
    private static Map<InetSocketAddress, Integer> parsedAddresses(String[] addrs) throws IgniteClientException {
        if (addrs == null || addrs.length == 0)
            throw new IgniteClientException("Empty addresses");

        Collection<HostAndPortRange> ranges = new ArrayList<>(addrs.length);

        for (String a : addrs) {
            try {
                ranges.add(HostAndPortRange.parse(
                        a,
                        ClientConnectorConfiguration.DFLT_PORT,
                        ClientConnectorConfiguration.DFLT_PORT + ClientConnectorConfiguration.DFLT_PORT_RANGE,
                        "Failed to parse Ignite server address"
                ));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteClientException(e);
            }
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

                if (idx >= holders.size())
                    curChIdx = 0;
                else
                    curChIdx = idx;
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
    private void onChannelFailure(ClientChannelHolder hld, ClientChannel ch) {
        if (ch != null && ch == hld.ch)
            hld.closeChannel();

        chFailLsnrs.forEach(Runnable::run);

        // Roll current channel even if a topology changes. To help find working channel faster.
        rollCurrentChannel(hld);

        if (scheduledChannelsReinit.get())
            channelsInit();
    }

    /**
     * Asynchronously try to establish a connection to all configured servers.
     */
    private void initAllChannelsAsync() {
        ForkJoinPool.commonPool().submit(
                () -> {
                    List<ClientChannelHolder> holders = channels;

                    for (ClientChannelHolder hld : holders) {
                        if (closed || (startChannelsReInit > finishChannelsReInit))
                            return; // New reinit task scheduled or channel is closed.

                        try {
                            hld.getOrCreateChannel(true);
                        }
                        catch (Exception ignore) {
                            // No-op.
                        }
                    }
                }
        );
    }

    /**
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
     * @return boolean wheter channels was reinited.
     */
    synchronized boolean initChannelHolders() {
        List<ClientChannelHolder> holders = channels;

        startChannelsReInit = System.currentTimeMillis();

        // Enable parallel threads to schedule new init of channel holders.
        scheduledChannelsReinit.set(false);

        Map<InetSocketAddress, Integer> newAddrs = null;

        if (clientCfg.getAddressesFinder() != null) {
            String[] hostAddrs = clientCfg.getAddressesFinder().getAddresses();

            if (hostAddrs.length == 0)
                throw new IgniteClientException("Empty addresses");

            if (!Arrays.equals(hostAddrs, prevHostAddrs)) {
                newAddrs = parsedAddresses(hostAddrs);
                prevHostAddrs = hostAddrs;
            }
        } else if (holders == null)
            newAddrs = parsedAddresses(clientCfg.getAddresses());

        if (newAddrs == null) {
            finishChannelsReInit = System.currentTimeMillis();

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

        if (idx != -1)
            currDfltHolder = holders.get(idx);

        for (InetSocketAddress addr : allAddrs) {
            if (shouldStopChannelsReinit())
                return false;

            // Obsolete addr, to be removed.
            if (!newAddrs.containsKey(addr)) {
                curAddrs.get(addr).close();

                continue;
            }

            // Create new holders for new addrs.
            if (!curAddrs.containsKey(addr)) {
                ClientChannelHolder hld = new ClientChannelHolder(new ClientChannelConfiguration(clientCfg, addr));

                for (int i = 0; i < newAddrs.get(addr); i++)
                    reinitHolders.add(hld);

                continue;
            }

            // This holder is up to date.
            ClientChannelHolder hld = curAddrs.get(addr);

            for (int i = 0; i < newAddrs.get(addr); i++)
                reinitHolders.add(hld);

            if (hld == currDfltHolder)
                dfltChannelIdx = reinitHolders.size() - 1;
        }

        if (dfltChannelIdx == -1)
            dfltChannelIdx = new Random().nextInt(reinitHolders.size());

        curChannelsGuard.writeLock().lock();

        try {
            channels = reinitHolders;
            curChIdx = dfltChannelIdx;
        }
        finally {
            curChannelsGuard.writeLock().unlock();
        }

        finishChannelsReInit = System.currentTimeMillis();

        return true;
    }

    /**
     * Establishing connections to servers. If partition awareness feature is enabled connections are created
     * for every configured server. Otherwise only default channel is connected.
     */
    void channelsInit() {
        // Do not establish connections if interrupted.
        if (!initChannelHolders())
            return;

        // Apply no-op function. Establish default channel connection.
        applyOnDefaultChannel(channel -> null);

        if (partitionAwarenessEnabled)
            initAllChannelsAsync();
    }

    /**
     * Apply specified {@code function} on a channel corresponding to specified {@code nodeId}.
     */
    private <T> T applyOnNodeChannel(UUID nodeId, Function<ClientChannel, T> function) {
        ClientChannelHolder hld = null;
        ClientChannel channel = null;

        try {
            hld = nodeChannels.get(nodeId);

            channel = hld != null ? hld.getOrCreateChannel() : null;

            if (channel != null)
                return function.apply(channel);
        } catch (IgniteClientConnectionException e) {
            onChannelFailure(hld, channel);
        }

        return null;
    }

    /** */
    private <T> T applyOnDefaultChannel(Function<ClientChannel, T> function) {
        return applyOnDefaultChannel(function, getRetryLimit(), DO_NOTHING);
    }

    /**
     * Apply specified {@code function} on any of available channel.
     */
    private <T> T applyOnDefaultChannel(Function<ClientChannel, T> function,
                                        int attemptsLimit,
                                        Consumer<Integer> attemptsCallback) {
        IgniteClientConnectionException failure = null;

        for (int attempt = 0; attempt < attemptsLimit; attempt++) {
            ClientChannelHolder hld = null;
            ClientChannel c = null;

            try {
                if (closed)
                    throw new IgniteClientException("Channel is closed");

                curChannelsGuard.readLock().lock();

                try {
                    hld = channels.get(curChIdx);
                } finally {
                    curChannelsGuard.readLock().unlock();
                }

                c = hld.getOrCreateChannel();

                if (c != null) {
                    attemptsCallback.accept(attempt + 1);

                    return function.apply(c);
                }
            }
            catch (IgniteClientConnectionException e) {
                if (failure == null)
                    failure = e;
                else
                    failure.addSuppressed(e);

                onChannelFailure(hld, c);
            }
        }

        throw failure;
    }

    /**
     * Try apply specified {@code function} on a channel corresponding to {@code tryNodeId}.
     * If failed then apply the function on any available channel.
     */
    private <T> T applyOnNodeChannelWithFallback(UUID tryNodeId, Function<ClientChannel, T> function) {
        ClientChannelHolder hld = nodeChannels.get(tryNodeId);

        int retryLimit = getRetryLimit();

        if (hld != null) {
            ClientChannel channel = null;

            try {
                channel = hld.getOrCreateChannel();

                if (channel != null)
                    return function.apply(channel);

            } catch (IgniteClientConnectionException e) {
                onChannelFailure(hld, channel);

                retryLimit -= 1;

                if (retryLimit == 0)
                    throw e;
            }
        }

        return applyOnDefaultChannel(function, retryLimit, DO_NOTHING);
    }

    /** Get retry limit. */
    private int getRetryLimit() {
        List<ClientChannelHolder> holders = channels;

        if (holders == null)
            throw new IgniteClientException("Connections to nodes aren't initialized.");

        int size = holders.size();

        return clientCfg.getRetryLimit() > 0 ? Math.min(clientCfg.getRetryLimit(), size) : size;
    }

    /**
     * Channels holder.
     */
    @SuppressWarnings("PackageVisibleInnerClass") // Visible for tests.
    class ClientChannelHolder {
        /** Channel configuration. */
        private final ClientChannelConfiguration chCfg;

        /** Channel. */
        private volatile ClientChannel ch;

        /** ID of the last server node that {@link ch} is or was connected to. */
        private volatile UUID serverNodeId;

        /** Address that holder is bind to (chCfg.addr) is not in use now. So close the holder. */
        private volatile boolean close;

        /** Timestamps of reconnect retries. */
        private final long[] reconnectRetries;

        /**
         * @param chCfg Channel config.
         */
        private ClientChannelHolder(ClientChannelConfiguration chCfg) {
            this.chCfg = chCfg;

            reconnectRetries = chCfg.getReconnectThrottlingRetries() > 0 && chCfg.getReconnectThrottlingPeriod() > 0L ?
                    new long[chCfg.getReconnectThrottlingRetries()] : null;
        }

        /**
         * @return Whether reconnect throttling should be applied.
         */
        private boolean applyReconnectionThrottling() {
            if (reconnectRetries == null)
                return false;

            long ts = System.currentTimeMillis();

            for (int i = 0; i < reconnectRetries.length; i++) {
                if (ts - reconnectRetries[i] >= chCfg.getReconnectThrottlingPeriod()) {
                    reconnectRetries[i] = ts;

                    return false;
                }
            }

            return true;
        }

        /**
         * Get or create channel.
         */
        private ClientChannel getOrCreateChannel()
                throws IgniteClientConnectionException, IgniteClientAuthenticationException {
            return getOrCreateChannel(false);
        }

        /**
         * Get or create channel.
         */
        private ClientChannel getOrCreateChannel(boolean ignoreThrottling)
                throws IgniteClientConnectionException, IgniteClientAuthenticationException {
            if (ch == null && !close) {
                synchronized (this) {
                    if (close)
                        return null;

                    if (ch != null)
                        return ch;

                    if (!ignoreThrottling && applyReconnectionThrottling())
                        throw new IgniteClientConnectionException("Reconnect is not allowed due to applied throttling");

                    ClientChannel channel = chFactory.apply(chCfg, connMgr);

                    if (channel.serverNodeId() != null) {
                        UUID prevId = serverNodeId;

                        if (prevId != null && !prevId.equals(channel.serverNodeId()))
                            nodeChannels.remove(prevId, this);

                        if (!channel.serverNodeId().equals(prevId)) {
                            serverNodeId = channel.serverNodeId();

                            // There could be multiple holders map to the same serverNodeId if user provide the same
                            // address multiple times in configuration.
                            nodeChannels.putIfAbsent(channel.serverNodeId(), this);
                        }
                    }

                    ch = channel;
                }
            }

            return ch;
        }

        /**
         * Close channel.
         */
        private synchronized void closeChannel() {
            if (ch != null) {
                try {
                    ch.close();
                } catch (Exception ignored) {
                }

                ch = null;
            }
        }

        /**
         * Close holder.
         */
        void close() {
            close = true;

            if (serverNodeId != null)
                nodeChannels.remove(serverNodeId, this);

            closeChannel();
        }

        /**
         * Wheteher the holder is closed. For test purposes.
         */
        boolean isClosed() {
            return close;
        }

        /**
         * Get address of the channel. For test purposes.
         */
        InetSocketAddress getAddress() {
            return chCfg.getAddress();
        }
    }

    /**
     * Get holders reference. For test purposes.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") // For tests.
    List<ClientChannelHolder> getChannelHolders() {
        return channels;
    }

    /**
     * Get node channels reference. For test purposes.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType") // For tests.
    Map<UUID, ClientChannelHolder> getNodeChannels() {
        return nodeChannels;
    }

    /**
     * Get scheduledChannelsReinit reference. For test purposes.
     */
    AtomicBoolean getScheduledChannelsReinit() {
        return scheduledChannelsReinit;
    }
}
