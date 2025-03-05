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

package org.apache.ignite.internal.network;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static org.apache.ignite.internal.util.FastTimestamps.coarseCurrentTimeMillis;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketTimeoutException;
import java.net.StandardSocketOptions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Multicast-based IP finder.
 *
 * <p>When TCP discovery starts this finder sends multicast request and waits for some time when other nodes
 * reply to this request with messages containing their addresses.
 */
public class MulticastNodeFinder implements NodeFinder {
    private static final IgniteLogger LOG = Loggers.forClass(MulticastNodeFinder.class);

    /** Discovery request message. */
    private static final byte[] REQUEST_MESSAGE = "IGNI".getBytes();

    /** Buffer size for receiving responses. */
    private static final int RECEIVE_BUFFER_SIZE = 1024;

    private static final int REQ_ATTEMPTS = 2;

    private final InetSocketAddress multicastSocketAddress;
    private final int multicastPort;
    private final int resultWaitMillis;
    private final int ttl;

    private final InetSocketAddress localAddress;
    private final ExecutorService threadPool;

    /** Flag to control running state of listener tasks. */
    private volatile boolean stopped = false;

    /** Listener tasks for each eligible interface. */
    private final List<CompletableFuture<Void>> listenerFutures = new ArrayList<>();

    /**
     * Constructs a new multicast node finder.
     *
     * @param multicastGroup Multicast group.
     * @param multicastPort Multicast port.
     * @param resultWaitMillis Wait time for responses.
     * @param ttl Time-to-live for multicast packets.
     * @param localAddress Local node address.
     */
    public MulticastNodeFinder(
            String multicastGroup,
            int multicastPort,
            int resultWaitMillis,
            int ttl,
            InetSocketAddress localAddress
    ) {
        this.multicastSocketAddress = new InetSocketAddress(multicastGroup, multicastPort);
        this.multicastPort = multicastPort;
        this.resultWaitMillis = resultWaitMillis;
        this.ttl = ttl;
        this.localAddress = localAddress;
        this.threadPool = Executors.newFixedThreadPool(4);
    }

    @Override
    public Collection<NetworkAddress> findNodes() {
        Set<NetworkAddress> result = new HashSet<>();
        List<CompletableFuture<Collection<NetworkAddress>>> discoveryFutures = new ArrayList<>();

        for (NetworkInterface networkInterface : getEligibleNetworkInterfaces()) {
            discoveryFutures.add(supplyAsync(() -> discoverOnInterface(networkInterface), threadPool));
        }

        for (CompletableFuture<Collection<NetworkAddress>> future : discoveryFutures) {
            try {
                result.addAll(future.join());
            } catch (Exception e) {
                LOG.error("Error during node discovery", e);
            }
        }

        if (result.isEmpty()) {
            LOG.warn("No nodes discovered on interfaces, using unbound multicast socket");
            result.addAll(discoverOnInterface(null));
        }

        LOG.info("Discovered nodes: {}", result);

        return result;
    }

    private Collection<NetworkAddress> discoverOnInterface(@Nullable NetworkInterface networkInterface) {
        Set<NetworkAddress> discovered = new HashSet<>();
        byte[] responseBuffer = new byte[RECEIVE_BUFFER_SIZE];

        try (MulticastSocket socket = new MulticastSocket(0)) {
            configureSocket(socket, networkInterface);

            for (int i = 0; i < REQ_ATTEMPTS; i++) {
                DatagramPacket requestPacket = new DatagramPacket(REQUEST_MESSAGE, REQUEST_MESSAGE.length);
                requestPacket.setSocketAddress(multicastSocketAddress);
                socket.send(requestPacket);

                waitForResponses(responseBuffer, socket, discovered);
            }
        } catch (Exception e) {
            LOG.error("Error during discovery on interface: " + networkInterface, e);
        }

        return discovered;
    }

    private void waitForResponses(byte[] responseBuffer, MulticastSocket socket, Set<NetworkAddress> discovered) throws IOException {
        long endTime = coarseCurrentTimeMillis() + resultWaitMillis;
        while (coarseCurrentTimeMillis() < endTime) {
            DatagramPacket responsePacket = new DatagramPacket(responseBuffer, responseBuffer.length);

            try {
                socket.receive(responsePacket);
                byte[] data = Arrays.copyOfRange(
                        responsePacket.getData(),
                        responsePacket.getOffset(),
                        responsePacket.getOffset() + responsePacket.getLength()
                );

                InetSocketAddress address = ByteUtils.fromBytes(data);
                if (!address.equals(localAddress)) {
                    discovered.add(NetworkAddress.from(address));
                }
            } catch (SocketTimeoutException ignored) {
                // No-op.
            }
        }
    }

    private void configureSocket(MulticastSocket socket, @Nullable NetworkInterface networkInterface) throws IOException {
        socket.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true);

        if (networkInterface != null) {
            socket.setNetworkInterface(networkInterface);
        }

        socket.setSoTimeout(resultWaitMillis);

        if (ttl != -1) {
            socket.setTimeToLive(ttl);
        }
    }

    /**
     * Listens on a given network interface for multicast discovery requests and responds with this node's address.
     *
     * @param networkInterface The network interface to listen on.
     */
    private void listenOnInterface(NetworkInterface networkInterface) {
        try (MulticastSocket socket = new MulticastSocket(multicastPort)) {
            configureSocket(socket, networkInterface);
            socket.joinGroup(multicastSocketAddress, networkInterface);

            byte[] responseData = ByteUtils.toBytes(localAddress);
            byte[] requestBuffer = new byte[REQUEST_MESSAGE.length];

            while (!stopped) {
                DatagramPacket requestPacket = new DatagramPacket(requestBuffer, requestBuffer.length);
                try {
                    socket.receive(requestPacket);

                    byte[] received = Arrays.copyOfRange(
                            requestPacket.getData(),
                            requestPacket.getOffset(),
                            requestPacket.getOffset() + requestPacket.getLength()
                    );

                    if (!Arrays.equals(received, REQUEST_MESSAGE)) {
                        LOG.error("Got unexpected request on multicast socket");
                        continue;
                    }

                    DatagramPacket responsePacket = new DatagramPacket(
                            responseData,
                            responseData.length,
                            requestPacket.getAddress(),
                            requestPacket.getPort()
                    );

                    socket.send(responsePacket);
                } catch (SocketTimeoutException ignored) {
                    // Timeout to check the running flag.
                }
            }
        } catch (Exception e) {
            if (!stopped) {
                LOG.error("Error in multicast listener on interface: " + networkInterface, e);
            } else {
                LOG.info("Multicast listener shutting down on interface: " + networkInterface);
            }
        }
    }

    /**
     * Returns a collection of eligible network interfaces that are up, non‑loopback, and support multicast.
     *
     * @return Collection of eligible network interfaces.
     */
    private static Collection<NetworkInterface> getEligibleNetworkInterfaces() {
        Set<NetworkInterface> eligible = new HashSet<>();
        try {
            Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
            while (interfaces.hasMoreElements()) {
                NetworkInterface networkInterface = interfaces.nextElement();
                if (networkInterface.isUp() && !networkInterface.isLoopback() && networkInterface.supportsMulticast()) {
                    eligible.add(networkInterface);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to enumerate network interfaces", e);
        }

        return eligible;
    }

    @Override
    public void close() {
        stopped = true;

        for (CompletableFuture<Void> future : listenerFutures) {
            future.cancel(true);
        }

        shutdownAndAwaitTermination(threadPool, 10, TimeUnit.SECONDS);
    }

    /** Starts address senders to respond to multicast requests. */
    public void start() {
        for (NetworkInterface networkInterface : getEligibleNetworkInterfaces()) {
            listenerFutures.add(runAsync(() -> listenOnInterface(networkInterface), threadPool));
        }
    }
}
