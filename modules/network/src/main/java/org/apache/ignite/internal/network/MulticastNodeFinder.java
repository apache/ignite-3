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

import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.network.serialization.NetworkAddressesSerializer;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Multicast-based node finder.
 *
 * <p>Sends multicast requests to {@link #multicastSocketAddress} and waits for responses from other nodes,
 * which reply with their network addresses. </p>
 *
 * <p>Runs a background listener on all eligible network interfaces (up, non-loopback, multicast-capable)
 * and an unbound socket.</p>
 *
 * <p>Listener uses a polling mechanism to cover multiple sockets using one thread.</p>
 *
 * <p>If error occurs on specific interfaces, node finder and listener log them, but continue operation using other ones.</p>
 */
public class MulticastNodeFinder implements NodeFinder {
    private static final IgniteLogger LOG = Loggers.forClass(MulticastNodeFinder.class);

    /** Discovery request message. */
    private static final byte[] REQUEST_MESSAGE = "IGNT".getBytes(UTF_8);

    /** Buffer size for receiving responses. */
    private static final int RECEIVE_BUFFER_SIZE = 1024;

    /** System default value will be used if {@code -1} is specified. */
    public static final int UNSPECIFIED_TTL = -1;
    public static final int MAX_TTL = 255;

    private static final int REQ_ATTEMPTS = 2;
    private static final int POLLING_TIMEOUT_MILLIS = 100;

    /** Multicast address used for sending and listening to requests. Must be a valid multicast address. */
    private final InetSocketAddress multicastSocketAddress;

    private final int multicastPort;

    /** Time to wait for multicast responses in milliseconds. */
    private final int resultWaitMillis;

    /** Amount of network "hops" allowed for the multicast request. Range is from {@link #UNSPECIFIED_TTL} to {@link #MAX_TTL}. */
    private final int ttl;

    /** Addresses sent in response to multicast requests. */
    private final Set<NetworkAddress> addressesToAdvertise;
    private final ExecutorService listenerThreadPool;
    private final String nodeName;

    /** Flag to control running state of node finder listener. */
    private volatile boolean stopped = false;

    /**
     * Constructs a new multicast node finder.
     *
     * @param multicastGroup Multicast group.
     * @param multicastPort Multicast port.
     * @param resultWaitMillis Wait time for responses.
     * @param ttl Time-to-live for multicast packets.
     * @param nodeName Node name.
     * @param addressesToAdvertise Local node addresses.
     */
    public MulticastNodeFinder(
            String multicastGroup,
            int multicastPort,
            int resultWaitMillis,
            int ttl,
            String nodeName,
            Set<NetworkAddress> addressesToAdvertise
    ) {
        this.multicastSocketAddress = new InetSocketAddress(multicastGroup, multicastPort);
        this.multicastPort = multicastPort;
        this.resultWaitMillis = resultWaitMillis;
        this.ttl = ttl;
        this.addressesToAdvertise = addressesToAdvertise;
        this.nodeName = nodeName;

        this.listenerThreadPool = Executors.newSingleThreadExecutor(IgniteThreadFactory.create(nodeName, "multicast-listener", LOG));
    }

    @Override
    public Collection<NetworkAddress> findNodes() {
        Set<NetworkAddress> result = new HashSet<>();

        // Creates multicast sockets for all eligible interfaces and an unbound socket. Will throw an exception if no sockets were created.
        List<MulticastSocket> sockets = createSockets(0, resultWaitMillis, false);

        ExecutorService executor = Executors.newFixedThreadPool(
                sockets.size(),
                IgniteThreadFactory.create(nodeName, "multicast-node-finder", LOG)
        );

        try {
            // Will contain nodes, found on all eligible interfaces and an unbound socket.
            List<CompletableFuture<Collection<NetworkAddress>>> futures = sockets.stream()
                    .map(socket -> supplyAsync(() -> findOnSocket(socket), executor))
                    .collect(toList());

            // Collect results.
            for (CompletableFuture<Collection<NetworkAddress>> future : futures) {
                try {
                    result.addAll(future.get(resultWaitMillis * REQ_ATTEMPTS * 2L, TimeUnit.MILLISECONDS));
                } catch (Exception e) {
                    LOG.error("Error while waiting for multicast node finder result", e);
                }
            }
        } finally {
            closeSockets(sockets);

            shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }

        LOG.info("Found addresses: {}", result);

        return result;
    }

    /** Finds nodes on the given socket. Errors are logged, but not thrown so they don't prevent discovery on other sockets. */
    private Collection<NetworkAddress> findOnSocket(MulticastSocket socket) {
        Set<NetworkAddress> discovered = new HashSet<>();
        byte[] responseBuffer = new byte[RECEIVE_BUFFER_SIZE];

        try {
            // To avoid hanging.
            assert socket.getSoTimeout() == resultWaitMillis;

            for (int i = 0; i < REQ_ATTEMPTS; i++) {
                // Send multicast request.
                DatagramPacket requestPacket = new DatagramPacket(REQUEST_MESSAGE, REQUEST_MESSAGE.length, multicastSocketAddress);
                socket.send(requestPacket);

                // Receive responses from the nodes with their local addresses.
                waitForResponses(responseBuffer, socket, discovered);
            }
        } catch (Exception e) {
            LOG.error("Error during multicast node finding on interface: ", e);
        }

        return discovered;
    }

    /** Waits for responses to the multicast request from other nodes. */
    private void waitForResponses(byte[] responseBuffer, MulticastSocket socket, Set<NetworkAddress> discovered) throws IOException {
        long endTime = currentTimeMillis() + resultWaitMillis;
        // Loop until the timeout expires.
        while (currentTimeMillis() < endTime) {
            DatagramPacket responsePacket = new DatagramPacket(responseBuffer, responseBuffer.length);

            try {
                byte[] data = receiveDatagramData(socket, responsePacket);

                Set<NetworkAddress> addresses = NetworkAddressesSerializer.deserialize(data);

                if (!addressesToAdvertise.contains(addresses.iterator().next())) {
                    discovered.addAll(addresses);
                }
            } catch (SocketTimeoutException ignored) {
                // No-op.
            }
        }
    }

    /**
     * Returns a collection of eligible network interfaces that are up, non‑loopback, and support multicast or empty if got an exception
     * while retrieving interfaces.
     *
     * @return Collection of eligible network interfaces.
     */
    private static Collection<NetworkInterface> getEligibleNetworkInterfaces() {
        try {
            return NetworkInterface.networkInterfaces()
                    .filter(itf -> {
                        try {
                            return itf.isUp() && !itf.isLoopback() && itf.supportsMulticast();
                        } catch (SocketException e) {
                            LOG.error("Error checking network interface {}", e, itf.getName());

                            return false;
                        }
                    }).collect(toSet());
        } catch (SocketException e) {
            LOG.error("Error getting network interfaces for multicast node finder", e);

            return Set.of();
        }
    }

    @Override
    public void close() {
        stopped = true;

        shutdownAndAwaitTermination(listenerThreadPool, 10, TimeUnit.SECONDS);
    }

    @Override
    public void start() {
        // Create multicast sockets for all eligible interfaces and an unbound socket. Will throw an exception if no sockets were created.
        List<MulticastSocket> sockets = createSockets(multicastPort, POLLING_TIMEOUT_MILLIS, true);

        if (sockets.isEmpty()) {
            throw new IgniteInternalException(INTERNAL_ERR, "No sockets for multicast listener were created.");
        }

        listenerThreadPool.submit(() -> {
            byte[] responseData = NetworkAddressesSerializer.serialize(addressesToAdvertise);
            byte[] requestBuffer = new byte[REQUEST_MESSAGE.length];

            // Listener uses a polling mechanism to cover all sockets using one thread.
            while (!stopped) {
                for (MulticastSocket socket : sockets) {
                    if (socket.isClosed()) {
                        continue;
                    }

                    try {
                        // Tries to receive a multicast request on the socket.
                        DatagramPacket requestPacket = new DatagramPacket(requestBuffer, requestBuffer.length);
                        byte[] received = receiveDatagramData(socket, requestPacket);

                        if (!Arrays.equals(received, REQUEST_MESSAGE)) {
                            LOG.error("Received unexpected request on multicast socket");
                            continue;
                        }

                        // Send response with local address.
                        DatagramPacket responsePacket = new DatagramPacket(
                                responseData,
                                responseData.length,
                                requestPacket.getAddress(),
                                requestPacket.getPort()
                        );

                        socket.send(responsePacket);
                    } catch (SocketTimeoutException ignored) {
                        // Timeout to check another socket.
                    } catch (Exception e) {
                        if (!stopped) {
                            LOG.error("Error in multicast listener, stopping socket on {}", e);
                        }

                        socket.close();
                    }
                }
            }

            closeSockets(sockets);
        });
    }

    /**
     * Creates multicast sockets for all eligible interfaces and an unbound socket. Errors on specific interface are logged, but not thrown.
     * Only unbound interface is used if there was error retrieving all interfaces.
     *
     * @throws IgniteInternalException If no multicast sockets were created.
     */
    private List<MulticastSocket> createSockets(int port, int soTimeout, boolean joinGroup) {
        List<MulticastSocket> sockets = new ArrayList<>();

        for (NetworkInterface networkInterface : getEligibleNetworkInterfaces()) {
            addSocket(sockets, port, networkInterface, soTimeout, joinGroup);

            if (joinGroup) {
                LOG.info("Multicast listener socket created for interface: {}", networkInterface.getDisplayName());
            } else {
                LOG.info("Multicast node finder socket created for interface: {}", networkInterface.getDisplayName());
            }
        }

        addSocket(sockets, multicastPort, null, soTimeout, joinGroup);

        if (sockets.isEmpty()) {
            throw new IgniteInternalException(INTERNAL_ERR, "No multicast sockets were created.");
        }

        return sockets;
    }

    /** Create socket, configure it with given parameters and add it to the given collection. */
    private void addSocket(
            Collection<MulticastSocket> sockets,
            int port,
            @Nullable NetworkInterface networkInterface,
            int soTimeout,
            boolean joinGroup
    ) {
        try {
            var socket = new MulticastSocket(port);

            // Using setLoopbackMode() (which is deprecated in Java versions starting with 14) because it still works in all Java versions,
            // while the replacement suggested by the deprecation message -
            // socket.setOption(StandardSocketOptions.IP_MULTICAST_LOOP, true/false) - is not portable across Java versions
            // (before Java 14, we need to pass 'false', while since Java 14, it's 'true').

            // Use 'false' to enable support for more than one node on the same machine.
            socket.setLoopbackMode(false);

            if (networkInterface != null) {
                socket.setNetworkInterface(networkInterface);
            }

            socket.setSoTimeout(soTimeout);

            if (ttl != UNSPECIFIED_TTL) {
                socket.setTimeToLive(ttl);
            }

            if (joinGroup) {
                socket.joinGroup(multicastSocketAddress, networkInterface);
            }

            sockets.add(socket);
        } catch (IOException e) {
            if (networkInterface != null) {
                LOG.error("Failed to create multicast socket for interface {}", e, networkInterface.getName());
            } else {
                LOG.error("Failed to create multicast socket for an unbound interface", e);
            }
        }
    }

    /** Waits for a datagram packet on the given socket and returns its data. */
    private static byte[] receiveDatagramData(MulticastSocket socket, DatagramPacket responsePacket) throws IOException {
        socket.receive(responsePacket);
        return Arrays.copyOfRange(
                responsePacket.getData(),
                responsePacket.getOffset(),
                responsePacket.getOffset() + responsePacket.getLength()
        );
    }

    private static void closeSockets(List<MulticastSocket> sockets) {
        try {
            IgniteUtils.closeAll(sockets);
        } catch (Exception e) {
            LOG.error("Could not close multicast listeners", e);
        }
    }
}
