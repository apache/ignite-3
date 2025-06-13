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
import java.util.Enumeration;
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
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Multicast-based IP finder.
 *
 * <p>It sends multicast request and waits for configured time when other nodes reply to this request with messages containing their
 * addresses.
 */
public class MulticastNodeFinder implements NodeFinder {
    private static final IgniteLogger LOG = Loggers.forClass(MulticastNodeFinder.class);

    /** Discovery request message. */
    private static final byte[] REQUEST_MESSAGE = "IGNT".getBytes(UTF_8);

    /** Buffer size for receiving responses. */
    private static final int RECEIVE_BUFFER_SIZE = 1024;

    /** System default value will be used. */
    public static final int UNSPECIFIED_TTL = -1;
    public static final int MAX_TTL = 255;

    private static final int REQ_ATTEMPTS = 2;
    private static final int POLLING_TIMEOUT_MILLIS = 100;

    private final InetSocketAddress multicastSocketAddress;
    private final int multicastPort;
    private final int resultWaitMillis;
    private final int ttl;

    private final InetSocketAddress localAddressToAdvertise;
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
     * @param localAddressToAdvertise Local node address.
     */
    public MulticastNodeFinder(
            String multicastGroup,
            int multicastPort,
            int resultWaitMillis,
            int ttl,
            String nodeName,
            InetSocketAddress localAddressToAdvertise
    ) {
        this.multicastSocketAddress = new InetSocketAddress(multicastGroup, multicastPort);
        this.multicastPort = multicastPort;
        this.resultWaitMillis = resultWaitMillis;
        this.ttl = ttl;
        this.localAddressToAdvertise = localAddressToAdvertise;
        this.nodeName = nodeName;
        this.listenerThreadPool = Executors.newSingleThreadExecutor(NamedThreadFactory.create(nodeName, "multicast-node-listener", LOG));
    }

    @Override
    public Collection<NetworkAddress> findNodes() {
        Collection<NetworkInterface> interfaces = getEligibleNetworkInterfaces();
        if (interfaces.isEmpty()) {
            throw new IgniteInternalException(INTERNAL_ERR, "No network interfaces eligible for a multicast found");
        }

        Set<NetworkAddress> result = new HashSet<>();
        List<CompletableFuture<Collection<NetworkAddress>>> findOnInterfaceFutures = new ArrayList<>();

        ExecutorService executor = Executors.newFixedThreadPool(
                interfaces.size(),
                NamedThreadFactory.create(nodeName, "multicast-node-finder", LOG)
        );

        try {
            for (NetworkInterface networkInterface : interfaces) {
                findOnInterfaceFutures.add(supplyAsync(() -> findOnInterface(networkInterface), executor));
            }

            for (CompletableFuture<Collection<NetworkAddress>> future : findOnInterfaceFutures) {
                result.addAll(future.get(resultWaitMillis * REQ_ATTEMPTS * 2L, TimeUnit.MILLISECONDS));
            }
        } catch (Exception e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Error during multicast node finding", e);
        } finally {
            shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
        }

        if (result.isEmpty()) {
            LOG.warn("No nodes discovered on interfaces, using unbound multicast socket");
            result.addAll(findOnInterface(null));
        }

        LOG.info("Found nodes: {}", result);

        return result;
    }

    private Collection<NetworkAddress> findOnInterface(@Nullable NetworkInterface networkInterface) {
        Set<NetworkAddress> discovered = new HashSet<>();
        byte[] responseBuffer = new byte[RECEIVE_BUFFER_SIZE];

        try (MulticastSocket socket = new MulticastSocket(0)) {
            configureSocket(socket, networkInterface, resultWaitMillis);

            for (int i = 0; i < REQ_ATTEMPTS; i++) {
                DatagramPacket requestPacket = new DatagramPacket(REQUEST_MESSAGE, REQUEST_MESSAGE.length, multicastSocketAddress);
                socket.send(requestPacket);

                waitForResponses(responseBuffer, socket, discovered);
            }
        } catch (Exception e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Error during multicast node finding on interface: " + networkInterface, e);
        }

        return discovered;
    }

    private void waitForResponses(byte[] responseBuffer, MulticastSocket socket, Set<NetworkAddress> discovered) throws IOException {
        long endTime = currentTimeMillis() + resultWaitMillis;
        // Loop until the timeout expires.
        while (currentTimeMillis() < endTime) {
            DatagramPacket responsePacket = new DatagramPacket(responseBuffer, responseBuffer.length);

            try {
                socket.receive(responsePacket);
                byte[] data = Arrays.copyOfRange(
                        responsePacket.getData(),
                        responsePacket.getOffset(),
                        responsePacket.getOffset() + responsePacket.getLength()
                );

                InetSocketAddress address = ByteUtils.fromBytes(data);
                if (!address.equals(localAddressToAdvertise)) {
                    discovered.add(NetworkAddress.from(address));
                }
            } catch (SocketTimeoutException ignored) {
                // No-op.
            }
        }
    }

    private void configureSocket(MulticastSocket socket, @Nullable NetworkInterface networkInterface, int soTimeout) throws IOException {
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
        } catch (SocketException e) {
            throw new IgniteInternalException(INTERNAL_ERR, "Error getting network interfaces", e);
        }

        return eligible;
    }

    @Override
    public void close() {
        stopped = true;

        shutdownAndAwaitTermination(listenerThreadPool, 10, TimeUnit.SECONDS);
    }

    @Override
    public void start() {
        listenerThreadPool.submit(() -> {
            List<MulticastSocket> sockets = new ArrayList<>();

            try {
                for (NetworkInterface networkInterface : getEligibleNetworkInterfaces()) {
                    MulticastSocket socket = new MulticastSocket(multicastPort);
                    configureSocket(socket, networkInterface, POLLING_TIMEOUT_MILLIS);
                    socket.joinGroup(multicastSocketAddress, networkInterface);

                    sockets.add(socket);
                }

                if (sockets.isEmpty()) {
                    LOG.warn("No interfaces eligible for multicast found; listener not started.");
                    return;
                }

                byte[] responseData = ByteUtils.toBytes(localAddressToAdvertise);
                byte[] requestBuffer = new byte[REQUEST_MESSAGE.length];
                while (!stopped) {
                    for (MulticastSocket socket : sockets) {
                        DatagramPacket requestPacket = new DatagramPacket(requestBuffer, requestBuffer.length);
                        try {
                            socket.receive(requestPacket);

                            byte[] received = Arrays.copyOfRange(
                                    requestPacket.getData(),
                                    requestPacket.getOffset(),
                                    requestPacket.getOffset() + requestPacket.getLength()
                            );

                            if (!Arrays.equals(received, REQUEST_MESSAGE)) {
                                LOG.error("Received unexpected request on multicast socket");
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
                            // Timeout to check another socket.
                        }
                    }
                }
            } catch (Exception e) {
                if (!stopped) {
                    throw new IgniteInternalException(INTERNAL_ERR, "Error in multicast listener", e);
                }
            } finally {
                try {
                    IgniteUtils.closeAll(sockets);
                } catch (Exception e) {
                    LOG.error("Could not close multicast sockets", e);
                }
            }
        });
    }
}
