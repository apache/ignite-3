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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.StaticNodeFinder.HostNameResolver;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

class StaticNodeFinderTest extends IgniteAbstractTest {
    @Test
    void returnsIpAddresses() {
        NetworkAddress ipv4 = new NetworkAddress("1.2.3.4", 3001);
        NetworkAddress ipv6 = new NetworkAddress("2001:1234:130f:1234:1234:99c0:876a:130b", 3002);
        NodeFinder finder = new StaticNodeFinder(List.of(ipv4, ipv6));

        assertThat(finder.findNodes(), contains(ipv4, ipv6));
    }

    @Test
    void removesDuplicateIpAddresses() {
        NetworkAddress ip1 = new NetworkAddress("1.2.3.4", 3001);
        NetworkAddress ip2 = new NetworkAddress("1.2.3.4", 3001);
        NodeFinder finder = new StaticNodeFinder(List.of(ip1,  ip2));

        assertThat(finder.findNodes(), contains(ip1));
    }

    @Test
    void returnsEmptyResultForEmptyInput() {
        NodeFinder finder = new StaticNodeFinder(List.of());

        assertEquals(0, finder.findNodes().size());
    }

    @Test
    void failsForNoResolvedIpAddresses() {
        NetworkAddress ip1 = new NetworkAddress("badIpString", 3001);
        NodeFinder finder = new StaticNodeFinder(List.of(ip1));

        assertThrows(IgniteInternalException.class, finder::findNodes);
    }

    @Test
    void succeedsForAtLeastOneResolvedIpAddresses() {
        NetworkAddress ip1 = new NetworkAddress("badIpString", 3001);
        NetworkAddress ip2 = new NetworkAddress("1.2.3.4", 3001);
        NodeFinder finder = new StaticNodeFinder(List.of(ip1, ip2));

        assertThat(finder.findNodes(), contains(ip2));
    }

    @Test
    void resolvesLocalHostToJustOneAddress() throws Exception {
        Path hostsFilePath = writeHostsFile(Map.of(
                "127.0.0.1", "localhost",
                "172.24.0.2", "localhost"
        ));

        List<NetworkAddress> foundAddresses = findAddressesWithOverriddenNameResolving(
                List.of(new NetworkAddress("localhost", 3001)),
                hostsFilePath
        );

        assertThat(foundAddresses, contains(new NetworkAddress("127.0.0.1", 3001)));
    }

    private Path writeHostsFile(Map<String, String> ipToHostname) throws IOException {
        Path hostsFilePath = workDir.resolve("hosts");
        Files.writeString(hostsFilePath, hostsFileContent(ipToHostname));
        return hostsFilePath;
    }

    @Test
    void resolvesNames() throws Exception {
        Path hostsFilePath = writeHostsFile(Map.of(
                "1.2.3.4", "abc.def",
                "1.2.3.5", "abc.def",
                "4.3.2.1", "def.abc"
        ));

        List<NetworkAddress> foundAddresses = findAddressesWithOverriddenNameResolving(
                List.of(new NetworkAddress("abc.def", 3001), new NetworkAddress("def.abc", 3002)),
                hostsFilePath
        );

        assertThat(
                foundAddresses,
                containsInAnyOrder(
                        new NetworkAddress("1.2.3.4", 3001),
                        new NetworkAddress("1.2.3.5", 3001),
                        new NetworkAddress("4.3.2.1", 3002)
                )
        );
    }

    @Test
    void ignoresUnresolvableName() throws Exception {
        Path hostsFilePath = writeHostsFile(Map.of("1.2.3.4", "abc.def"));

        List<NetworkAddress> foundAddresses = findAddressesWithOverriddenNameResolving(
                List.of(new NetworkAddress("abc.def", 3001), new NetworkAddress("def.abc", 3002)),
                hostsFilePath
        );

        assertThat(foundAddresses, contains(new NetworkAddress("1.2.3.4", 3001)));
    }

    @Test
    void makesSeveralAttemptsToResolveHost() throws Exception {
        NetworkAddress addr = new NetworkAddress("unknownHost", 3001);

        HostNameResolver hostNameResolver = mock(HostNameResolver.class);

        when(hostNameResolver.getAllByName(anyString())).thenThrow(new UnknownHostException());

        int nameResolutionAttempts = 3;

        NodeFinder finder = new StaticNodeFinder(List.of(addr), hostNameResolver, nameResolutionAttempts);

        assertThrows(IgniteInternalException.class, finder::findNodes);

        verify(hostNameResolver, times(nameResolutionAttempts)).getAllByName(addr.host());
    }

    /**
     * Invokes {@link StaticNodeFinder} with name resolving overridden with our name->ips mapping.
     * Does this by starting {@link StaticNodeFinderMain} in another JVM to allow jdk.net.hosts.file property
     * override name resolving (this defines path to a file in /etc/hosts format).
     *
     * @param addresses Addresses to initialize a {@link StaticNodeFinder} with.
     * @param hostsFilePath Path to hosts file (this file defines the overrides).
     * @return Found addresses.
     * @throws IOException If something goes wrong.
     * @throws InterruptedException If interrupted while waiting for another JVM to terminate.
     */
    private static List<NetworkAddress> findAddressesWithOverriddenNameResolving(
            List<NetworkAddress> addresses,
            Path hostsFilePath
    ) throws IOException, InterruptedException {
        String javaBinaryPath = ProcessHandle.current().info().command().orElseThrow();
        String javaClassPath = System.getProperty("java.class.path");

        //noinspection UseOfProcessBuilder
        ProcessBuilder processBuilder = new ProcessBuilder(
                javaBinaryPath,
                "-cp", javaClassPath,
                "-Djdk.net.hosts.file=" + hostsFilePath,
                StaticNodeFinderMain.class.getName(),
                addresses.stream().map(NetworkAddress::toString).collect(joining(","))
        );
        Process process = processBuilder.start();

        if (!process.waitFor(10, SECONDS)) {
            throw new RuntimeException("Process did not finish in 10 seconds");
        }
        if (process.exitValue() != 0) {
            throw new RuntimeException("Return code " + process.exitValue()
                    + ", stdout: " + stdoutString(process) + ", stderr: " + stderrString(process));
        }

        String output = stdoutString(process);
        return Arrays.stream(output.split(","))
                .map(NetworkAddress::from)
                .collect(toList());
    }

    private static String hostsFileContent(Map<String, String> ipToHostname) {
        return ipToHostname.entrySet().stream()
                .map(entry -> entry.getKey() + " " + entry.getValue())
                .collect(joining("\n"));
    }

    private static String stdoutString(Process process) throws IOException {
        try (InputStream stdout = process.getInputStream()) {
            return new String(stdout.readAllBytes(), UTF_8);
        }
    }

    private static String stderrString(Process process) throws IOException {
        try (InputStream stderr = process.getErrorStream()) {
            return new String(stderr.readAllBytes(), UTF_8);
        }
    }
}
