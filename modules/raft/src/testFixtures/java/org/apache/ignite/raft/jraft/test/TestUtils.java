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
package org.apache.ignite.raft.jraft.test;

import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.conf.ConfigurationEntry;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.rpc.RpcClientEx;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.impl.core.DefaultRaftClientService;
import org.junit.jupiter.api.TestInfo;
import org.mockito.ArgumentCaptor;

/**
 * Test helper
 */
public class TestUtils {
    public static ConfigurationEntry getConfEntry(String confStr, String oldConfStr) {
        ConfigurationEntry entry = new ConfigurationEntry();
        entry.setConf(JRaftUtils.getConfiguration(confStr));
        entry.setOldConf(JRaftUtils.getConfiguration(oldConfStr));
        return entry;
    }

    public static void dumpThreads() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = bean.dumpAllThreads(true, true);
        for (ThreadInfo info : infos)
            System.out.println(info);
    }

    public static LogEntry mockEntry(int index, int term) {
        return mockEntry(index, term, 0);
    }

    public static LogEntry mockEntry(int index, int term, int dataSize) {
        LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
        entry.setId(new LogId(index, term));
        if (dataSize > 0) {
            byte[] bs = new byte[dataSize];
            ThreadLocalRandom.current().nextBytes(bs);
            entry.setData(ByteBuffer.wrap(bs));
        }
        return entry;
    }

    public static List<LogEntry> mockEntries() {
        return mockEntries(10);
    }

    /**
     * Returns the localhost IP address.
     *
     * @return localhost IP address
     */
    public static String getLocalAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        }
        catch (UnknownHostException e) {
            throw new IgniteInternalException(e);
        }
    }

    public static List<LogEntry> mockEntries(int n) {
        List<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            LogEntry entry = mockEntry(i, i);
            if (i > 0)
                entry.setData(ByteBuffer.wrap(String.valueOf(i).getBytes(UTF_8)));
            entries.add(entry);
        }
        return entries;
    }

    public static RpcRequests.PingRequest createPingRequest() {
        return new RaftMessagesFactory()
                .pingRequest()
                .sendTimestamp(System.currentTimeMillis())
                .build();
    }

    public static final int INIT_PORT = 5003;

    public static List<TestPeer> generatePeers(TestInfo testInfo, int n) {
        List<TestPeer> ret = new ArrayList<>();
        for (int i = 0; i < n; i++)
            ret.add(new TestPeer(testInfo, INIT_PORT + i));
        return ret;
    }

    public static List<TestPeer> generatePriorityPeers(TestInfo testInfo, int n, List<Integer> priorities) {
        List<TestPeer> ret = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            ret.add(new TestPeer(testInfo, INIT_PORT + i, priorities.get(i)));
        }
        return ret;
    }

    public static byte[] getRandomBytes() {
        byte[] requestContext = new byte[ThreadLocalRandom.current().nextInt(10) + 1];
        ThreadLocalRandom.current().nextBytes(requestContext);
        return requestContext;
    }

    /**
     * @param cluster The cluster.
     * @param expected Expected count.
     * @param timeout The timeout in millis.
     * @return {@code True} if topology size is equal to expected.
     */
    public static boolean waitForTopology(ClusterService cluster, int expected, long timeout) {
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= expected, timeout);
    }

    /**
     * @param cond The condition.
     * @param timeout The timeout.
     * @return {@code True} if condition has happened within the timeout.
     */
    @SuppressWarnings("BusyWait") public static boolean waitForCondition(BooleanSupplier cond, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean())
                return true;

            try {
                sleep(50);
            }
            catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }

    /**
     * @param captor The captor.
     * @param timeout The timeout.
     * @return {@code True} if condition has happened within the timeout.
     */
    public static boolean waitForArgumentCapture(ArgumentCaptor<?> captor, long timeout) {
        return waitForCondition(() -> {
            try {
                return captor.getValue() != null;
            }
            catch (Exception e) {
                return false;
            }
        }, timeout);
    }

    /**
     * Waits until all RAFT threads are stopped, throwing an AssertionException otherwise.
     */
    public static void assertAllJraftThreadsStopped() {
        assertTrue(waitForCondition(() -> Thread.getAllStackTraces().keySet().stream().
                        noneMatch(t -> t.getName().contains("JRaft")), 5_000),
                Thread.getAllStackTraces().keySet().stream().filter(t -> t.getName().contains("JRaft")).
                        sorted(comparing(Thread::getName)).collect(toList()).toString());
    }

    /**
     * Returns a message sender for this node.
     *
     * @param node The node.
     * @return The message sender.
     */
    public static RpcClientEx sender(Node node) {
        NodeImpl node0 = (NodeImpl) node;

        DefaultRaftClientService rpcService = (DefaultRaftClientService) node0.getRpcClientService();

        return (RpcClientEx) rpcService.getRpcClient();
    }
}
