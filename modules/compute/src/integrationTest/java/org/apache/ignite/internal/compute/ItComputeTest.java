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

package org.apache.ignite.internal.compute;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.compute.message.ComputeMessagesSerializationRegistryInitializer;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Integration tests for {@link IgniteComputeImpl}.
 */
public class ItComputeTest {
    private static CountDownLatch latch1;
    private static CountDownLatch latch2;

    private static AtomicInteger counter;

    private List<ClusterService> cluster;

    @AfterEach
    void tearDown() {
        cluster.forEach(ClusterService::stop);

        cluster = null;
    }

    @AfterAll
    static void afterAll() {
        latch1 = null;
        latch2 = null;
        counter = null;
    }

    private static class SimpleCompute implements ComputeTask<Integer, Integer, Integer> {
        @Override
        public Integer execute(Integer argument) {
            return 1 + argument;
        }

        @Override
        public Integer reduce(Collection<Integer> results) {
            return results.stream().reduce(Integer::sum).get();
        }
    }

    private static class DisconnectingCompute implements ComputeTask<Integer, Integer, Integer> {
        @Override
        public Integer execute(Integer argument) {
            latch1.countDown();

            try {
                latch2.await();
            } catch (InterruptedException e) {
                fail();
            }

            return argument + 1;
        }

        @Override
        public Integer reduce(Collection<Integer> results) {
            return results.stream().reduce(Integer::sum).get();
        }
    }

    private static class ExceptionalCompute implements ComputeTask<Integer, Integer, Integer> {
        @Override
        public Integer execute(Integer argument) {
            if (counter.getAndIncrement() == 1) {
                throw new RuntimeException("Fail");
            }

            return argument + 1;
        }

        @Override
        public Integer reduce(Collection<Integer> results) {
            return results.stream().reduce(Integer::sum).get();
        }
    }

    @Test
    public void testSuccess(TestInfo testInfo) throws Exception {
        cluster = createCluster(testInfo, 5);

        IgniteComputeImpl compute = createCompute(cluster);

        Integer result = compute.execute(SimpleCompute.class, 1);

        assertEquals(10, result);
    }

    @Test
    public void testPartialSuccess(TestInfo testInfo) throws Exception {
        latch1 = new CountDownLatch(5);
        latch2 = new CountDownLatch(1);

        cluster = createCluster(testInfo, 5);

        IgniteComputeImpl compute = createCompute(cluster);

        Thread t = new Thread(() -> {
            try {
                latch1.await();
            } catch (InterruptedException e) {
                fail();
            }

            ClusterService clusterService = cluster.get(1);
            clusterService.stop();
            latch2.countDown();
        });

        t.start();

        Integer result = compute.execute(DisconnectingCompute.class, 1);

        assertEquals(8, result);
    }

    @Test
    public void testException(TestInfo testInfo) throws Exception {
        counter = new AtomicInteger();

        cluster = createCluster(testInfo, 5);

        IgniteComputeImpl compute = createCompute(cluster);

        ComputeException computeException = assertThrows(ComputeException.class,
                () -> compute.execute(ExceptionalCompute.class, 1));

        Throwable cause = computeException.getCause();

        assertInstanceOf(RuntimeException.class, cause);

        assertEquals("Fail", cause.getMessage());
    }

    private IgniteComputeImpl createCompute(List<ClusterService> cluster) {
        List<IgniteComputeImpl> computes = cluster.stream().map(IgniteComputeImpl::new).collect(toList());

        computes.forEach(IgniteComputeImpl::start);

        return computes.get(0);
    }

    private List<ClusterService> createCluster(TestInfo testInfo, int count) throws InterruptedException {
        int port = 3344;

        List<ClusterService> services = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            ClusterService service = clusterService(testInfo, port + i);
            service.start();

            services.add(service);
        }

        for (ClusterService service : services) {
            assertTrue(waitForTopology(service, count, 1000));
        }

        return services;
    }

    private ClusterService clusterService(TestInfo testInfo, int port) {
        MessageSerializationRegistryImpl registry = new MessageSerializationRegistryImpl();
        ComputeMessagesSerializationRegistryInitializer.registerFactories(registry);

        return ClusterServiceTestUtils.clusterService(
            testInfo,
            port,
            new StaticNodeFinder(List.of(new NetworkAddress("localhost", 3344))),
            new TestScaleCubeClusterServiceFactory(),
            registry
        );
    }

    private static boolean waitForTopology(ClusterService cluster, int expected, int timeout) throws InterruptedException {
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= expected, timeout);
    }
}
