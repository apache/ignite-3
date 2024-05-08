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

package org.apache.ignite.disruptor;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.raft.jraft.disruptor.NodeIdAware;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.junit.jupiter.api.Test;

/**
 * Tests for striped disruptor.
 */
public class StripedDisruptorTest extends IgniteAbstractTest {
    /**
     * Checks the correctness of disruptor batching in a handler. This test creates only one stripe in order to the real Disruptor is shared
     * between two groups.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDisruptorBatch() throws Exception {
        StripedDisruptor<NodeIdAwareTestObj> disruptor = new StripedDisruptor<>("test", "test-disruptor",
                16384,
                NodeIdAwareTestObj::new,
                1,
                false,
                false,
                null);

        var nodeId1 = new NodeId("grp1", new PeerId("foo"));
        var nodeId2 = new NodeId("grp2", new PeerId("foo"));

        GroupAwareTestObjHandler handler1 = new GroupAwareTestObjHandler();
        GroupAwareTestObjHandler handler2 = new GroupAwareTestObjHandler();

        RingBuffer<NodeIdAwareTestObj> taskQueue1 = disruptor.subscribe(nodeId1, handler1);
        RingBuffer<NodeIdAwareTestObj> taskQueue2 = disruptor.subscribe(nodeId2, handler2);

        assertSame(taskQueue1, taskQueue2);

        for (int i = 0; i < 1_000; i++) {
            int finalInt = i;

            taskQueue1.tryPublishEvent((event, sequence) -> {
                event.nodeId = nodeId1;
                event.num = finalInt;
            });

            taskQueue2.tryPublishEvent((event, sequence) -> {
                event.nodeId = nodeId2;
                event.num = finalInt;
            });

            if (i % 10 == 0) {
                assertTrue(IgniteTestUtils.waitForCondition(() -> handler1.applied == finalInt + 1, 10_000),
                        IgniteStringFormatter.format("Batch was not committed [applied={}, expected={}, buffered={}]",
                                handler1.applied, finalInt + 1, handler1.batch));
                assertTrue(IgniteTestUtils.waitForCondition(() -> handler2.applied == finalInt + 1, 10_000),
                        IgniteStringFormatter.format("Batch was not committed [applied={}, expected={}, buffered={}]",
                                handler2.applied, finalInt + 1, handler2.batch));
            }
        }

        disruptor.shutdown();
    }

    /**
     * The test checks that the Striped Disruptor work same as real one in the circumstances when we have only one consumer group.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testDisruptorSimple() throws Exception {
        StripedDisruptor<NodeIdAwareTestObj> disruptor = new StripedDisruptor<>("test", "test-disruptor",
                16384,
                NodeIdAwareTestObj::new,
                5,
                false,
                false,
                null);

        GroupAwareTestObjHandler handler = new GroupAwareTestObjHandler();

        var nodeId = new NodeId("grp", new PeerId("foo"));

        RingBuffer<NodeIdAwareTestObj> taskQueue = disruptor.subscribe(nodeId, handler);

        for (int i = 0; i < 1_000; i++) {
            int finalInt = i;

            taskQueue.publishEvent((event, sequence) -> {
                event.nodeId = nodeId;
                event.num = finalInt;
            });
        }

        assertTrue(IgniteTestUtils.waitForCondition(() -> handler.applied == 1_000, 10_000));

        disruptor.shutdown();
    }

    /**
     * Checks the distribution of subscribed handlers across stripes.
     * The distribution algorithm has to distribute handlers as evenly as possible using the round-robin algorithm.
     */
    @Test
    public void testDistributionHandlers() {
        Random random = new Random();

        int stripes = random.nextInt(20) + 1;

        StripedDisruptor<NodeIdAwareTestObj> disruptor = new StripedDisruptor<>("test", "test-disruptor",
                16384,
                NodeIdAwareTestObj::new,
                stripes,
                false,
                false,
                null);

        int handlers = random.nextInt(100) + 1;

        log.info("Handlers will be distributed across stripes [handlers={}, stripes={}]", handlers, stripes);

        int[] distribution = new int[stripes];

        for (int i = 0; i < handlers; i++) {
            GroupAwareTestObjHandler handler = new GroupAwareTestObjHandler();

            var nodeId = new NodeId("grp", new PeerId(UUID.randomUUID().toString()));

            disruptor.subscribe(nodeId, handler);

            int stripe = disruptor.getStripe(nodeId);

            assertNotEquals(-1, stripe);

            distribution[stripe]++;
        }

        log.info("Result distribution [distribution={}]", distribution);

        int reference = distribution[0];

        for (int i = 1; i < stripes; i++) {
            assertTrue(distribution[i] == reference || distribution[i] + 1 == reference || distribution[i] - 1 == reference);
        }
    }

    /** Group event handler. */
    private static class GroupAwareTestObjHandler implements EventHandler<NodeIdAwareTestObj> {
        /** This is a container for the batch events. */
        ArrayList<Integer> batch = new ArrayList<>();

        /** Counter of applied events. */
        int applied = 0;

        /** {@inheritDoc} */
        @Override
        public void onEvent(NodeIdAwareTestObj event, long sequence, boolean endOfBatch) {
            batch.add(event.num);

            if (endOfBatch) {
                applied += batch.size();

                batch.clear();
            }
        }
    }

    /**
     * Group aware object implementation to test the striped disruptor.
     */
    private static class NodeIdAwareTestObj implements NodeIdAware {
        /** Node id. */
        NodeId nodeId;

        /** Any integer number. */
        int num;

        @Override
        public NodeId nodeId() {
            return nodeId;
        }
    }
}
