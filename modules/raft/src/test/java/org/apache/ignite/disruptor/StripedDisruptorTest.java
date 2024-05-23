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

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.raft.jraft.disruptor.DisruptorEventType;
import org.apache.ignite.raft.jraft.disruptor.NodeIdAware;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
                event.handler = null;
                event.evtType = DisruptorEventType.REGULAR;
                event.num = finalInt;
            });

            taskQueue2.tryPublishEvent((event, sequence) -> {
                event.nodeId = nodeId2;
                event.handler = null;
                event.evtType = DisruptorEventType.REGULAR;
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
                event.handler = null;
                event.evtType = DisruptorEventType.REGULAR;
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

    @Test
    public void testOneSubscriberBatching() throws Exception {
        Random random = new Random();

        int stripes = random.nextInt(20) + 1;

        StripedDisruptor<NodeIdAwareTestObj> disruptor = new StripedDisruptor<>("test", "test-disruptor",
                16384,
                NodeIdAwareTestObj::new,
                stripes,
                false,
                false,
                null);

        HashMap<NodeId, RingBuffer<NodeIdAwareTestObj>> queues = new HashMap<>();
        GroupAwareTestObjHandler[] handlers = new GroupAwareTestObjHandler[stripes];
        NodeId[] nodesIds = new NodeId[stripes];

        for (int i = 0; i < stripes; i++) {
            GroupAwareTestObjHandler handler = new GroupAwareTestObjHandler();

            var nodeId = new NodeId("grp", new PeerId(String.valueOf(i)));

            queues.put(nodeId, disruptor.subscribe(nodeId, handler));
            handlers[i] = handler;
            nodesIds[i] = nodeId;
        }

        int batchSize = random.nextInt(50) + 1;

        for (NodeId nodeId : nodesIds) {
            EventTranslator<NodeIdAwareTestObj>[] eventTranslators = new EventTranslator[batchSize];

            for (int i = 0; i < batchSize; i++) {
                int finalI = i;

                eventTranslators[i] = (event, sequence) -> {
                    event.nodeId = nodeId;
                    event.handler = null;
                    event.evtType = DisruptorEventType.REGULAR;
                    event.num = finalI;
                };
            }

            queues.get(nodeId).publishEvents(eventTranslators);
        }

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            for (GroupAwareTestObjHandler handler : handlers) {
                if (handler.applied != batchSize || handler.batchesApplied != 1) {
                    return false;
                }
            }

            return true;
        }, 10_000));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMultipleSubscriberBatching(boolean supportBatching) throws Exception {
        Random random = new Random();

        int totalHandlers = random.nextInt(20) + 1;

        StripedDisruptor<NodeIdAwareTestObj> disruptor = new StripedDisruptor<>("test", "test-disruptor",
                16384,
                NodeIdAwareTestObj::new,
                1,
                supportBatching,
                false,
                null);

        RingBuffer<NodeIdAwareTestObj> queue = null;
        GroupAwareTestObjHandler[] handlers = new GroupAwareTestObjHandler[totalHandlers];
        NodeId[] nodesIds = new NodeId[totalHandlers];

        for (int i = 0; i < totalHandlers; i++) {
            GroupAwareTestObjHandler handler = new GroupAwareTestObjHandler();

            var nodeId = new NodeId("grp", new PeerId(String.valueOf(i)));

            // Any queue can use here, because the striped disruptor has the only stripe.
            queue = disruptor.subscribe(nodeId, handler);

            handlers[i] = handler;
            nodesIds[i] = nodeId;

            assertEquals(0, disruptor.getStripe(nodeId));
        }

        int batchSize = random.nextInt(50) + 1;

        EventTranslator<NodeIdAwareTestObj>[] eventTranslators = new EventTranslator[totalHandlers * batchSize];

        for (int i = 0; i < totalHandlers * batchSize; i++) {
            int finalI = i;

            eventTranslators[i] = (event, sequence) -> {
                event.nodeId = nodesIds[finalI % totalHandlers];
                event.handler = null;
                event.evtType = DisruptorEventType.REGULAR;
                event.num = finalI;
            };
        }

        queue.publishEvents(eventTranslators);

        if (supportBatching) {
            assertTrue(IgniteTestUtils.waitForCondition(() -> {
                int batchCommited = 0;

                for (GroupAwareTestObjHandler handler : handlers) {
                    if (handler.applied == batchSize && handler.batchesApplied == 1) {
                        batchCommited++;
                    }
                }

                return batchCommited == 1;
            }, 10_000));
        } else {
            assertTrue(IgniteTestUtils.waitForCondition(() -> {
                for (GroupAwareTestObjHandler handler : handlers) {
                    if (handler.applied != batchSize || handler.batchesApplied != 1) {
                        return false;
                    }
                }

                return true;
            }, 10_000));
        }
    }

    @Test
    public void testConcurrentSubscribe() throws Exception {
        Random random = new Random();

        int totalHandlers = random.nextInt(20) + 1;

        StripedDisruptor<NodeIdAwareTestObj> disruptor = new StripedDisruptor<>("test", "test-disruptor",
                16384,
                NodeIdAwareTestObj::new,
                1,
                false,
                false,
                null);

        RingBuffer<NodeIdAwareTestObj> queue = null;
        GroupAwareTestObjHandler[] handlers = new GroupAwareTestObjHandler[totalHandlers];
        NodeId[] nodesIds = new NodeId[totalHandlers];

        for (int i = 0; i < totalHandlers; i++) {
            GroupAwareTestObjHandler handler = new GroupAwareTestObjHandler();

            var nodeId = new NodeId("grp", new PeerId(String.valueOf(i)));

            // Any queue can use here, because the striped disruptor has the only stripe.
            queue = disruptor.subscribe(nodeId, handler);

            handlers[i] = handler;
            nodesIds[i] = nodeId;

            assertEquals(0, disruptor.getStripe(nodeId));
        }

        AtomicBoolean stop = new AtomicBoolean();

        int unstableSubscriberNumber = random.nextInt(totalHandlers);
        NodeId unstableSubscriber = nodesIds[unstableSubscriberNumber];
        GroupAwareTestObjHandler unstableSubscriberHandler = handlers[unstableSubscriberNumber];

        CompletableFuture<Void> stopTreadCompleted = IgniteTestUtils.runAsync(() -> {
            while (!stop.get()) {
                if (disruptor.getStripe(unstableSubscriber) == -1) {
                    disruptor.subscribe(unstableSubscriber, unstableSubscriberHandler);
                } else {
                    disruptor.unsubscribe(unstableSubscriber);
                }
            }
        });

        Map<NodeId, Integer> appliedMap = new HashMap<>(totalHandlers);

        for (int iter = 0; iter < 100_000; iter++) {
            int batchSize = random.nextInt(10) + 1;

            List<EventTranslator<NodeIdAwareTestObj>> eventTranslators = random.ints(
                    batchSize,
                    0,
                    totalHandlers
            ).mapToObj(value -> {
                appliedMap.compute(nodesIds[value], (nodeId, count) -> {
                    if (count == null) {
                        return 1;
                    }
                    return count + 1;
                });

                return (EventTranslator<NodeIdAwareTestObj>) (event, sequence) -> {
                    event.nodeId = nodesIds[value];
                    event.handler = null;
                    event.evtType = DisruptorEventType.REGULAR;
                    event.num = value;
                };
            }).collect(Collectors.toList());

            queue.publishEvents(eventTranslators.toArray(new EventTranslator[0]));
        }

        stop.set(true);

        assertThat(stopTreadCompleted, willCompleteSuccessfully());

        assertTrue(IgniteTestUtils.waitForCondition(() -> {
            for (int i = 0; i < totalHandlers; i++) {
                if (i == unstableSubscriberNumber) {
                    continue;
                }

                if (handlers[i].applied != appliedMap.getOrDefault(nodesIds[i], 0)) {
                    return false;
                }
            }

            return true;
        }, 10_000));
    }

    /** Group event handler. */
    private static class GroupAwareTestObjHandler implements EventHandler<NodeIdAwareTestObj> {
        /** This is a container for the batch events. */
        ArrayList<Integer> batch = new ArrayList<>();

        /** Counter of applied events. */
        int applied = 0;

        /** Amount of applied batches. */
        int batchesApplied = 0;

        /** {@inheritDoc} */
        @Override
        public void onEvent(NodeIdAwareTestObj event, long sequence, boolean endOfBatch) {
            batch.add(event.num);

            if (endOfBatch) {
                applied += batch.size();

                batchesApplied++;

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
        EventHandler<NodeIdAware> handler;
        DisruptorEventType evtType;

        /** Any integer number. */
        int num;

        @Override
        public NodeId nodeId() {
            return nodeId;
        }

        @Override
        public void nodeId(NodeId nodeId) {
            this.nodeId = nodeId;

        }

        @Override
        public void handler(EventHandler<NodeIdAware> handler) {
            this.handler = handler;
        }

        @Override
        public EventHandler<NodeIdAware> handler() {
            return handler;
        }

        @Override
        public void type(DisruptorEventType type) {
            this.evtType = type;
        }

        @Override
        public DisruptorEventType type() {
            return evtType;
        }
    }
}
