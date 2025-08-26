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

package org.apache.ignite.raft.jraft.disruptor;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.raft.jraft.disruptor.DisruptorEventType.SUBSCRIBE;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.PhasedBackoffWaitStrategy;import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource.DisruptorMetrics;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.jetbrains.annotations.Nullable;

/**
 * Stripe Disruptor is a set of queues which process several independent groups in one queue (in the stripe). It makes fewer threads that
 * the groups and gives the same sequential guaranties and a close performance.
 *
 * @param <T> Event type. This event should implement {@link NodeIdAware} interface.
 */
public class StripedDisruptor<T extends INodeIdAware> {
    /**
     * It is an id that does not represent any node to batch events in one stripe although {@link NodeId} may vary. This is a cached event
     * in case the disruptor supports batching, because the {@link DisruptorEventType#SUBSCRIBE} event might be a finale one and have to be
     * handled.
     */
    private final NodeId FAKE_NODE_ID = new NodeId(null, null);

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(StripedDisruptor.class);

    /** The map stores a matching node id for the stripe that sends messages to the node. */
    protected ConcurrentHashMap<NodeId, Integer> stripeMapper = new ConcurrentHashMap<>();

    /** The counter is used to generate the next stripe to subscribe to in order to be a round-robin hash. */
    private final AtomicInteger incrementalCounter = new AtomicInteger();

    /** Array of disruptors. Each Disruptor in the appropriate stripe. */
    private final Disruptor<T>[] disruptors;

    /** Array of Ring buffer. It placed according to disruptors in the array. */
    private final RingBuffer<T>[] queues;

    /** Disruptor event handler array. It placed according to disruptors in the array. */
    private final ArrayList<EventHandler<T>> eventHandlers;

    /** Disruptor error handler array. It placed according to disruptors in the array. */
    private final ArrayList<StripeExceptionHandler> exceptionHandlers;

    /** Amount of stripes. */
    private final int stripes;

    /** The Striped disruptor name. */
    private final String name;

    private final DisruptorMetrics metrics;

    /** If it is true, the disruptor batch shares across all subscribers. Otherwise, the batch sends for each one. */
    private final boolean sharedStripe;

    /**
     * Creates a disruptor for the specific RAFT group. This type of disruption is intended for only one group.
     *
     * @param nodeName Name of the Ignite node.
     * @param poolName Name of the pool.
     * @param threadFactorySupplier Function that produces a thread factory given stripe name.
     * @param bufferSize Buffer size for each Disruptor.
     * @param eventFactory Event factory for the Striped disruptor.
     * @param useYieldStrategy If {@code true}, the yield strategy is to be used, otherwise the blocking strategy.
     * @param metrics Metrics.
     * @param <U> Type of disruptor events.
     * @return A disruptor instance.
     */
    public static <U extends INodeIdAware> StripedDisruptor<U> createSerialDisruptor(
            String nodeName,
            String poolName,
            BiFunction<String, IgniteLogger, ThreadFactory> threadFactorySupplier,
            int bufferSize,
            EventFactory<U> eventFactory,
            boolean useYieldStrategy,
            @Nullable DisruptorMetrics metrics

    ) {
        return new StripedDisruptor<>(
                nodeName,
                poolName,
                threadFactorySupplier,
                bufferSize,
                eventFactory,
                1,
                true,
                useYieldStrategy,
                metrics
        );
    }

    /**
     * @param nodeName Name of the Ignite node.
     * @param poolName Name of the pool.
     * @param threadFactorySupplier Function that produces a thread factory given stripe name.
     * @param bufferSize Buffer size for each Disruptor.
     * @param eventFactory Event factory for the Striped disruptor.
     * @param stripes Amount of stripes.
     * @param sharedStripe If it is true, the disruptor batch shares across all subscribers. Otherwise, the batch sends for each
     *         one.
     * @param useYieldStrategy If {@code true}, the yield strategy is to be used, otherwise the blocking strategy.
     * @param raftMetrics Metrics.
     */
    public StripedDisruptor(
            String nodeName,
            String poolName,
            BiFunction<String, IgniteLogger, ThreadFactory> threadFactorySupplier,
            int bufferSize,
            EventFactory<T> eventFactory,
            int stripes,
            boolean sharedStripe,
            boolean useYieldStrategy,
            @Nullable DisruptorMetrics raftMetrics
    ) {
        disruptors = new Disruptor[stripes];
        queues = new RingBuffer[stripes];
        eventHandlers = new ArrayList<>(stripes);
        exceptionHandlers = new ArrayList<>(stripes);
        this.stripes = stripes;
        this.name = IgniteThread.threadPrefix(nodeName, poolName);
        this.sharedStripe = sharedStripe;
        this.metrics = raftMetrics;

        for (int i = 0; i < stripes; i++) {
            String stripeName = format("{}_stripe_{}", poolName, i);

            Disruptor<T> disruptor = DisruptorBuilder.<T>newInstance()
                    .setRingBufferSize(bufferSize)
                    .setEventFactory(eventFactory)
                    .setThreadFactory(threadFactorySupplier.apply(stripeName, LOG))
                    .setProducerType(ProducerType.MULTI)
                    .setWaitStrategy(new PhasedBackoffWaitStrategy(100, 0, TimeUnit.MICROSECONDS, new BlockingWaitStrategy()))
                    .build();

            eventHandlers.add(handler(i));
            exceptionHandlers.add(new StripeExceptionHandler(name));

            disruptor.handleEventsWith(eventHandlers.get(i));
            disruptor.setDefaultExceptionHandler(exceptionHandlers.get(i));

            queues[i] = disruptor.start();
            disruptors[i] = disruptor;
        }
    }

    protected EventHandler<T> handler(int i) {
        return new StripeEntryHandler(i);
    }

    /**
     * Shutdowns all nested disruptors.
     */
    public void shutdown() {
        for (int i = 0; i < stripes; i++) {
            if (disruptors[i] != null) {
                disruptors[i].shutdown();
            }

            // Help GC to collect unused resources.
            queues[i] = null;
            disruptors[i] = null;
        }

        eventHandlers.clear();
        exceptionHandlers.clear();
    }

    public RingBuffer<T> subscribe(NodeId nodeId, EventHandler<T> handler, DisruptorEventSourceType type,
            BiConsumer<T, Throwable> exceptionHandler) {
        int stripeId = getStripe(nodeId);

        if (stripeId == -1) {
            stripeId = nextStripeToSubscribe();
            stripeMapper.put(nodeId, stripeId);

            LOG.info("Node {}:{} mapped to {}{}", nodeId.toString(), type, name, stripeId);
        }

        queues[stripeId].publishEvent((event, sequence) -> {
            event.reset();

            event.setEvtType(SUBSCRIBE);
            event.setSrcType(type);
            event.setNodeId(nodeId);
            event.setHandler((EventHandler<INodeIdAware>) handler);
        });

        if (exceptionHandler != null) {
            // TODO is this correct?
            exceptionHandlers.get(stripeId).subscribe(nodeId, exceptionHandler);
        }

        return queues[stripeId];
    }

    public void unsubscribe(NodeId nodeId, DisruptorEventSourceType type) {
        int stripeId = getStripe(nodeId);

        if (stripeId == -1) {
            return; // Already unsubscribed.
        }

        //stripeMapper.remove(nodeId);

        queues[stripeId].publishEvent((event, sequence) -> {
            event.reset();

            event.setEvtType(SUBSCRIBE);
            event.setSrcType(type);
            event.setNodeId(nodeId);
            event.setHandler(null);
        });

        exceptionHandlers.get(stripeId).unsubscribe(nodeId);
    }

    /**
     * If the replication group is already subscribed, this method determines a stripe by a node id and returns a stripe number. If the
     * replication group did not subscribed yet, this method returns {@code -1};
     *
     * @param nodeId Node id.
     * @return Stripe of the Striped disruptor.
     */
    public int getStripe(NodeId nodeId) {
        return stripeMapper.getOrDefault(nodeId, -1);
    }

    /**
     * Generates the next stripe number in a round-robin manner.
     *
     * @return The stripe number.
     */
    private int nextStripeToSubscribe() {
        return Math.abs(incrementalCounter.getAndIncrement() % stripes);
    }

    /**
     * Event handler for stripe of the Striped disruptor. It routes an event to the event handler for a group.
     */
    private class StripeEntryHandler implements EventHandler<T> {
        private final Map<NodeId, EnumMap<DisruptorEventSourceType, EventHandler<T>>> subscribers = new HashMap<>();

        /** Holds last unprocessed event in non shared event loop mode. */
        private final Map<NodeId, T> eventCache = new HashMap<>();

        /** Stripe id. */
        private final int stripeId;

        /**
         * The constructor.
         */
        StripeEntryHandler(int stripeId) {
            this.stripeId = stripeId;
        }

        /** {@inheritDoc} */
        @Override
        public void onEvent(T event, long sequence, boolean endOfBatch) throws Exception {
            if (event.getEvtType() == SUBSCRIBE) {
                if (endOfBatch) {
                    consumeBatch(sequence);
                }

                if (event.getHandler() == null) {
                    subscribers.compute(event.nodeId(), (k, v) -> {
                        assert v != null;

                        v.remove(event.getSrcType());

                        if (v.isEmpty()) {
                            stripeMapper.remove(event.nodeId());
                        }

                        return v.isEmpty() ? null : v;
                    });
                } else {
                    subscribers.compute(event.nodeId(), (k, v) -> {
                        if (v == null) {
                            v = new EnumMap<>(DisruptorEventSourceType.class);
                        }

                        EventHandler<T> prev = v.put(event.getSrcType(), (EventHandler<T>) event.getHandler());

                        assert prev == null;

                        return v;
                    });
                }
            } else {
                internalBatching(event, sequence);

                if (endOfBatch) {
                    consumeBatch(sequence);
                }
            }
        }

        private void consumeBatch(long sequence) throws Exception {
            for (Map.Entry<NodeId, T> grpEvent : eventCache.entrySet()) {
                T prevEvent = grpEvent.getValue();

                EnumMap<DisruptorEventSourceType, EventHandler<T>> map = subscribers.get(prevEvent.nodeId());

                assert map != null : "Event handler is not registered for group " + prevEvent.nodeId() + ":" + prevEvent.getSrcType();

                EventHandler<T> grpHandler = map.get(grpEvent.getValue().getSrcType());

                assert grpHandler != null :
                        "Event handler is not registered for group " + prevEvent.nodeId() + ":" + prevEvent.getSrcType();

//                    if (grpHandler != null) {
//                                if (metrics != null && metrics.enabled()) {
//                                    metrics.hitToStripe(stripeId);
//
//                                    metrics.addBatchSize(currentBatchSizes.getOrDefault(grpEvent.getKey(), 0) + 1);
//                                }

                grpHandler.onEvent(grpEvent.getValue(), sequence, true);
//                    }
            }

            //currentBatchSizes.clear();
            eventCache.clear();
        }

        /**
         * Processes the event with intermediate cache to batch internally for each subscriber for the stripe.
         *
         * @param event Disruptor event to process.
         * @param sequence Number in the sequence of the element.
         * @throws Exception Throw when some handler fails.
         */
        private void internalBatching(T event, long sequence) throws Exception {
            NodeId pushNodeId = sharedStripe ? FAKE_NODE_ID : event.nodeId();

            T prevEvent = eventCache.put(pushNodeId, event);

            if (prevEvent != null) {
                EnumMap<DisruptorEventSourceType, EventHandler<T>> map = subscribers.get(prevEvent.nodeId());

                assert map != null : "Event handler is not registered for group " + prevEvent.nodeId() + ":" + prevEvent.getSrcType();

                EventHandler<T> grpHandler = map.get(prevEvent.getSrcType());

                assert grpHandler != null :
                        "Event handler is not registered for type " + prevEvent.nodeId() + ":" + prevEvent.getSrcType();

//                    if (grpHandler != null) {
//                    if (metrics != null && metrics.enabled()) {
//                        metrics.hitToStripe(stripeId);
//
//                        currentBatchSizes.compute(pushNodeId, (nodeId, cnt) -> {
//                            if (cnt == null) {
//                                return 1;
//                            }
//
//                            return cnt + 1;
//                        });
//                    }

                grpHandler.onEvent(prevEvent, sequence, false);
//                    }
            }
        }
    }

    /**
     * Striped disruptor exception handler. It prints into log when an exception has occurred and route it to the handler for group.
     */
    private class StripeExceptionHandler implements ExceptionHandler<T> {
        /** Name of the Disruptor instance. */
        private final String name;

        /** There are exception handlers per group. */
        private final ConcurrentHashMap<NodeId, BiConsumer<T, Throwable>> subscribers;

        /**
         * @param name Name of the Disruptor instance.
         */
        StripeExceptionHandler(String name) {
            this.name = name;
            this.subscribers = new ConcurrentHashMap<>();
        }

        /**
         * Subscribes a group to an exception, that might happen during handling an event for the group.
         *
         * @param nodeId Node id.
         * @param handler Exception handler.
         */
        void subscribe(NodeId nodeId, BiConsumer<T, Throwable> handler) {
            subscribers.put(nodeId, handler);
        }

        /**
         * Unsubscribes a group for any exception.
         *
         * @param nodeId Node id.
         */
        void unsubscribe(NodeId nodeId) {
            subscribers.remove(nodeId);
        }

        /** {@inheritDoc} */
        @Override
        public void handleOnStartException(Throwable ex) {
            LOG.error("Fail to start disruptor [name={}]", ex, name);
        }

        /** {@inheritDoc} */
        @Override
        public void handleOnShutdownException(Throwable ex) {
            LOG.error("Fail to shutdown disruptor [name={}]", ex, name);

        }

        /** {@inheritDoc} */
        @Override
        public void handleEventException(Throwable ex, long sequence, T event) {
            NodeId nodeId = event.nodeId();

            BiConsumer<T, Throwable> handler = nodeId == null ? null : subscribers.get(nodeId);

            LOG.error("Handle disruptor event error [name={}, event={}, hasHandler={}]", ex, name, event, handler != null);

            if (handler != null) {
                handler.accept(event, ex);
            }
        }
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return format("{} [name={}]", StripedDisruptor.class.getSimpleName(), name);
    }
}
