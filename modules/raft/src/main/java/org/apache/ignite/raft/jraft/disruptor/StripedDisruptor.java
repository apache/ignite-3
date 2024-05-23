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
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource.DisruptorMetrics;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.jetbrains.annotations.Nullable;

/**
 * Stripe Disruptor is a set of queues which process several independent groups in one queue (in the stripe). It makes fewer threads that
 * the groups and gives the same sequential guaranties and a close performance.
 *
 * @param <T> Event type. This event should implement {@link NodeIdAware} interface.
 */
public class StripedDisruptor<T extends NodeIdAware> {
    /**
     * It is an id that does not represent any node. It is used to collect events in the disruptor implementation,
     * where the particular node does not mean.
     */
    private final NodeId NOT_NODE_ID = new NodeId(null, null);

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(StripedDisruptor.class);

    /** The map stores a matching node id for the stripe that sends messages to the node. */
    private ConcurrentHashMap<NodeId, Integer> stripeMapper = new ConcurrentHashMap<>();

    /** The counter is used to generate the next stripe to subscribe to in order to be a round-robin hash. */
    private final AtomicInteger incrementalCounter = new AtomicInteger();

    /** Array of disruptors. Each Disruptor in the appropriate stripe. */
    private final Disruptor<T>[] disruptors;

    /** Array of Ring buffer. It placed according to disruptors in the array. */
    private final RingBuffer<T>[] queues;

    /** Disruptor event handler array. It placed according to disruptors in the array. */
    private final ArrayList<StripeEntryHandler> eventHandlers;

    /** Disruptor error handler array. It placed according to disruptors in the array. */
    private final ArrayList<StripeExceptionHandler> exceptionHandlers;

    /** Amount of stripes. */
    private final int stripes;

    /** The Striped disruptor name. */
    private final String name;

    private final DisruptorMetrics metrics;

    /**
     * If {@code false}, this stripe will always pass {@code true} into {@link EventHandler#onEvent(Object, long, boolean)}. Otherwise, the
     * data will be provided with batches.
     */
    private final boolean supportsBatches;

    /**
     * @param nodeName Name of the Ignite node.
     * @param poolName Name of the pool.
     * @param bufferSize Buffer size for each Disruptor.
     * @param eventFactory Event factory for the Striped disruptor.
     * @param stripes Amount of stripes.
     * @param supportsBatches If {@code false}, this stripe will always pass {@code true} into
     *         {@link EventHandler#onEvent(Object, long, boolean)}. Otherwise, the data will be provided with batches.
     * @param useYieldStrategy If {@code true}, the yield strategy is to be used, otherwise the blocking strategy.
     * @param metrics Metrics.
     */
    public StripedDisruptor(
            String nodeName,
            String poolName,
            int bufferSize,
            EventFactory<T> eventFactory,
            int stripes,
            boolean supportsBatches,
            boolean useYieldStrategy,
            @Nullable DisruptorMetrics metrics
    ) {
        this(
                nodeName,
                poolName,
                (igniteName, stripeName) -> NamedThreadFactory.create(igniteName, stripeName, true, LOG),
                bufferSize,
                eventFactory,
                stripes,
                supportsBatches,
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
     * @param supportsBatches If {@code false}, this stripe will always pass {@code true} into
     *         {@link EventHandler#onEvent(Object, long, boolean)}. Otherwise, the data will be provided with batches.
     * @param useYieldStrategy If {@code true}, the yield strategy is to be used, otherwise the blocking strategy.
     * @param raftMetrics Metrics.
     */
    public StripedDisruptor(
            String nodeName,
            String poolName,
            BiFunction<String, String, ThreadFactory> threadFactorySupplier,
            int bufferSize,
            EventFactory<T> eventFactory,
            int stripes,
            boolean supportsBatches,
            boolean useYieldStrategy,
            @Nullable DisruptorMetrics raftMetrics
    ) {
        disruptors = new Disruptor[stripes];
        queues = new RingBuffer[stripes];
        eventHandlers = new ArrayList<>(stripes);
        exceptionHandlers = new ArrayList<>(stripes);
        this.stripes = stripes;
        this.name = NamedThreadFactory.threadPrefix(nodeName, poolName);
        this.supportsBatches = supportsBatches;
        this.metrics = raftMetrics;

        for (int i = 0; i < stripes; i++) {
            String stripeName = format("{}_stripe_{}", poolName, i);

            Disruptor<T> disruptor = DisruptorBuilder.<T>newInstance()
                    .setRingBufferSize(bufferSize)
                    .setEventFactory(eventFactory)
                    .setThreadFactory(threadFactorySupplier.apply(nodeName, stripeName))
                    .setProducerType(ProducerType.MULTI)
                    .setWaitStrategy(useYieldStrategy ? new YieldingWaitStrategy() : new BlockingWaitStrategy())
                    .build();

            eventHandlers.add(new StripeEntryHandler(i));
            exceptionHandlers.add(new StripeExceptionHandler(name));

            disruptor.handleEventsWith(eventHandlers.get(i));
            disruptor.setDefaultExceptionHandler(exceptionHandlers.get(i));

            queues[i] = disruptor.start();
            disruptors[i] = disruptor;
        }
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

    /**
     * Subscribes an event handler to one stripe of the Striped disruptor. The stripe is determined by a group id.
     *
     * @param nodeId Node id.
     * @param handler Event handler for the group specified.
     * @return Disruptor queue appropriate to the group.
     */
    public RingBuffer<T> subscribe(NodeId nodeId, EventHandler<T> handler) {
        return subscribe(nodeId, handler, null);
    }

    /**
     * Subscribes an event handler and a exception handler to one stripe of the Striped disruptor. The stripe is determined by a group id.
     *
     * @param nodeId Node id.
     * @param handler Event handler for the group specified.
     * @param exceptionHandler Exception handler for the group specified.
     * @return Disruptor queue appropriate to the group.
     */
    public RingBuffer<T> subscribe(NodeId nodeId, EventHandler<T> handler, BiConsumer<T, Throwable> exceptionHandler) {
        assert getStripe(nodeId) == -1 : "The double subscriber for the one replication group [nodeId=" + nodeId + "].";

        int stripeId = nextStripeToSubscribe();

        stripeMapper.put(nodeId, stripeId);

        queues[stripeId].publishEvent((event, sequence) -> {
            event.type(SUBSCRIBE);
            event.nodeId(nodeId);
            event.handler((EventHandler<NodeIdAware>) handler);
        });

        if (exceptionHandler != null) {
            exceptionHandlers.get(stripeId).subscribe(nodeId, exceptionHandler);
        }

        return queues[stripeId];
    }

    /**
     * Unsubscribes group for the Striped disruptor.
     *
     * @param nodeId Node id.
     */
    public void unsubscribe(NodeId nodeId) {
        int stripeId = getStripe(nodeId);

        assert stripeId != -1 : "The replication group has not subscribed yet [nodeId=" + nodeId + "].";

        stripeMapper.remove(nodeId);

        queues[stripeId].publishEvent((event, sequence) -> {
            event.type(SUBSCRIBE);
            event.nodeId(nodeId);
            event.handler(null);
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
     * Event handler for stripe of the Striped disruptor. It routs an event to the event handler for a group.
     */
    private class StripeEntryHandler implements EventHandler<T> {
        private final Map<NodeId, EventHandler<T>> subscribers = new HashMap<>();

        /** The cache is used to correct handling the disruptor batch. */
        private final Map<NodeId, T> eventCache = new HashMap<>();

        /** Current batch sizes. */
        private final Map<NodeId, Integer> currentBatchSizes = new HashMap<>();

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
            if (event.type() == SUBSCRIBE) {
                if (event.handler() == null) {
                    subscribers.remove(event.nodeId());
                } else {
                    subscribers.put(event.nodeId(), (EventHandler<T>) event.handler());
                }
            } else {
                internalBatching(event, sequence);
            }

            if (endOfBatch) {
                for (Map.Entry<NodeId, T> grpEvent : eventCache.entrySet()) {
                    EventHandler<T> grpHandler = subscribers.get(grpEvent.getValue().nodeId());

                    if (grpHandler != null) {
                        if (metrics != null && metrics.enabled()) {
                            metrics.hitToStripe(stripeId);

                            metrics.addBatchSize(currentBatchSizes.getOrDefault(grpEvent.getKey(), 0) + 1);
                        }

                        grpHandler.onEvent(grpEvent.getValue(), sequence, true);
                    }
                }

                currentBatchSizes.clear();
                eventCache.clear();
            }
        }

        /**
         * Processes the event with intermediate cache to batch internally for each subscriber for the stripe.
         *
         * @param event Disruptor event to process.
         * @param sequence Number in the sequence of the element.
         * @throws Exception Throw when some handler fails.
         */
        private void internalBatching(T event, long sequence) throws Exception {
            NodeId pushNodeId = supportsBatches ? NOT_NODE_ID : event.nodeId();

            T prevEvent = eventCache.put(pushNodeId, event);

            if (prevEvent != null) {
                EventHandler<T> grpHandler = subscribers.get(prevEvent.nodeId());

                if (grpHandler != null) {
                    if (metrics != null && metrics.enabled()) {
                        metrics.hitToStripe(stripeId);

                        currentBatchSizes.compute(pushNodeId, (nodeId, cnt) -> {
                            if (cnt == null) {
                                return 1;
                            }

                            return cnt + 1;
                        });
                    }

                    grpHandler.onEvent(prevEvent, sequence, false);
                }
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
