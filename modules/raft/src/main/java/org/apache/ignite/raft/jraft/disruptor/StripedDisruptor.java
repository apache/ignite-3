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

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.ArrayList;
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
 * Stripe Disruptor is a set of queues which process several independent groups in one queue (in the stripe).
 * It makes fewer threads that the groups and gives the same sequential guaranties and a close performance.
 *
 * @param <T> Event type. This event should implement {@link NodeIdAware} interface.
 */
public class StripedDisruptor<T extends NodeIdAware> {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(StripedDisruptor.class);

    /** The counter is used to generate the next stripe to subscribe to in order to be a round-robin hash. */
    private final AtomicInteger incrementalCounter = new AtomicInteger();

    /** Array of disruptors. Each Disruptor in the appropriate stripe. */
    private final Disruptor<T>[] disruptors;

    /** Array of Ring buffer. It placed according to disruptors in the array. */
    private final RingBuffer<T>[] queues;

    /** Disruptor event handler array. It placed according to disruptors in the array.*/
    private final ArrayList<StripeEntryHandler> eventHandlers;

    /** Disruptor error handler array. It placed according to disruptors in the array.*/
    private final ArrayList<StripeExceptionHandler> exceptionHandlers;

    /** Amount of stripes. */
    private final int stripes;

    /** The Striped disruptor name. */
    private final String name;

    private final DisruptorMetrics metrics;

    /**
     * If {@code false}, this stripe will always pass {@code true} into {@link EventHandler#onEvent(Object, long, boolean)}.
     * Otherwise, the data will be provided with batches.
     */
    // TODO: IGNITE-15568 endOfBatch should be set to true to prevent caching tasks until IGNITE-15568 has fixed.
    private final boolean supportsBatches;

    /**
     * @param nodeName Name of the Ignite node.
     * @param poolName Name of the pool.
     * @param bufferSize Buffer size for each Disruptor.
     * @param eventFactory Event factory for the Striped disruptor.
     * @param stripes Amount of stripes.
     * @param supportsBatches If {@code false}, this stripe will always pass {@code true} into
     *      {@link EventHandler#onEvent(Object, long, boolean)}. Otherwise, the data will be provided with batches.
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
     *      {@link EventHandler#onEvent(Object, long, boolean)}. Otherwise, the data will be provided with batches.
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
            if (disruptors[i] != null)
                disruptors[i].shutdown();

            // Help GC to collect unused resources.
            queues[i] = null;
            disruptors[i] = null;
        }

        eventHandlers.clear();
        exceptionHandlers.clear();
    }

    /**
     * Subscribes an event handler to one stripe of the Striped disruptor.
     * The stripe is determined by a group id.
     *
     * @param nodeId Node id.
     * @param handler Event handler for the group specified.
     * @return Disruptor queue appropriate to the group.
     */
    public RingBuffer<T> subscribe(NodeId nodeId, EventHandler<T> handler) {
        return subscribe(nodeId, handler, null);
    }

    /**
     * Subscribes an event handler and a exception handler to one stripe of the Striped disruptor.
     * The stripe is determined by a group id.
     *
     * @param nodeId Node id.
     * @param handler Event handler for the group specified.
     * @param exceptionHandler Exception handler for the group specified.
     * @return Disruptor queue appropriate to the group.
     */
    public RingBuffer<T> subscribe(NodeId nodeId, EventHandler<T> handler, BiConsumer<T, Throwable> exceptionHandler) {
        assert getStripe(nodeId) == -1 : "The double subscriber for the one replication group [nodeId=" + nodeId + "].";

        int stripeId = nextStripeToSubscribe();

        eventHandlers.get(stripeId).subscribe(nodeId, handler);

        if (exceptionHandler != null)
            exceptionHandlers.get(stripeId).subscribe(nodeId, exceptionHandler);

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

        eventHandlers.get(stripeId).unsubscribe(nodeId);
        exceptionHandlers.get(stripeId).unsubscribe(nodeId);
    }

    /**
     * If the replication group is already subscribed, this method determines a stripe by a node id and returns a stripe number.
     * If the replication group did not subscribed yet, this method returns {@code -1};
     *
     * @param nodeId Node id.
     * @return Stripe of the Striped disruptor.
     */
    public int getStripe(NodeId nodeId) {
        for (StripeEntryHandler handler : eventHandlers) {
            if (handler.isSubscribed(nodeId)) {
                return handler.stripeId;
            }
        }

        return -1;
    }

    /**
     * Generates the next stripe number in a round-robin manner.
     * @return The stripe number.
     */
    private int nextStripeToSubscribe() {
        return Math.abs(incrementalCounter.getAndIncrement() % stripes);
    }

    /**
     * Determines a Disruptor queue by a group id.
     *
     * @param nodeId Node id.
     * @return Disruptor queue appropriate to the group.
     */
    public RingBuffer<T> queue(NodeId nodeId) {
        int stripeId = getStripe(nodeId);

        assert stripeId != -1 : "The replication group has not subscribed yet [nodeId=" + nodeId + "].";

        return queues[stripeId];
    }

    /**
     * Event handler for stripe of the Striped disruptor.
     * It routs an event to the event handler for a group.
     */
    private class StripeEntryHandler implements EventHandler<T> {
        private final ConcurrentHashMap<NodeId, EventHandler<T>> subscribers = new ConcurrentHashMap<>();

        /** Size of the batch that is currently being handled. */
        private int currentBatchSize = 0;

        /** Stripe id. */
        private final int stripeId;

        /**
         * The constructor.
         */
        StripeEntryHandler(int stripeId) {
            this.stripeId = stripeId;
        }

        /**
         * Checks the replication group is subscribed to this stripe or not.
         * @param nodeId Replication group node id.
         * @return True if the group is subscribed, false otherwise.
         */
        public boolean isSubscribed(NodeId nodeId) {
            return subscribers.containsKey(nodeId);
        }

        /**
         * Subscribes a group to appropriate events for it.
         *
         * @param nodeId Node id.
         * @param handler Event handler for the group specified.
         */
        void subscribe(NodeId nodeId, EventHandler<T> handler) {
            subscribers.put(nodeId, handler);
        }

        /**
         * Unsubscribes a group for any event.
         *
         * @param nodeId Node id.
         */
        void unsubscribe(NodeId nodeId) {
            subscribers.remove(nodeId);
        }

        /** {@inheritDoc} */
        @Override public void onEvent(T event, long sequence, boolean endOfBatch) throws Exception {
            EventHandler<T> handler = subscribers.get(event.nodeId());

            // TODO: IGNITE-20536 Need to add assert that handler is not null and to implement a no-op handler.
            if (handler != null) {
                if (metrics != null && metrics.enabled()) {
                    metrics.hitToStripe(stripeId);

                    if (endOfBatch) {
                        metrics.addBatchSize(currentBatchSize + 1);

                        currentBatchSize = 0;
                    } else {
                        currentBatchSize ++;
                    }
                }

                handler.onEvent(event, sequence, endOfBatch || subscribers.size() > 1 && !supportsBatches);
            } else {
                LOG.warn(format("Group of the event is unsupported [nodeId={}, event={}]", event.nodeId(), event));
            }
        }
    }

    /**
     * Striped disruptor exception handler.
     * It prints into log when an exception has occurred and route it to the handler for group.
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
        @Override public void handleOnStartException(Throwable ex) {
            LOG.error("Fail to start disruptor [name={}]", ex, name);
        }

        /** {@inheritDoc} */
        @Override public void handleOnShutdownException(Throwable ex) {
            LOG.error("Fail to shutdown disruptor [name={}]", ex, name);

        }

        /** {@inheritDoc} */
        @Override public void handleEventException(Throwable ex, long sequence, T event) {
            NodeId nodeId = event.nodeId();

            BiConsumer<T, Throwable> handler = nodeId == null ? null : subscribers.get(nodeId);

            LOG.error("Handle disruptor event error [name={}, event={}, hasHandler={}]", ex, name, event, handler != null);

            if (handler != null)
                handler.accept(event, ex);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return format("{} [name={}]", StripedDisruptor.class.getSimpleName(), name);
    }
}
