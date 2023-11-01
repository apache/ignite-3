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
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.raft.jraft.entity.NodeId;

/**
 * Stripe Disruptor is a set of queues which process several independent groups in one queue (in the stripe).
 * It makes fewer threads that the groups and gives the same sequential guaranties and a close performance.
 *
 * @param <T> Event type. This event should implement {@link NodeIdAware} interface.
 */
public class StripedDisruptor<T extends NodeIdAware> {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(StripedDisruptor.class);

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

    /**
     * If {@code false}, this stripe will always pass {@code true} into {@link EventHandler#onEvent(Object, long, boolean)}.
     * Otherwise, the data will be provided with batches.
     */
    //TODO: IGNITE-15568 endOfBatch should be set to true to prevent caching tasks until IGNITE-15568 has fixed.
    private final boolean supportsBatches;

    /**
     * @param name Name of the Striped disruptor.
     * @param bufferSize Buffer size for each Disruptor.
     * @param eventFactory Event factory for the Striped disruptor.
     * @param stripes Amount of stripes.
     * @param supportsBatches If {@code false}, this stripe will always pass {@code true} into
     *      {@link EventHandler#onEvent(Object, long, boolean)}. Otherwise, the data will be provided with batches.
     */
    public StripedDisruptor(String name, int bufferSize, EventFactory<T> eventFactory, int stripes, boolean supportsBatches) {
        disruptors = new Disruptor[stripes];
        queues = new RingBuffer[stripes];
        eventHandlers = new ArrayList<>(stripes);
        exceptionHandlers = new ArrayList<>(stripes);
        this.stripes = stripes;
        this.name = name;
        this.supportsBatches = supportsBatches;

        for (int i = 0; i < stripes; i++) {
            String stripeName = format("{}_stripe_{}-", name, i);

            Disruptor<T> disruptor = DisruptorBuilder.<T>newInstance()
                .setRingBufferSize(bufferSize)
                .setEventFactory(eventFactory)
                .setThreadFactory(new NamedThreadFactory(stripeName, true, LOG))
                .setProducerType(ProducerType.MULTI)
                .setWaitStrategy(new BlockingWaitStrategy())
                .build();

            eventHandlers.add(new StripeEntryHandler());
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
        eventHandlers.get(getStripe(nodeId)).subscribe(nodeId, handler);

        if (exceptionHandler != null)
            exceptionHandlers.get(getStripe(nodeId)).subscribe(nodeId, exceptionHandler);

        return queues[getStripe(nodeId)];
    }

    /**
     * Unsubscribes group for the Striped disruptor.
     *
     * @param nodeId Node id.
     */
    public void unsubscribe(NodeId nodeId) {
        eventHandlers.get(getStripe(nodeId)).unsubscribe(nodeId);
        exceptionHandlers.get(getStripe(nodeId)).unsubscribe(nodeId);
    }

    /**
     * Determines a stripe by a node id and returns a stripe number.
     *
     * @param nodeId Node id.
     * @return Stripe of the Striped disruptor.
     */
    public int getStripe(NodeId nodeId) {
        return Math.abs(nodeId.hashCode() % stripes);
    }

    /**
     * Determines a Disruptor queue by a group id.
     *
     * @param nodeId Node id.
     * @return Disruptor queue appropriate to the group.
     */
    public RingBuffer<T> queue(NodeId nodeId) {
        return queues[getStripe(nodeId)];
    }

    /**
     * Event handler for stripe of the Striped disruptor.
     * It routs an event to the event handler for a group.
     */
    private class StripeEntryHandler implements EventHandler<T> {
        private final ConcurrentHashMap<NodeId, EventHandler<T>> subscribers;

        /**
         * The constructor.
         */
        StripeEntryHandler() {
            subscribers = new ConcurrentHashMap<>();
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
