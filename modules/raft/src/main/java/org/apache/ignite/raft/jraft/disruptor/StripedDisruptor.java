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
package org.apache.ignite.raft.jraft.disruptor;

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
import org.apache.ignite.raft.jraft.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.ignite.lang.LoggerMessageHelper.format;

/**
 * Stripe Disruptor is a set of queues which process several independent groups in one queue (in the stripe).
 * It makes fewer threads that the groups and gives the same sequential guaranties and a close performance.
 *
 * @param <T> Event type. This event should implement {@link GroupAware} interface.
 */
public class StripedDisruptor<T extends GroupAware> {
    /** The logger. */
    private static final Logger LOG = LoggerFactory.getLogger(StripedDisruptor.class);

    private final Disruptor<T>[] disruptors;
    private final RingBuffer<T>[] queues;
    private final ArrayList<StripeEntryHandler> eventHandlers;
    private final ArrayList<StripeExceptionHandler> exceptionHandlers;
    private final int stripes;
    private final String name;

    public StripedDisruptor(String name, int bufferSize, EventFactory<T> eventFactory, int stripes) {
        disruptors = new Disruptor[stripes];
        queues = new RingBuffer[stripes];
        eventHandlers = new ArrayList<>(stripes);
        exceptionHandlers = new ArrayList<>(stripes);
        this.stripes = stripes;
        this.name = name;

        for (int i = 0; i < stripes; i++) {
            String stripeName = format("{}_stripe_{}-", name, i);

            Disruptor<T> disruptor = DisruptorBuilder.<T>newInstance()
                .setRingBufferSize(bufferSize)
                .setEventFactory(eventFactory)
                .setThreadFactory(new NamedThreadFactory(stripeName, true))
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

        LOG.info("Striped disruptor was started [name={}]", name);
    }

    /**
     * Shutdowns all nested disruptors.
     */
    public void shutdown() {
        for (int i = 0; i < stripes; i++)
            disruptors[i].shutdown();

        LOG.info("Striped disruptor stopped [name={}]", name);
    }

    public RingBuffer<T> subscribe(String group, EventHandler<T> handler) {
        return subscribe(group, handler, null);
    }

    public RingBuffer<T> subscribe(String group, EventHandler<T> handler, BiConsumer<T, Throwable> exceptionHandler) {
        eventHandlers.get(getStripe(group)).subscribe(group, handler);

        if (exceptionHandler != null)
            exceptionHandlers.get(getStripe(group)).subscribe(group, exceptionHandler);

        LOG.info("Consumer subscribed [poolName={}, group={}]", name, group);

        return queues[getStripe(group)];
    }

    public void unsubscribe(String group) {
        eventHandlers.get(getStripe(group)).unsubscribe(group);
        exceptionHandlers.get(getStripe(group)).unsubscribe(group);

        LOG.info("Consumer subscribed [poolName={}, group={}]", name, group);
    }

    private int getStripe(String group) {
        return Math.abs(group.hashCode() % stripes);
    }

    public RingBuffer<T> queue(String groupId) {
        return queues[getStripe(groupId)];
    }

    private class StripeEntryHandler implements EventHandler<T> {
        private final ConcurrentHashMap<String, EventHandler<T>> subscrivers;

        StripeEntryHandler() {
            subscrivers = new ConcurrentHashMap<>();
        }

        void subscribe(String group, EventHandler<T> handler) {
            subscrivers.put(group, handler);
        }

        void unsubscribe(String group) {
            subscrivers.remove(group);
        }

        @Override public void onEvent(T event, long sequence, boolean endOfBatch) throws Exception {
            EventHandler<T> handler = subscrivers.get(event.groupId());

            assert handler != null : format("Group of the event is unsupported [group={}, event={}]", event.groupId(), event);

            handler.onEvent(event, sequence, endOfBatch);
        }
    }

    private class StripeExceptionHandler implements ExceptionHandler<T> {
        private final String name;
        private final ConcurrentHashMap<String, BiConsumer<T, Throwable>> subscrivers;

        StripeExceptionHandler(String name) {
            this.name = name;
            this.subscrivers = new ConcurrentHashMap<>();
        }

        void subscribe(String group, BiConsumer<T, Throwable> handler) {
            subscrivers.put(group, handler);
        }

        void unsubscribe(String group) {
            subscrivers.remove(group);
        }

        @Override public void handleOnStartException(Throwable ex) {
            LOG.error("Fail to start disruptor [name={}]", name, ex);
        }

        @Override public void handleOnShutdownException(Throwable ex) {
            LOG.error("Fail to shutdown disruptor [name={}]", name, ex);

        }

        @Override public void handleEventException(Throwable ex, long sequence, T event) {
            BiConsumer<T, Throwable> handler = subscrivers.get(event.groupId());

            LOG.error("Handle disruptor event error [name={}, event={}, hasHandler={}]", name, event, handler != null, ex);

            if (handler != null)
                handler.accept(event, ex);
        }
    }

    @Override public String toString() {
        return format("{} [name={}]", StripedDisruptor.class.getSimpleName(), name);
    }
}
