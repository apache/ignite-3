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

import static org.apache.ignite.raft.jraft.disruptor.DisruptorEventType.SUBSCRIBE;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiFunction;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.metrics.sources.RaftMetricSource.DisruptorMetrics;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.jetbrains.annotations.Nullable;

/**
 * Stripe Disruptor is a set of queues which process several independent groups in one queue (in the stripe). It makes fewer threads that
 * the groups and gives the same sequential guaranties and a close performance.
 *
 * @param <T> Event type. This event should implement {@link INodeIdAware} interface.
 */
public class SharedStripedDisruptor<T extends INodeIdAware> extends StripedDisruptor<T> {
    public SharedStripedDisruptor(String nodeName, String poolName, BiFunction<String, IgniteLogger, ThreadFactory> threadFactorySupplier,
            int bufferSize, EventFactory<T> eventFactory, int stripes, boolean sharedStripe, boolean useYieldStrategy,
            @Nullable DisruptorMetrics raftMetrics) {
        super(nodeName, poolName, threadFactorySupplier, bufferSize, eventFactory, stripes, sharedStripe, useYieldStrategy, raftMetrics);
    }

    @Override
    protected EventHandler<T> handler(int i) {
        return new SharedStripeEntryHandler(i);
    }

    /**
     * Event handler for stripe of the Striped disruptor. It routes an event to the event handler for a group.
     */
    private class SharedStripeEntryHandler implements EventHandler<T> {
        private final Map<NodeId, EnumMap<DisruptorEventSourceType, EventHandler<T>>> subscribers = new HashMap<>();

        /** Holds events of different type in shared event loop mode. */
        private final Map<NodeId, List<T>> sharedEventCache = new HashMap<>();

        /** Stripe id. */
        private final int stripeId;

        /**
         * The constructor.
         */
        SharedStripeEntryHandler(int stripeId) {
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
            for (Entry<NodeId, List<T>> entry : sharedEventCache.entrySet()) {
                List<T> cached = entry.getValue();

                // TODO support different types reordering within batch.
                for (int i = 0; i < cached.size(); i++) {
                    T t = cached.get(i);

                    boolean endB = true;

                    if (i != cached.size() - 1) {
                        T next = cached.get(i + 1);

                        // Batch events of same class.
                        if (next.getSrcType() == t.getSrcType()) {
                            endB = false;
                        }
                    }

                    EnumMap<DisruptorEventSourceType, EventHandler<T>> map = subscribers.get(t.nodeId());
                    assert map != null : "Event handler is not registered for group " + t.nodeId() + ":" + t.getSrcType();
                    EventHandler<T> grpHandler = map.get(t.getSrcType());
                    assert grpHandler != null :
                            "Event handler is not registered for group " + t.nodeId() + ":" + t.getSrcType();

                    grpHandler.onEvent(t, sequence, endB);
                }
            }

            sharedEventCache.clear();
        }

        /**
         * Processes the event with intermediate cache to batch internally for each subscriber for the stripe.
         *
         * @param event Disruptor event to process.
         * @param sequence Number in the sequence of the element.
         * @throws Exception Throw when some handler fails.
         */
        private void internalBatching(T event, long sequence) throws Exception {
            // Shared stripe accumulates events from different groups.
            // Currently only log events can be batched together.
            NodeId nodeId = sharedStripe && event.getSrcType() == DisruptorEventSourceType.LOG ? FAKE_NODE_ID : event.nodeId();

            sharedEventCache.compute(nodeId, (k, v) -> {
                if (v == null) {
                    v = new ArrayList<>(); // Use Avg batch size TODO.
                }

                v.add(event);

                return v;
            });
        }
    }
}
