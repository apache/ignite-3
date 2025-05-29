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

package org.apache.ignite.internal.raft;

import static java.lang.Math.log;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.util.SlidingAverageValueTracker;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

public class ThrottlingContextHolderImpl implements ThrottlingContextHolder {
    private static final IgniteLogger LOG = Loggers.forClass(ThrottlingContextHolder.class);

    private static final double AVERAGE_VALUE_TRACKER_DEFAULT = 0.0;

    private final RaftConfiguration configuration;

    private final double maxInflightOverflowRate;

    private final Map<String, PeerContextHolder> peerContexts = new ConcurrentHashMap<>();

    @TestOnly
    public ThrottlingContextHolderImpl(RaftConfiguration configuration) {
        this(configuration, 1.0);
    }

    public ThrottlingContextHolderImpl(
            RaftConfiguration configuration,
            double maxInflightOverflowRate
    ) {
        this.configuration = configuration;
        this.maxInflightOverflowRate = maxInflightOverflowRate;
    }

    private PeerContextHolder peerContext(Peer peer) {
        return peerContexts.computeIfAbsent(peer.consistentId(), k -> new PeerContextHolder(peer));
    }

    @Override
    public boolean isOverloaded(Peer peer) {
        return peerContext(peer).isOverloaded();
    }

    @Override
    public void beforeRequest(Peer peer) {
        peerContext(peer).beforeRequest();
    }

    @Override
    public void afterRequest(Peer peer, long requestStartTimestamp, @Nullable Throwable err) {
        peerContext(peer).afterRequest(requestStartTimestamp, err);
    }

    @Override
    public long peerRequestTimeoutMillis(Peer peer) {
        return peerContext(peer).adaptiveResponseTimeoutMillis.get();
    }

    private class PeerContextHolder {
        private static final double INCREASE_MULTIPLIER = 2.0;
        private static final double INCREASE_ON_TIMEOUT_MULTIPLIER = 2.0;
        private static final double DECREASE_MULTIPLIER = 0.99;

        private final SlidingAverageValueTracker averageValueTracker =
                new SlidingAverageValueTracker(50, 20, AVERAGE_VALUE_TRACKER_DEFAULT);

        private final Peer peer;

        private final long decreaseDelay;

        PeerContextHolder(Peer peer) {
            this.peer = peer;

            this.decreaseDelay = (long) (configuration.retryTimeoutMillis().value() * 2 / (log (0.5) / log(DECREASE_MULTIPLIER)));
        }

        private final AtomicInteger currentInFlights = new AtomicInteger();

        private AtomicLong adaptiveResponseTimeoutMillis = new AtomicLong(3000);

        private volatile long lastDecTime = System.currentTimeMillis();

        boolean isOverloaded() {
            return currentInFlights.get() >= computeMaxInFlights() * maxInflightOverflowRate;
        }

        int computeMaxInFlights() {
            double avg = averageValueTracker.avg();

            return avg == AVERAGE_VALUE_TRACKER_DEFAULT
                    ? Integer.MAX_VALUE
                    : (int) max(adaptiveResponseTimeoutMillis.get() / avg, 1.0);
        }

        void beforeRequest() {
            currentInFlights.incrementAndGet();
        }

        void afterRequest(long requestStartTimestamp, @Nullable Throwable err) {
            currentInFlights.decrementAndGet();

            err = unwrapCause(err);

            if (err == null || err instanceof TimeoutException) {
                long now = System.currentTimeMillis();
                long duration = System.currentTimeMillis() - requestStartTimestamp;

                averageValueTracker.record(duration);

                adaptRequestTimeout(now, err instanceof TimeoutException);
            }
        }

        private void adaptRequestTimeout(long now, boolean timedOut) {
            double avg = averageValueTracker.avg();
            long defaultResponseTimeout = configuration.responseTimeoutMillis().value();
            long retryTimeout = configuration.retryTimeoutMillis().value();
            long r = adaptiveResponseTimeoutMillis.get();

            if (avg < r * 0.3 && r > configuration.responseTimeoutMillis().value()) {
                if (now - lastDecTime > decreaseDelay) {
                    if (adaptiveResponseTimeoutMillis.compareAndSet(r, (long) max(defaultResponseTimeout, r * DECREASE_MULTIPLIER))) {
                        LOG.info("Adaptive response timeout changed for peer={}: {} from {} to {}; avg={}",
                                peer.consistentId(), "DECREMENTED", r, adaptiveResponseTimeoutMillis.get(), avg);
                        lastDecTime = now;
                    }
                }
            }

            while (true) {
                r = adaptiveResponseTimeoutMillis.get();

                if (avg >= r * 0.7 && r < retryTimeout) {
                    if (adaptiveResponseTimeoutMillis.compareAndSet(r, (long) min(retryTimeout, r * INCREASE_MULTIPLIER))) {
                        LOG.info("Adaptive response timeout changed for peer={}: {} from {} to {}; avg={}",
                                peer.consistentId(), "INCREMENTED", r, adaptiveResponseTimeoutMillis.get(), avg);

                        break;
                    }
                } else if (timedOut) {
                    if (adaptiveResponseTimeoutMillis.compareAndSet(r, (long) min(retryTimeout, r * INCREASE_ON_TIMEOUT_MULTIPLIER))) {
                        LOG.info("Adaptive response timeout changed for peer={}: {} from {} to {}; avg={}",
                                peer.consistentId(), "INCREMENTED ON TIMEOUT", r, adaptiveResponseTimeoutMillis.get(), avg);

                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }

    long adaptiveResponseTimeoutMillis(Peer peer) {
        return peerContext(peer).adaptiveResponseTimeoutMillis.get();
    }
}
