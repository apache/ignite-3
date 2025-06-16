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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.util.SlidingAverageValueTracker;
import org.apache.ignite.internal.util.SlidingHistogram;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Throttling context holder implementation.
 * This class has the map storing contexts for each peer, because the {@link org.apache.ignite.raft.jraft.disruptor.StripedDisruptor}
 * is shared between groups on the peers.
 */
public class ThrottlingContextHolderImpl implements ThrottlingContextHolder {
    private static final IgniteLogger LOG = Loggers.forClass(ThrottlingContextHolder.class);

    private final RaftConfiguration configuration;

    private final double maxInflightOverflowRate;

    private final Map<String, PeerContextHolder> peerContexts = new ConcurrentHashMap<>();

    private static final boolean ENABLED = IgniteSystemProperties.getBoolean("RAFT_CLIENT_THROTTLING_ENABLED", true);

    @TestOnly
    public ThrottlingContextHolderImpl(RaftConfiguration configuration) {
        this(configuration, 1.3);
    }

    /**
     * Constructor.
     *
     * @param configuration Raft configuration.
     * @param maxInflightOverflowRate Maximum inflight overflow multiplier.
     */
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
    public boolean isOverloaded(Peer peer, String requestClassName) {
        if (!ENABLED) {
            return false;
        }

        return peerContext(peer).isOverloaded();
    }

    @Override
    public void beforeRequest(Peer peer) {
        if (!ENABLED) {
            return;
        }

        peerContext(peer).beforeRequest();
    }

    @Override
    public void afterRequest(Peer peer, long requestStartTimestamp, @Nullable Boolean retriableError) {
        if (!ENABLED) {
            return;
        }

        peerContext(peer).afterRequest(requestStartTimestamp, retriableError);
    }

    @Override
    public long peerRequestTimeoutMillis(Peer peer) {
        if (!ENABLED) {
            return 3000;
        }

        return peerContext(peer).adaptiveResponseTimeoutMillis.get();
    }

    private class PeerContextHolder {
        private static final double INCREASE_MULTIPLIER = 2.0;
        private static final double DECREASE_MULTIPLIER = 0.99;

        private static final int AVERAGE_VALUE_TRACKER_WINDOW_SIZE = 50;
        private static final int AVERAGE_VALUE_TRACKER_MINIMIM_VALUES = 20;
        private static final double AVERAGE_VALUE_TRACKER_DEFAULT = 0.0;

        private static final int HISTOGRAM_WINDOW_SIZE = 1000;
        private static final double HISTOGRAM_PERCENTILE = 0.98;
        private static final long HISTOGRAM_ESTIMATION_DEFAULT = 0;

        private final SlidingHistogram histogram = new SlidingHistogram(HISTOGRAM_WINDOW_SIZE, HISTOGRAM_ESTIMATION_DEFAULT);

        private final Peer peer;

        /**
         * Delay in milliseconds after which the response timeout is decreased.
         * It prevents the timeout from decreasing too fast. When {@link TimeoutException} happens, this
         * may be a sign that the peer is overloaded, and it's better to decrease the timeout slowly
         * to avoid excessive retries.
         */
        private final long decreaseDelay;

        /** Counter of current number of requests in-flight. */
        private final LongAdder currentInFlights = new LongAdder();

        /**
         * Response timeout in milliseconds. This value is adapted if the average response time from peer grows or
         * single {@link TimeoutException} happens. Increasing it prevents the excessive retries of requests to the peer,
         * which may cause unnecessary stress on the peer and make the situation worse.
         */
        private final AtomicLong adaptiveResponseTimeoutMillis;

        /** When the response timeout was last decreased. */
        private volatile long lastDecreaseTime = System.currentTimeMillis();

        PeerContextHolder(Peer peer) {
            this.peer = peer;

            // Number of iterations to return to the default response timeout, when each iteration
            // is multiplication on DECREASE_MULTIPLIER.
            // i = INCREASE_MULTIPLIER
            // d = DECREASE_MULTIPLIER
            // t = default timeout value
            // n = numberOfIterationsToReturnToDefault
            // Find n such that:
            // i * t * d^n <= t
            // i * d^n <= 1
            // d^n <= 1/i
            // ln(d^n) <= ln(1/i)
            // n * ln(d) <= -ln(i)
            // 0 < d < 1
            // n = -ln(i) / ln(d)
            int numberOfIterationsToReturnToDefault = (int) (- log(INCREASE_MULTIPLIER) / log(DECREASE_MULTIPLIER));

            // The delay after which the response timeout can be decreased. For smooth decreasing, let the total time be 2 * retryTimeout.
            this.decreaseDelay = 2 * configuration.retryTimeoutMillis().value() / numberOfIterationsToReturnToDefault;
            this.adaptiveResponseTimeoutMillis = new AtomicLong(configuration.responseTimeoutMillis().value());
        }

        /**
         * Checks if the peer is overloaded. The assumption is based on the number of in-flight requests
         * and the maximum allowed in-flight requests, calculated from the average request duration and
         * the value of {@link #adaptiveResponseTimeoutMillis}.
         *
         * @return Whether the peer is overloaded or not.
         */
        boolean isOverloaded() {
            return currentInFlights.longValue() >= computeMaxInFlights() * maxInflightOverflowRate;
        }

        int computeMaxInFlights() {
            long timeForMostOfRequests = histogram.estimatePercentile(HISTOGRAM_PERCENTILE);

            return timeForMostOfRequests == HISTOGRAM_ESTIMATION_DEFAULT
                    ? Integer.MAX_VALUE
                    : (int) max((double) adaptiveResponseTimeoutMillis.get() / timeForMostOfRequests, 1.0);
        }

        void beforeRequest() {
            currentInFlights.increment();
        }

        void afterRequest(long requestStartTimestamp, Boolean retriableError) {
            currentInFlights.decrement();

            if (retriableError == null || retriableError) {
                long now = System.currentTimeMillis();
                long duration = now - requestStartTimestamp;

                histogram.record(duration);

                boolean timedOut = retriableError != null;
                adaptRequestTimeout(now, timedOut);
            }
        }

        private void adaptRequestTimeout(long now, boolean timedOut) {
            double avg = histogram.estimatePercentile(0.5);
            long defaultResponseTimeout = configuration.responseTimeoutMillis().value();
            long retryTimeout = configuration.retryTimeoutMillis().value();
            long r = adaptiveResponseTimeoutMillis.get();

            if (now - lastDecreaseTime > decreaseDelay
                    && avg < r * 0.3
                    && r > configuration.responseTimeoutMillis().value()) {
                if (adaptiveResponseTimeoutMillis.compareAndSet(r, (long) max(defaultResponseTimeout, r * DECREASE_MULTIPLIER))) {
                    LOG.debug("Adaptive response timeout changed [peer={}, action={}, from={}, to={}, avg={}].",
                            peer.consistentId(), "DECREMENTED", r, adaptiveResponseTimeoutMillis.get(), avg);

                    lastDecreaseTime = now;
                }
            }

            // Case of timeout exception may be dangerous so CAS is performed in loop.
            long newTimeout = (long) min(retryTimeout, r * INCREASE_MULTIPLIER);

            while (true) {
                r = adaptiveResponseTimeoutMillis.get();

                if (r >= retryTimeout) {
                    break;
                }

                if (avg >= r * 0.7 || timedOut) {
                    if (adaptiveResponseTimeoutMillis.compareAndSet(r, newTimeout)) {
                        LOG.debug("Adaptive response timeout changed [peer={}, action={}, from={}, to={}, avg={}, timedOut={}].",
                                peer.consistentId(), "INCREMENTED", r, adaptiveResponseTimeoutMillis.get(), avg, timedOut);

                        break;
                    }
                } else {
                    break;
                }
            }
        }
    }
}
