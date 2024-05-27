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

package org.apache.ignite.internal.components;

import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * Class for detection of long JVM pauses. It has a worker thread, which wakes up in cycle every {@code PRECISION} (default is 50)
 * milliseconds, and monitors a time values between awakenings. If worker pause exceeds the expected value more than {@code THRESHOLD}
 * default is 500), the difference is considered as JVM pause, most likely STW, and event of long JVM pause is registered. The values of
 * {@code PRECISION}, {@code THRESHOLD} and {@code EVT_CNT} (event window size, default is 20) can be configured in system or environment
 * properties IGNITE_JVM_PAUSE_DETECTOR_PRECISION, IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD and IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT
 * accordingly.
 */
public class LongJvmPauseDetector implements IgniteComponent {
    private final IgniteLogger log = Loggers.forClass(LongJvmPauseDetector.class);

    /** Ignite JVM pause detector threshold default value. */
    public static final int DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD = 500;

    /** System property to change default is IGNITE_JVM_PAUSE_DETECTOR_PRECISION. */
    public static final int DFLT_JVM_PAUSE_DETECTOR_PRECISION = 50;

    /** System property to change default is IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT. */
    public static final int DFLT_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT = 20;

    /** Precision. */
    private static final int PRECISION = getInteger("IGNITE_JVM_PAUSE_DETECTOR_PRECISION", DFLT_JVM_PAUSE_DETECTOR_PRECISION);

    /** Threshold. */
    private final int threshold = getInteger("IGNITE_JVM_PAUSE_DETECTOR_THRESHOLD", DEFAULT_JVM_PAUSE_DETECTOR_THRESHOLD);

    /** Event count. */
    private static final int EVT_CNT = getInteger("IGNITE_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT",
            DFLT_JVM_PAUSE_DETECTOR_LAST_EVENTS_COUNT);

    /** Disabled flag. */
    private static final boolean DISABLED = getBoolean("IGNITE_JVM_PAUSE_DETECTOR_DISABLED");

    /** Worker reference. */
    private final AtomicReference<Thread> workerRef = new AtomicReference<>();

    /** Long pause count. */
    private long longPausesCnt;

    /** Long pause total duration. */
    private long longPausesTotalDuration;

    /** Last detector's wake up time. */
    private long lastWakeUpTime;

    /** Long pauses timestamps. */
    private final long[] longPausesTimestamps = new long[EVT_CNT];

    /** Long pauses durations. */
    private final long[] longPausesDurations = new long[EVT_CNT];

    private final String nodeName;

    public LongJvmPauseDetector(String nodeName) {
        this.nodeName = nodeName;
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        if (DISABLED) {
            log.debug("JVM Pause Detector is disabled");

            return nullCompletedFuture();
        }

        final Thread worker = new Thread(NamedThreadFactory.threadPrefix(nodeName, "jvm-pause-detector-worker")) {

            @Override
            public void run() {
                synchronized (LongJvmPauseDetector.this) {
                    lastWakeUpTime = System.currentTimeMillis();
                }

                log.debug("Detector worker has been started [thread={}]", getName());

                while (true) {
                    try {
                        Thread.sleep(PRECISION);

                        final long now = System.currentTimeMillis();
                        final long pause = now - PRECISION - lastWakeUpTime;

                        if (pause >= threshold) {
                            log.warn("Possible too long JVM pause [duration={}ms]", pause);

                            synchronized (LongJvmPauseDetector.this) {
                                final int next = (int) (longPausesCnt % EVT_CNT);

                                longPausesCnt++;

                                longPausesTotalDuration += pause;

                                longPausesTimestamps[next] = now;

                                longPausesDurations[next] = pause;

                                lastWakeUpTime = now;
                            }
                        } else {
                            synchronized (LongJvmPauseDetector.this) {
                                lastWakeUpTime = now;
                            }
                        }
                    } catch (InterruptedException e) {
                        if (workerRef.compareAndSet(this, null)) {
                            log.debug("Thread has been interrupted [thread={}]", e, getName());
                        } else {
                            log.debug("Thread has been stopped [thread={}]", getName());
                        }

                        break;
                    }
                }
            }
        };

        if (!workerRef.compareAndSet(null, worker)) {
            log.debug("{} already started", LongJvmPauseDetector.class.getSimpleName());

            return nullCompletedFuture();
        }

        worker.setDaemon(true);
        worker.start();

        log.debug("{} was successfully started", LongJvmPauseDetector.class.getSimpleName());

        return nullCompletedFuture();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        final Thread worker = workerRef.getAndSet(null);

        if (worker != null && worker.isAlive() && !worker.isInterrupted()) {
            worker.interrupt();
        }

        return nullCompletedFuture();
    }

    /**
     * Gets a logging status.
     *
     * @return {@code false} if IGNITE_JVM_PAUSE_DETECTOR_DISABLED set to {@code true}, and {@code true} otherwise.
     */
    public static boolean enabled() {
        return !DISABLED;
    }

    /**
     * Gets a pause count.
     *
     * @return Long JVM pauses count.
     */
    synchronized long longPausesCount() {
        return longPausesCnt;
    }

    /**
     * Gets a total duration.
     *
     * @return Long JVM pauses total duration.
     */
    synchronized long longPausesTotalDuration() {
        return longPausesTotalDuration;
    }

    /**
     * Gets a last wakeup time.
     *
     * @return Last checker's wake up time.
     */
    public synchronized long getLastWakeUpTime() {
        return lastWakeUpTime;
    }

    /**
     * Gets long pause events.
     *
     * @return Last long JVM pause events.
     */
    synchronized Map<Long, Long> longPauseEvents() {
        final Map<Long, Long> evts = new TreeMap<>();

        for (int i = 0; i < longPausesTimestamps.length && longPausesTimestamps[i] != 0; i++) {
            evts.put(longPausesTimestamps[i], longPausesDurations[i]);
        }

        return evts;
    }

    /**
     * Gets the last long pause event.
     *
     * @return Pair ({@code last long pause event time}, {@code pause time duration}) or {@code null}, if long pause wasn't occurred.
     */
    public synchronized @Nullable IgniteBiTuple<Long, Long> getLastLongPause() {
        int lastPauseIdx = (int) ((EVT_CNT + longPausesCnt - 1) % EVT_CNT);

        if (longPausesTimestamps[lastPauseIdx] == 0) {
            return null;
        }

        return new IgniteBiTuple<>(longPausesTimestamps[lastPauseIdx], longPausesDurations[lastPauseIdx]);
    }

    /**
     * Return long JVM pause threshold in mills.
     */
    public long longJvmPauseThreshold() {
        return threshold;
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(LongJvmPauseDetector.class, this);
    }
}
