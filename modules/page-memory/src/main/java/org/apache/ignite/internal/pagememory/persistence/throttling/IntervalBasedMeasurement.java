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

package org.apache.ignite.internal.pagememory.persistence.throttling;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.Nullable;

/**
 * Speed tracker for determine speed of processing based on increments or exact counters value. <br>
 * Measurement is performed using several intervals (1 current + 3 historical by default). <br>
 * Too old measurements (intervals) may be dropped if automatic switch mode activated.<br>
 * To determine speed current measurement is reduced with all historical.<br>
 *     <br>
 *  For mode of manual measurements switch it is possible to use
 *  <br> default ctor
 *  {@link #IntervalBasedMeasurement()} and methods <br>
 *     {@link #setCounter(long, long)} (automatically opens interval if not opened) and <br>
 *     {@link #finishInterval()} to close measurement.<br>
 *     <br>
 *  For mode of automatic measurements switch it is possible to use
 *  <br> parametrized ctor
 *  {@link #IntervalBasedMeasurement(int, int)} and methods <br>
 *     {@link #setCounter(long, long)} (automatically opens interval if not opened) or
 *     {@link #addMeasurementForAverageCalculation(long)} to provide metrics value in addition to event.<br>
 *     {@link #finishInterval()} is also supported, but not required<br>
 *     <br>
 *
 *  <p>To get results of speed calculation it is possible to use <br>
 *  Method {@link #getSpeedOpsPerSec(long)} to get current speed (and swicth/open interval if needed). <br>
 *   or method {@link #getSpeedOpsPerSecReadOnly()} to get current speed without interval modification.<br>
 *
 *  <p>If metric value was provided using {@link #addMeasurementForAverageCalculation(long)}
 *  then method {@link #getAverage()} can be used to get resulting metrics average value during period of time.
 */
class IntervalBasedMeasurement {
    /** Nanos in second. */
    private static final long NANOS_IN_SECOND = TimeUnit.SECONDS.toNanos(1);

    /** Current Measurement interval atomic reference. */
    private final AtomicReference<MeasurementInterval> measurementIntervalAtomicRef = new AtomicReference<>();

    /** Interval automatic switch nanoseconds. Negative value means no automatic switch. */
    private final long intervalSwitchNanos;

    /** Max historical measurements to keep. */
    private final int maxMeasurements;

    /**
     * Previous (historical) measurements. One thread can write (winner in CAS of {@link
     * #measurementIntervalAtomicRef}), all other threads may read.
     */
    private final ConcurrentLinkedQueue<MeasurementInterval> prevMeasurements = new ConcurrentLinkedQueue<>();

    /**
     * Default constructor. No automatic switch, 3 historical measurements.
     */
    IntervalBasedMeasurement() {
        this(-1, 3);
    }

    /**
     * Constructor.
     *
     * @param intervalSwitchMs Interval switch milliseconds.
     * @param maxMeasurements Max historical measurements to keep.
     */
    IntervalBasedMeasurement(int intervalSwitchMs, int maxMeasurements) {
        this.intervalSwitchNanos = intervalSwitchMs > 0 ? TimeUnit.MILLISECONDS.toNanos(intervalSwitchMs) : -1;
        this.maxMeasurements = maxMeasurements;
    }

    /**
     * Gets speed, start interval (if not started).
     *
     * @param curNanoTime current time nanos.
     * @return Speed in pages per second based on current data.
     */
    long getSpeedOpsPerSec(long curNanoTime) {
        return calcSpeed(interval(true, curNanoTime), curNanoTime);
    }

    /**
     * Gets current speed, does not start measurement.
     *
     * @return Speed in pages per second based on current data.
     */
    long getSpeedOpsPerSecReadOnly() {
        MeasurementInterval interval = measurementIntervalAtomicRef.get();

        long endNanoTime = interval == null || interval.endNanoTime == 0
                ? System.nanoTime()
                : interval.endNanoTime;

        return calcSpeed(interval, endNanoTime);
    }

    /**
     * Reduce measurements to calculate average speed.
     *
     * @param interval Current measurement.
     * @param curNanoTime Current time in nanoseconds, according to which we'll calculate the speed.
     * @return Speed in operations per second from historical only measurements.
     */
    private long calcSpeed(@Nullable MeasurementInterval interval, long curNanoTime) {
        long nanosPassed = 0;
        long opsDone = 0;

        if (!isOutdated(interval, curNanoTime)) {
            nanosPassed += curNanoTime - interval.startNanoTime;
            opsDone += interval.cntr.get();
        }

        for (MeasurementInterval prevMeasurement : prevMeasurements) {
            if (!isOutdated(prevMeasurement, curNanoTime)) {
                nanosPassed += prevMeasurement.endNanoTime - prevMeasurement.startNanoTime;
                opsDone += prevMeasurement.cntr.get();
            }
        }

        return nanosPassed <= 0 ? 0 : opsDone * NANOS_IN_SECOND / nanosPassed;
    }

    /**
     * Check if measurement is outdated.
     *
     * @param interval Measurement to check. {@code null} is always outdated.
     * @param curNanoTime Current time in nanoseconds.
     * @return {@code true} if measurement is outdated.
     */
    private boolean isOutdated(@Nullable MeasurementInterval interval, long curNanoTime) {
        if (interval == null) {
            return true;
        }

        long elapsedNs = curNanoTime - interval.startNanoTime;

        if (elapsedNs <= 0) {
            return true; // Interval is started only now.
        }

        return (intervalSwitchNanos > 0) && elapsedNs > (maxMeasurements + 1) * intervalSwitchNanos;
    }

    /**
     * Gets or creates measurement interval, performs switch to new measurement by timeout.
     *
     * @param canInit If {@code true}, {@link #measurementIntervalAtomicRef} will always be not-null when this method is finished.
     *      If {@code false} and {@link #measurementIntervalAtomicRef} is null, this method will return null.
     * @param curNanoTime Current nano time.
     * @return Interval to use.
     */
    @Contract("true, _ -> !null")
    private @Nullable MeasurementInterval interval(boolean canInit, long curNanoTime) {
        MeasurementInterval interval;

        do {
            interval = measurementIntervalAtomicRef.get();
            if (interval == null) {
                if (!canInit) {
                    return null;
                }

                MeasurementInterval newInterval = new MeasurementInterval(curNanoTime);

                if (measurementIntervalAtomicRef.compareAndSet(null, newInterval)) {
                    interval = newInterval;
                } else {
                    continue;
                }
            }

            if (intervalSwitchNanos > 0 && (curNanoTime - interval.startNanoTime) > intervalSwitchNanos) {
                MeasurementInterval newInterval = new MeasurementInterval(curNanoTime);

                if (measurementIntervalAtomicRef.compareAndSet(interval, newInterval)) {
                    interval.endNanoTime = curNanoTime;

                    pushToHistory(interval);
                }
            }
        } while (interval == null);

        return interval;
    }

    /**
     * Adds measurement interval to the history.
     *
     * @param interval Finished interval to push to history.
     */
    private void pushToHistory(MeasurementInterval interval) {
        prevMeasurements.offer(interval);

        if (prevMeasurements.size() > maxMeasurements) {
            prevMeasurements.remove();
        }
    }

    /**
     * Set exact value for counter in current measurement interval, useful only for manually managed measurements. Does nothing if
     * checkpoint is not in progress according to current measurements.
     *
     * @param val New value to set.
     * @param curNanoTime Current nano time.
     * @see #measurementIntervalAtomicRef
     */
    void setCounter(long val, long curNanoTime) {
        MeasurementInterval interval = interval(false, curNanoTime);

        if (interval == null) {
            return;
        }

        interval.cntr.set(val);
    }

    /**
     * Set exact value for counter in current measurement interval, useful only for manually managed measurements. Unlike
     * {@link #setCounter(long, long)}, works even if checkpoint is not in progress. Could be used to trigger the measurement of checkpoint
     * that is currently starting.
     *
     * @param val New value to set.
     * @param curNanoTime Current nano time.
     */
    void forceCounter(long val, long curNanoTime) {
        interval(true, curNanoTime).cntr.set(val);
    }

    /**
     * Manually switch interval to empty (not started measurement).
     */
    void finishInterval() {
        while (true) {
            MeasurementInterval interval = measurementIntervalAtomicRef.get();

            if (interval == null) {
                return;
            }

            if (measurementIntervalAtomicRef.compareAndSet(interval, null)) {
                interval.endNanoTime = System.nanoTime();

                pushToHistory(interval);

                return;
            }
        }
    }

    /**
     * Gets average metric value previously reported by {@link #addMeasurementForAverageCalculation(long)}.
     *
     * @return Average metric value.
     */
    public long getAverage() {
        long time = System.nanoTime();

        return avgMeasurementWithHistorical(interval(false, time), time);
    }

    /**
     * Reduce measurements to calculate average value.
     *
     * @param interval Current measurement. If null only historical is used.
     * @param curNanoTime Current time nanoseconds.
     * @return Speed in page per second.
     */
    private long avgMeasurementWithHistorical(@Nullable MeasurementInterval interval, long curNanoTime) {
        long cnt = 0;
        long sum = 0;
        if (!isOutdated(interval, curNanoTime)) {
            cnt += interval.cntr.get();
            sum += interval.sum.get();
        }
        for (MeasurementInterval prevMeasurement : prevMeasurements) {
            if (!isOutdated(prevMeasurement, curNanoTime)) {
                cnt += prevMeasurement.cntr.get();
                sum += prevMeasurement.sum.get();
            }
        }

        return cnt <= 0 ? 0 : sum / cnt;
    }

    /**
     * Adds measurement to be used for average calculation. Calling this method will later calculate speed of
     * measurements come. Result can be taken from {@link #getAverage()}.
     *
     * @param val Value measured now, to be used for average calculation.
     */
    void addMeasurementForAverageCalculation(long val) {
        MeasurementInterval interval = interval(true, System.nanoTime());

        interval.cntr.incrementAndGet();
        interval.sum.addAndGet(val);
    }

    /**
     * Measurement interval, completed or open.
     */
    private static class MeasurementInterval {
        /** Counter of performed operations, pages. */
        private AtomicLong cntr = new AtomicLong();

        /** Sum of measured value, used only for average calculation. */
        private AtomicLong sum = new AtomicLong();

        /** Timestamp in nanoseconds of measurement start. */
        private final long startNanoTime;

        /** Timestamp in nanoseconds of measurement end. 0 for open (running) measurements.*/
        private volatile long endNanoTime;

        /**
         * Constructor.
         *
         * @param startNanoTime Timestamp of measurement start.
         */
        MeasurementInterval(long startNanoTime) {
            this.startNanoTime = startNanoTime;
        }
    }
}
