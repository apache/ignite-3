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

package org.apache.ignite.internal.metastorage.timebag;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;

/**
 * An object that measures elapsed time in nanoseconds. It is useful to measure elapsed time using
 * this class instead of direct calls to {@link System#nanoTime} for a few reasons:
 *
 * <ul>
 *   <li>An alternate time source can be substituted, for testing or performance reasons.
 *   <li>As documented by {@code nanoTime}, the value returned has no absolute meaning, and can only
 *       be interpreted as relative to another timestamp returned by {@code nanoTime} at a different
 *       time. {@code Stopwatch} is a more effective abstraction because it exposes only these
 *       relative values, not the absolute ones.
 * </ul>
 *
 * <p>Basic usage:
 *
 * <pre>{@code
 * Stopwatch stopwatch = Stopwatch.createStarted();
 * doSomething();
 * stopwatch.stop(); // optional
 *
 * Duration duration = stopwatch.elapsed();
 *
 * log.info("time: " + stopwatch); // formatted string like "12.3 ms"
 * }</pre>
 *
 * <p>Stopwatch methods are not idempotent; it is an error to start or stop a stopwatch that is
 * already in the desired state.
 *
 * <p>When testing code that uses this class, use {@link #createUnstarted(IgniteTicker)} or {@link
 * #createStarted(IgniteTicker)} to supply a fake or mock ticker. This allows you to simulate any valid
 * behavior of the stopwatch.
 *
 * <p><b>Note:</b> This class is not thread-safe.
 */
@SuppressWarnings("GoodTime") // lots of violations
final class IgniteStopwatch {
    /** Ticker. */
    private final IgniteTicker ticker;

    /** Is running. */
    private boolean isRunning;

    /** Elapsed nanos. */
    private long elapsedNanos;

    /** Start tick. */
    private long startTick;

    /**
     * Creates (but does not start) a new stopwatch using {@link System#nanoTime} as its time source.
     */
    static IgniteStopwatch createUnstarted() {
        return new IgniteStopwatch();
    }

    /**
     * Creates (and starts) a new stopwatch using {@link System#nanoTime} as its time source.
     */
    static IgniteStopwatch createStarted() {
        return new IgniteStopwatch().start();
    }

    /**
     * Creates (and starts) a new stopwatch, using the specified time source.
     */
    static IgniteStopwatch createStarted(IgniteTicker ticker) {
        return new IgniteStopwatch(ticker).start();
    }

    /**
     * Creates a new stopwatch using a ticker based on {@link System#nanoTime}.
     */
    IgniteStopwatch() {
        this.ticker = IgniteTicker.systemTicker();
    }

    /**
     * Creates a new stopwatch with the specified ticker.
     *
     * @param ticker Ticker.
     */
    IgniteStopwatch(IgniteTicker ticker) {
        this.ticker = ticker;
    }

    /**
     * Returns {@code true} if {@link #start()} has been called on this stopwatch, and {@link #stop()}
     * has not been called since the last call to {@code start()}.
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * Starts the stopwatch.
     *
     * @return this {@code Stopwatch} instance
     * @throws IllegalStateException if the stopwatch is already running.
     */
    public IgniteStopwatch start() {
        assert !isRunning : "This stopwatch is already running.";

        isRunning = true;

        startTick = ticker.read();

        return this;
    }

    /**
     * Stops the stopwatch. Future reads will return the fixed duration that had elapsed up to this
     * point.
     *
     * @return this {@code Stopwatch} instance
     * @throws IllegalStateException if the stopwatch is already stopped.
     */
    public IgniteStopwatch stop() {
        long tick = ticker.read();

        assert isRunning : "This stopwatch is already stopped.";

        isRunning = false;
        elapsedNanos += tick - startTick;
        return this;
    }

    /**
     * Sets the elapsed time for this stopwatch to zero, and places it in a stopped state.
     *
     * @return this {@code Stopwatch} instance
     */
    public IgniteStopwatch reset() {
        elapsedNanos = 0;

        isRunning = false;

        return this;
    }

    /**
     * Returns the current elapsed time shown on this stopwatch.
     *
     * @return the elapsed time.
     */
    private long elapsedNanos() {
        return isRunning ? ticker.read() - startTick + elapsedNanos : elapsedNanos;
    }

    /**
     * Returns the current elapsed time shown on this stopwatch, expressed in the desired time unit,
     * with any fraction rounded down.
     *
     * <p><b>Note:</b> the overhead of measurement can be more than a microsecond, so it is generally
     * not useful to specify {@link TimeUnit#NANOSECONDS} precision here.
     *
     * <p>It is generally not a good idea to use an ambiguous, unitless {@code long} to represent
     * elapsed time. Therefore, we recommend using {@link #elapsed()} instead, which returns a
     * strongly-typed {@link Duration} instance.
     *
     * @return the elapsed time.
     */
    long elapsed(TimeUnit desiredUnit) {
        return desiredUnit.convert(elapsedNanos(), NANOSECONDS);
    }

}
