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

package org.apache.ignite.internal.tracing;

import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import jdk.jfr.Event;
import jdk.jfr.Label;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.NotNull;

/**
 * Instrumentation class.
 */
public class Instrumentation {
    private static final IgniteLogger LOG = Loggers.forClass(Instrumentation.class);

    private static volatile boolean firstRun = true;

    private static volatile boolean jfr = false;

    private static final Path file = Path.of("/opt/pubagent/poc/log/stats/dstat/trace.txt");

    private static volatile ConcurrentLinkedQueue<Measurement> measurements = new ConcurrentLinkedQueue<>();

    private static volatile long startTime = System.nanoTime();

    private static volatile boolean isStarted = false;

    private static final AtomicInteger opCnt = new AtomicInteger();

    private static final int rate = 1000;


    /**
     * Starts instrumentation.
     *
     * @param jfr True to write in jfr, false in file.
     */
    public static void start(boolean jfr) {
        if (isStarted) {
            return;
        }

        Instrumentation.jfr = jfr;
        if (firstRun && !jfr) {
            try {
                Files.deleteIfExists(file);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            firstRun = false;

            LOG.info("Instrumentation was started.");
        }

        if (opCnt.incrementAndGet() % rate == 0) {
            startTime = System.nanoTime();
            isStarted = true;
        }
    }

    /**
     * Ends instrumentation.
     */
    public static void end() {
        if (!isStarted) {
            return;
        }

        isStarted = false;

        if (jfr) {
            endJfr();
        } else {
            endPrintln();
        }
    }

    private static void endPrintln() {
        var endTime = System.nanoTime();
        List<Measurement> m = new ArrayList<>(measurements);
        measurements.clear();
        m.sort(Measurement::compareTo);
        AtomicLong found = new AtomicLong(0L);
        var sb = new StringBuilder();
        AtomicLong previousEnd = new AtomicLong(0L);
        m.forEach(measurement -> {
            found.addAndGet(measurement.end - measurement.start);
            if (previousEnd.get() != 0) {
                var f = measurement.start - previousEnd.get();
                if (f >= 0) {
                    sb.append("    Here is hidden " + f / 1000.0 + " us\n");
                } else {
                    sb.append("    Here is hidden n/a \n");
                }
            }
            previousEnd.set(measurement.end);
            sb.append(measurement.message + " " + (measurement.end - measurement.start) / 1000.0 + " " + measurement.start % 10000000 + " "
                    + measurement.end % 10000000 + "\n");
        });
        double wholeTime = (endTime - startTime);
        sb.append("Whole time: " + (endTime - startTime) / 1000.0 + " us" + "\n");
        sb.append(
                "Found time: " + found + " Not found time: " + (wholeTime - found.get()) + " Percentage of found: "
                        + 100 * found.get() / wholeTime + "\n\n");
        try {
            Files.write(file, sb.toString().getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void endJfr() {
        List<Measurement> m = new ArrayList<>(measurements);
        m.forEach(Event::commit);
        measurements.clear();
    }

    public static void add(Measurement measurement) {
        if (!isStarted) {
            return;
        }

        measurements.add(measurement);
    }

    public static void mark(String message) {
        if (!isStarted) {
            return;
        }

        var measure = new Measurement(message);
        measure.start();
        measure.stop();
        measurements.add(measure);
    }


    public static <T> T measure(Action<T> block, String message) {
        if (!isStarted) {
            try {
                return block.action();
            } catch (Exception e) {
                sneakyThrow(e);
            }
        }

        var measure = new Measurement(message);
        measure.start();
        T results = null;
        try {
            results = block.action();
        } catch (Exception e) {
            sneakyThrow(e);
        } finally {
            measure.stop();
        }

        measurements.add(measure);

        return results;
    }

    public static void measure(VoidAction block, String message) {
        if (!isStarted) {
            try {
                block.action();

                return;
            } catch (Exception e) {
                sneakyThrow(e);
            }
        }

        var measure = new Measurement(message);
        measure.start();
        try {
            block.action();
        } catch (Exception e) {
            sneakyThrow(e);
        } finally {
            measure.stop();
        }

        measurements.add(measure);
    }

    public static <T> CompletableFuture<T> measure(Supplier<CompletableFuture<T>> future, String message) {
        if (!isStarted) {
            return future.get();
        }

        var measure = new Measurement(message);
        measure.start();
        return Optional.ofNullable(future.get()).orElse(CompletableFuture.completedFuture(null)).thenApply(v -> {
            measure.stop();
            measurements.add(measure);
            return v;
        });
    }

    public interface Action<T> {
        T action() throws Exception;
    }

    public interface VoidAction {
        void action() throws Exception;
    }

    public static class Measurement extends Event implements Comparable<Measurement> {
        private long start;
        private long end;

        @Label("Message")
        private String message;

        public Measurement(String message) {
            this.message = message;
        }

        public void start() {
            begin();

            start = System.nanoTime();
        }

        public void stop() {
            end();

            end = System.nanoTime();
        }

        @Override
        public int compareTo(@NotNull Instrumentation.Measurement o) {
            return Long.compare(start, o.start);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Measurement that = (Measurement) o;
            return start == that.start && end == that.end && message.equals(that.message);
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end, message);
        }
    }
}
