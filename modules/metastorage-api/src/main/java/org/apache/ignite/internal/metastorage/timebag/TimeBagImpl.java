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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class TimeBagImpl implements TimeBag {
    /** Lock. */
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** Global stopwatch. */
    private final IgniteStopwatch globalStopwatch;

    /** Measurement unit. */
    private final TimeUnit measurementUnit;

    /** List of global stages (guarded by {@code lock}). */
    private final List<CompositeStage> stages;

    /** List of current local stages separated by threads (guarded by {@code lock}). */
    private Map<String, List<Stage>> localStages;

    /** Last seen global stage by thread. */
    private final ThreadLocal<CompositeStage> tlLastSeenStage = new ThreadLocal<>();

    /** Thread-local stopwatch. */
    private final ThreadLocal<IgniteStopwatch> tlStopwatch = ThreadLocal.withInitial(IgniteStopwatch::createUnstarted);

    /**
     * Creates a new {@link TimeBag} with specified measurement unit.
     *
     * @param started Flag indicating whether the time bag is started or not.
     * @param measurementUnit Measurement unit.
     */
    TimeBagImpl(boolean started, TimeUnit measurementUnit) {
        this.stages = new ArrayList<>();
        this.localStages = new ConcurrentHashMap<>();
        this.measurementUnit = measurementUnit;
        this.globalStopwatch = started ? IgniteStopwatch.createStarted() : IgniteStopwatch.createUnstarted();

        CompositeStage initStage = new CompositeStage("", 0, new HashMap<>(), measurementUnit);

        this.stages.add(initStage);

        tlLastSeenStage.set(initStage);
    }

    /**
     * Returns the last completed global stage.
     */
    private CompositeStage lastCompletedGlobalStage() {
        return stages.get(stages.size() - 1);
    }

    @Override
    public void start() {
        lock.writeLock().lock();

        try {
            globalStopwatch.start();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Finishes the global stage with given description and resets the local stages.
     *
     * @param description Description.
     */
    @Override
    public void finishGlobalStage(String description) {
        lock.writeLock().lock();

        try {
            stages.add(
                    new CompositeStage(
                            description,
                            globalStopwatch.elapsed(measurementUnit),
                            Collections.unmodifiableMap(localStages),
                            measurementUnit
                    )
            );

            localStages = new ConcurrentHashMap<>();

            globalStopwatch.reset().start();
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Finishes the local stage with given description.
     *
     * @param description Description.
     */
    @Override
    public void finishLocalStage(String description) {
        lock.readLock().lock();

        try {
            CompositeStage lastSeen = tlLastSeenStage.get();
            CompositeStage lastCompleted = lastCompletedGlobalStage();
            IgniteStopwatch localStopWatch = tlStopwatch.get();

            Stage stage;

            // We see this stage first time, get elapsed time from last completed global stage and start tracking local.
            if (lastSeen != lastCompleted) {
                stage = new Stage(description, globalStopwatch.elapsed(measurementUnit), measurementUnit);

                tlLastSeenStage.set(lastCompleted);
            } else {
                stage = new Stage(description, localStopWatch.elapsed(measurementUnit), measurementUnit);
            }

            localStopWatch.reset().start();

            // Associate local stage with current thread name.
            String threadName = Thread.currentThread().getName();

            localStages.computeIfAbsent(threadName, t -> new ArrayList<>()).add(stage);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns short name of desired measurement unit.
     *
     * @return Short name of desired measurement unit.
     */
    private static String measurementUnitShort(TimeUnit measurementUnit) {
        switch (measurementUnit) {
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case NANOSECONDS:
                return "ns";
            case MICROSECONDS:
                return "us";
            case HOURS:
                return "h";
            case MINUTES:
                return "min";
            case DAYS:
                return "days";
            default:
                return "";
        }
    }

    /**
     * Returns list of string representation of all stage timings.
     *
     * @return List of string representation of all stage timings.
     */
    @Override
    public List<String> stagesTimings() {
        lock.readLock().lock();

        try {
            List<String> timings = new ArrayList<>();

            long totalTime = 0;

            // Skip initial stage.
            for (int i = 1; i < stages.size(); i++) {
                CompositeStage stage = stages.get(i);

                totalTime += stage.time();

                timings.add(stage.toString());
            }

            // Add last stage with summary time of all global stages.
            timings.add(new Stage("Total time", totalTime, measurementUnit).toString());

            return timings;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Returns list of string representation of longest local stages per each composite stage.
     *
     * @param maxPerCompositeStage Max count of local stages to collect per composite stage.
     * @return List of string representation of longest local stages per each composite stage.
     */
    @Override
    public List<String> longestLocalStagesTimings(int maxPerCompositeStage) {
        lock.readLock().lock();

        try {
            List<String> timings = new ArrayList<>();

            for (int i = 1; i < stages.size(); i++) {
                CompositeStage stage = stages.get(i);

                if (!stage.localStages.isEmpty()) {
                    PriorityQueue<Stage> stagesByTime = new PriorityQueue<>();

                    for (Map.Entry<String, List<Stage>> threadAndStages : stage.localStages.entrySet()) {
                        stagesByTime.addAll(threadAndStages.getValue());
                    }

                    int stageCount = 0;
                    while (!stagesByTime.isEmpty() && stageCount < maxPerCompositeStage) {
                        stageCount++;

                        Stage locStage = stagesByTime.poll();

                        timings.add(locStage.toString() + " (parent=" + stage.description() + ")");
                    }
                }
            }

            return timings;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Stage.
     */
    private static class Stage implements Comparable<Stage> {
        /** Description. */
        private final String description;

        /** Time. */
        private final long time;

        /** Measurement unit. */
        private final TimeUnit measurementUnit;

        /**
         * Creates a new stage.
         *
         * @param description Description.
         * @param time Time.
         */
        private Stage(String description, long time, TimeUnit measurementUnit) {
            this.description = description;
            this.time = time;
            this.measurementUnit = measurementUnit;
        }

        /**
         * Returns description of the stage.
         *
         * @return Description of the stage.
         */
        String description() {
            return description;
        }

        /**
         * Returns time of the stage.
         *
         * @return Time of the stage.
         */
        long time() {
            return time;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("stage=")
                    .append('"')
                    .append(description()).append('"')
                    .append(' ')
                    .append('(')
                    .append(time()).append(' ')
                    .append(measurementUnitShort(measurementUnit))
                    .append(')');

            return sb.toString();
        }

        @Override
        public int compareTo(Stage o) {
            if (o.time < time) {
                return -1;
            }
            if (o.time > time) {
                return 1;
            }

            return o.description.compareTo(description);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Stage stage = (Stage) o;
            return time == stage.time && Objects.equals(description, stage.description) && measurementUnit == stage.measurementUnit;
        }

        @Override
        public int hashCode() {
            return Objects.hash(description, time, measurementUnit);
        }
    }

    /** Composite stage. */
    private static class CompositeStage extends Stage {
        /** Local stages. */
        private final Map<String, List<Stage>> localStages;

        /**
         * Create a new composite stage.
         *
         * @param description Description.
         * @param time Time.
         * @param localStages Local stages.
         */
        private CompositeStage(String description, long time, Map<String, List<Stage>> localStages, TimeUnit measurementUnit) {
            super(description, time, measurementUnit);

            this.localStages = localStages;
        }

        /**
         * Returns local stages associated with this composite stage.
         *
         * @return Local stages associated with this composite stage.
         */
        Map<String, List<Stage>> localStages() {
            return localStages;
        }
    }
}
