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

package org.apache.ignite.internal.compute.state;

import static org.apache.ignite.compute.JobState.CANCELED;
import static org.apache.ignite.compute.JobState.CANCELING;
import static org.apache.ignite.compute.JobState.COMPLETED;
import static org.apache.ignite.compute.JobState.EXECUTING;
import static org.apache.ignite.compute.JobState.FAILED;
import static org.apache.ignite.compute.JobState.QUEUED;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.NamedThreadFactory;

/**
 * In memory implementation of {@link ComputeStateMachine}.
 */
public class InMemoryComputeStateMachine implements ComputeStateMachine {
    private static final IgniteLogger LOG = Loggers.forClass(InMemoryComputeStateMachine.class);

    private final ComputeConfiguration configuration;

    private ExecutorService cleaner;

    private final Set<UUID> toRemove = new HashSet<>();

    private final Set<UUID> waitToRemove = ConcurrentHashMap.newKeySet();

    private final Map<UUID, JobStatus> statuses = new ConcurrentHashMap<>();

    public InMemoryComputeStateMachine(ComputeConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void start() {
        Long lifetime = configuration.statesLifetimeMillis().value();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                new NamedThreadFactory("InMemoryComputeStateMachine-pool", LOG)
        );
        executor.scheduleAtFixedRate(() -> {
            Set<UUID> nextToRemove = Set.of(waitToRemove.toArray(UUID[]::new));
            this.waitToRemove.removeAll(nextToRemove);

            for (UUID jobId : toRemove) {
                statuses.remove(jobId);
            }
            toRemove.clear();
            toRemove.addAll(nextToRemove);
        }, lifetime, lifetime, TimeUnit.MILLISECONDS);
        cleaner = executor;
    }

    @Override
    public void stop() {
        shutdownAndAwaitTermination(cleaner, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public JobStatus currentStatus(UUID jobId) {
        return statuses.get(jobId);
    }

    @Override
    public UUID initJob() {
        UUID uuid = UUID.randomUUID();
        JobStatus status = JobStatus.builder()
                .id(uuid)
                .state(QUEUED)
                .createTime(Instant.now())
                .build();

        if (statuses.putIfAbsent(uuid, status) != null) {
            LOG.info("UUID collision detected! UUID: {}", uuid);
            return initJob();
        }

        return uuid;
    }

    @Override
    public void executeJob(UUID jobId) {
        changeJobState(jobId, EXECUTING);
    }

    @Override
    public void failJob(UUID jobId) {
        changeJobState(jobId, FAILED);
        waitToRemove.add(jobId);
    }

    @Override
    public void completeJob(UUID jobId) {
        changeJobState(jobId, COMPLETED);
        waitToRemove.add(jobId);
    }

    @Override
    public void cancelingJob(UUID jobId) {
        changeJobState(jobId, currentState -> {
            if (currentState == QUEUED) {
                waitToRemove.add(jobId);
                return CANCELED;
            } else if (currentState == EXECUTING) {
                return CANCELING;
            }

            throw new IllegalJobStateTransition(jobId, currentState, CANCELING);
        });
    }

    @Override
    public void cancelJob(UUID jobId) {
        changeJobState(jobId, CANCELED);
        waitToRemove.add(jobId);
    }

    private void changeJobState(UUID jobId, JobState newState) {
        changeJobState(jobId, ignored -> newState);
    }

    private void changeJobState(UUID jobId, Function<JobState, JobState> newStateFunction) {
        changeState(jobId, currentStatus -> {
            JobState currentState = currentStatus.state();
            JobState newState = newStateFunction.apply(currentState);

            validateStateTransition(jobId, currentState, newState);

            JobStatus.Builder builder = currentStatus.toBuilder().state(newState);

            if (newState == EXECUTING) {
                builder.startTime(Instant.now());
            } else if (isFinal(newState)) {
                builder.finishTime(Instant.now());
            }

            return builder.build();
        });
    }

    private void changeState(UUID jobId, Function<JobStatus, JobStatus> newStateFunction) {
        if (statuses.computeIfPresent(jobId, (k, v) -> newStateFunction.apply(v)) == null) {
            throw new IllegalJobStateTransition(jobId);
        }
    }

    /**
     * Returns {@code true} if the state is final.
     */
    private static boolean isFinal(JobState state) {
        return state == FAILED || state == COMPLETED || state == CANCELED;
    }

    /**
     * Validates the state transition.
     */
    private static void validateStateTransition(UUID jobId, JobState current, JobState target) {
        if (!isValidStateTransition(current, target)) {
            throw new IllegalJobStateTransition(jobId, current, target);
        }
    }

    /**
     * Returns {@code true} if the transition is valid.
     */
    private static boolean isValidStateTransition(JobState from, JobState toState) {
        switch (from) {
            case QUEUED:
                return toState == EXECUTING || toState == CANCELING || toState == CANCELED;
            case EXECUTING:
                return toState == FAILED || toState == COMPLETED || toState == CANCELING || toState == CANCELED;
            case CANCELING:
                return toState == CANCELED || toState == FAILED || toState == COMPLETED;
            case FAILED:
            case COMPLETED:
            case CANCELED:
                return false;
            default:
                throw new IllegalStateException("Unknown job state: " + from);
        }
    }
}
