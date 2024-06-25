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

import static org.apache.ignite.compute.JobStatus.CANCELED;
import static org.apache.ignite.compute.JobStatus.CANCELING;
import static org.apache.ignite.compute.JobStatus.COMPLETED;
import static org.apache.ignite.compute.JobStatus.EXECUTING;
import static org.apache.ignite.compute.JobStatus.FAILED;
import static org.apache.ignite.compute.JobStatus.QUEUED;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.Cleaner;
import org.apache.ignite.internal.compute.JobStateImpl;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * In memory implementation of {@link ComputeStateMachine}.
 */
public class InMemoryComputeStateMachine implements ComputeStateMachine {
    private static final IgniteLogger LOG = Loggers.forClass(InMemoryComputeStateMachine.class);

    private final ComputeConfiguration configuration;

    private final String nodeName;

    private final Cleaner<JobState> cleaner = new Cleaner<>();

    private final Map<UUID, JobState> states = new ConcurrentHashMap<>();

    public InMemoryComputeStateMachine(ComputeConfiguration configuration, String nodeName) {
        this.configuration = configuration;
        this.nodeName = nodeName;
    }

    @Override
    public void start() {
        long ttlMillis = configuration.statesLifetimeMillis().value();
        cleaner.start(states::remove, ttlMillis, nodeName);
    }

    @Override
    public void stop() {
        cleaner.stop();
    }

    @Override
    public JobState currentState(UUID jobId) {
        return states.get(jobId);
    }

    @Override
    public UUID initJob() {
        UUID uuid = UUID.randomUUID();
        JobState state = JobStateImpl.builder()
                .id(uuid)
                .status(QUEUED)
                .createTime(Instant.now())
                .build();

        if (states.putIfAbsent(uuid, state) != null) {
            LOG.info("UUID collision detected! UUID: {}", uuid);
            return initJob();
        }

        return uuid;
    }

    @Override
    public void executeJob(UUID jobId) {
        changeJobStatus(jobId, EXECUTING);
    }

    @Override
    public void failJob(UUID jobId) {
        changeJobStatus(jobId, FAILED);
        cleaner.scheduleRemove(jobId);
    }

    @Override
    public void queueJob(UUID jobId) {
        changeJobStatus(jobId, QUEUED);
    }

    @Override
    public void completeJob(UUID jobId) {
        changeJobStatus(jobId, COMPLETED);
        cleaner.scheduleRemove(jobId);
    }

    @Override
    public void cancelingJob(UUID jobId) {
        changeJobStatus(jobId, currentStatus -> {
            if (currentStatus == QUEUED) {
                cleaner.scheduleRemove(jobId);
                return CANCELED;
            } else if (currentStatus == EXECUTING) {
                return CANCELING;
            }

            throw new IllegalJobStatusTransition(jobId, currentStatus, CANCELING);
        });
    }

    @Override
    public void cancelJob(UUID jobId) {
        changeJobStatus(jobId, CANCELED);
        cleaner.scheduleRemove(jobId);
    }

    private void changeJobStatus(UUID jobId, JobStatus newStatus) {
        changeJobStatus(jobId, ignored -> newStatus);
    }

    private void changeJobStatus(UUID jobId, Function<JobStatus, JobStatus> newStatusFunction) {
        changeStatus(jobId, currentState -> {
            JobStatus currentStatus = currentState.status();
            JobStatus newStatus = newStatusFunction.apply(currentStatus);

            validateStatusTransition(jobId, currentStatus, newStatus);

            JobStateImpl.Builder builder = JobStateImpl.toBuilder(currentState).status(newStatus);

            if (newStatus == EXECUTING) {
                builder.startTime(Instant.now());
            } else if (isFinal(newStatus)) {
                builder.finishTime(Instant.now());
            }

            return builder.build();
        });
    }

    private void changeStatus(UUID jobId, Function<JobState, JobState> newStateFunction) {
        if (states.computeIfPresent(jobId, (k, v) -> newStateFunction.apply(v)) == null) {
            throw new IllegalJobStatusTransition(jobId);
        }
    }

    /**
     * Returns {@code true} if the status is final.
     */
    private static boolean isFinal(JobStatus status) {
        return status == FAILED || status == COMPLETED || status == CANCELED;
    }

    /**
     * Validates the status transition.
     */
    private static void validateStatusTransition(UUID jobId, JobStatus current, JobStatus target) {
        if (!isValidStatusTransition(current, target)) {
            throw new IllegalJobStatusTransition(jobId, current, target);
        }
    }

    /**
     * Returns {@code true} if the transition is valid.
     */
    private static boolean isValidStatusTransition(JobStatus from, JobStatus toStatus) {
        switch (from) {
            case QUEUED:
                return toStatus == EXECUTING || toStatus == CANCELING || toStatus == CANCELED;
            case EXECUTING:
                return toStatus == FAILED || toStatus == COMPLETED || toStatus == CANCELING || toStatus == CANCELED || toStatus == QUEUED;
            case CANCELING:
                return toStatus == CANCELED || toStatus == FAILED || toStatus == COMPLETED;
            case FAILED:
            case COMPLETED:
            case CANCELED:
                return false;
            default:
                throw new IllegalStateException("Unknown job status: " + from);
        }
    }
}
