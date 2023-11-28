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
import static org.apache.ignite.compute.JobState.SUBMITTED;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * In memory implementation of {@link ComputeStateMachine}.
 */
public class InMemoryComputeStateMachine implements ComputeStateMachine {
    private static final IgniteLogger LOG = Loggers.forClass(InMemoryComputeStateMachine.class);

    private final Map<UUID, JobState> states = new ConcurrentHashMap<>();

    @Override
    public JobState currentState(UUID jobId) {
        return states.get(jobId);
    }

    @Override
    public UUID initJob() {
        UUID uuid = UUID.randomUUID();
        JobState prevValue = states.putIfAbsent(uuid, SUBMITTED);
        if (prevValue != null) {
            LOG.info("UUID collision detected! UUID: {}", uuid);
            return initJob();
        }

        return uuid;
    }

    @Override
    public void queueJob(UUID jobId) {
        changeState(jobId, QUEUED, SUBMITTED);
    }

    @Override
    public void executeJob(UUID jobId) {
        changeState(jobId, EXECUTING, SUBMITTED, QUEUED);
    }

    @Override
    public void failJob(UUID jobId) {
        changeState(jobId, FAILED, EXECUTING, CANCELING);
    }

    @Override
    public void completeJob(UUID jobId) {
        changeState(jobId, COMPLETED, EXECUTING, CANCELING);
    }

    @Override
    public boolean cancelJob(UUID jobId) {
        AtomicBoolean result = new AtomicBoolean();
        Function<JobState, JobState> func = currentState -> {
            if (currentState == EXECUTING) {
                result.set(false);
                return CANCELING;
            } else if (currentState == CANCELING || currentState == QUEUED || currentState == SUBMITTED) {
                result.set(true);
                return CANCELED;
            }

            throw new IllegalJobStateTransition(jobId, currentState, CANCELED);
        };
        changeState(jobId, func);
        return result.get();
    }

    private void changeState(UUID jobId, JobState newState, JobState... requiredStates) {
        changeState(jobId, currentState -> {
            for (JobState requiredState : requiredStates) {
                if (currentState == requiredState) {
                    return newState;
                }
            }

            throw new IllegalJobStateTransition(jobId, currentState, newState);
        });
    }

    private void changeState(
            UUID jobId,
            Function<JobState, JobState> newStateFunction
    ) {
        JobState prevValue = states.computeIfPresent(jobId,
                (uuid, currentState) -> newStateFunction.apply(currentState)
        );

        if (prevValue == null) {
            throw new IllegalJobStateTransition(jobId);
        }
    }

}
