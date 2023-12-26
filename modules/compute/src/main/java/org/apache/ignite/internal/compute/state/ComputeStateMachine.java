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

import java.util.UUID;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.JobStatus;
import org.jetbrains.annotations.Nullable;

/**
 * State machine of Compute Jobs.
 */
public interface ComputeStateMachine {
    /**
     * Start Compute jobs state machine. The instance can't be used before it is started.
     */
    void start();

    /**
     * Stop Compute jobs state machine. The instance can't be used after it is stopped.
     */
    void stop();

    /**
     * Initialize Compute job in state machine. This job should have status {@link JobState#QUEUED}.
     *
     * @return Compute job identifier.
     */
    UUID initJob();

    /**
     * Tries to transfer Compute Job to complete state.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to complete state.
     */
    void completeJob(UUID jobId);

    /**
     * Tries to transfer Compute Job to execute state.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to execute state.
     */
    void executeJob(UUID jobId);

    /**
     * Tries to transfer Compute Job to canceling state, it means that execution may continue.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to canceling state.
     */
    void cancelingJob(UUID jobId);

    /**
     * Tries to transfer Compute Job to cancel state, it means that execution canceled.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to canceled state.
     */
    void cancelJob(UUID jobId);

    /**
     * Tries to transfer Compute Job to fail state.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to failed state.
     */
    void failJob(UUID jobId);

    /**
     * Tries to transfer Compute Job to queued state from the {@link JobState#EXECUTING} state, used for retrying.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to failed state.
     */
    void queueJob(UUID jobId);

    /**
     * Returns current status of Compute Job.
     *
     * @param jobId Compute job identifier.
     * @return Current status of Compute Job or {@code null} in case if job with provided identifier doesn't exist.
     */
    @Nullable
    JobStatus currentStatus(UUID jobId);
}
