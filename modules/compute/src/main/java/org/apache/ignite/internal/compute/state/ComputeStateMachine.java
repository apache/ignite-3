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

/**
 * State machine of Compute Jobs.
 */
public interface ComputeStateMachine {
    /**
     * Initialize Compute job in state machine. This job should have status {@link JobState#QUEUED}.
     *
     * @return Compute job identifier.
     */
    UUID initJob();

    /**
     * Try to transfer Compute Job to complete state.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to complete state.
     */
    void completeJob(UUID jobId);

    /**
     * Try to transfer Compute Job to execute state.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to complete state.
     */
    void executeJob(UUID jobId);

    /**
     * Try to transfer Compute Job to canceling state, it means that
     *
     * @param jobId Compute job identifier.
     * @return {@code true} in case when cancel finished fully.
     *     {@code false} in case when cancel not finished and job execution can't finished immediately.
     * @throws IllegalJobStateTransition in case when job can't be transferred to complete state.
     */
    void cancelingJob(UUID jobId);

    /**
     *
     * @param jobId
     */
    void cancelJob(UUID jobId);

    /**
     * Try to transfer Compute Job to fail state.
     *
     * @param jobId Compute job identifier.
     * @throws IllegalJobStateTransition in case when job can't be transferred to failed state.
     */
    void failJob(UUID jobId);

    /**
     * Returns current state of Compute Job.
     *
     * @param jobId Compute job identifier.
     * @return Current state of Compute Job or {@code null} in case if job with provided identifier doesn't exist.
     */
    JobState currentState(UUID jobId);
}
