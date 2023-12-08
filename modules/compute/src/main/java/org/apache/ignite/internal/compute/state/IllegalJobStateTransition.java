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

import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_STATE_TRANSITION_ERR;

import java.util.UUID;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Thrown from Compute Jobs state machine {@link ComputeStateMachine} when job state transfer is illegal.
 */
public class IllegalJobStateTransition extends IgniteInternalException {
    public IllegalJobStateTransition(UUID jobId) {
        super(COMPUTE_JOB_STATE_TRANSITION_ERR, "Failed to transfer job state for nonexistent job " + jobId + ".");
    }

    public IllegalJobStateTransition(UUID jobId, JobState prevState, JobState newState) {
        super(COMPUTE_JOB_STATE_TRANSITION_ERR, message(jobId, prevState, newState));
    }

    private static String message(UUID jobId, JobState prevState, JobState newState) {
        return "Failed to transfer job " + jobId
                + " from state " + prevState
                + " to state " + newState;
    }
}
