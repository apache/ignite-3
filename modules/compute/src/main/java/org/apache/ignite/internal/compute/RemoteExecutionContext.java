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

package org.apache.ignite.internal.compute;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecution;

/**
 * Captures the context of a remote job execution. Also provides methods to access the job execution object
 * that is returned to the user. The access is thread safe.
 *
 * @param <R> type of the result of the job.
 */
class RemoteExecutionContext<T, R> {

    private final ExecutionOptions executionOptions;

    private final List<DeploymentUnit> units;

    private final String jobClassName;

    private final byte[] args;

    private final AtomicReference<FailSafeJobExecution<R>> jobExecution;

    RemoteExecutionContext(List<DeploymentUnit> units, String jobClassName, ExecutionOptions executionOptions, byte[] args) {
        this.executionOptions = executionOptions;
        this.units = units;
        this.jobClassName = jobClassName;
        this.args = args;
        this.jobExecution = new AtomicReference<>(null);
    }

    /**
     * Initializes the job execution object that is supposed to be returned to the client. This method can be called only once.
     *
     * @param jobExecution the instance of job execution that should be returned to the client.
     */
    void initJobExecution(FailSafeJobExecution<R> jobExecution) {
        if (!this.jobExecution.compareAndSet(null, jobExecution)) {
            throw new IllegalStateException("Job execution is already initialized.");
        }
    }

    /**
     * Getter to the job execution object that is supposed to be returned to the client.
     *
     * @return fail-safe job execution object.
     */
    FailSafeJobExecution<R> failSafeJobExecution() {
        FailSafeJobExecution<R> jobExecution = this.jobExecution.get();
        if (jobExecution == null) {
            throw new IllegalStateException("Job execution is not initialized. Call initJobExecution() first.");
        }

        return jobExecution;
    }


    /**
     * Updates the state of the job execution object but does not change the link to the object.
     * The context holds exactly one link that is returned to the user and mutates its internal state only.
     *
     * @param jobExecution the new job execution object (supposed to be a restarted job but in another worker node).
     */
    void updateJobExecution(JobExecution<R> jobExecution) {
        failSafeJobExecution().updateJobExecution(jobExecution);
    }

    ExecutionOptions executionOptions() {
        return executionOptions;
    }

    List<DeploymentUnit> units() {
        return units;
    }

    String jobClassName() {
        return jobClassName;
    }

    byte[] args() {
        return args;
    }
}
