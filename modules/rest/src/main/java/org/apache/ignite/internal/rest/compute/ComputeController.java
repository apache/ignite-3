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

package org.apache.ignite.internal.rest.compute;

import static java.util.stream.Collectors.toList;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.compute.ComputeComponent;
import org.apache.ignite.internal.rest.api.compute.ComputeApi;
import org.apache.ignite.internal.rest.api.compute.JobState;
import org.apache.ignite.internal.rest.api.compute.JobStatus;
import org.apache.ignite.internal.rest.api.compute.OperationResult;
import org.apache.ignite.internal.rest.compute.exception.ComputeJobNotFoundException;

/**
 * REST controller for compute operations.
 */
@Controller
public class ComputeController implements ComputeApi {
    private final ComputeComponent computeComponent;

    public ComputeController(ComputeComponent computeComponent) {
        this.computeComponent = computeComponent;
    }

    @Override
    public CompletableFuture<Collection<JobStatus>> jobStatuses() {
        return computeComponent.statusesAsync()
                .thenApply(statuses -> statuses.stream().map(ComputeController::toJobStatus).collect(toList()));
    }

    @Override
    public CompletableFuture<JobStatus> jobStatus(UUID jobId) {
        return computeComponent.statusAsync(jobId)
                .thenApply(status -> {
                    if (status == null) {
                        throw new ComputeJobNotFoundException(jobId.toString());
                    } else {
                        return toJobStatus(status);
                    }
                });
    }

    @Override
    public CompletableFuture<OperationResult> updatePriority(UUID jobId, int priority) {
        return computeComponent.changePriorityAsync(jobId, priority)
                .thenApply(result -> {
                    if (result == null) {
                        throw new ComputeJobNotFoundException(jobId.toString());
                    } else {
                        return new OperationResult(result);
                    }
                });
    }

    @Override
    public CompletableFuture<OperationResult> cancelJob(UUID jobId) {
        return computeComponent.cancelAsync(jobId)
                .thenApply(result -> {
                    if (result == null) {
                        throw new ComputeJobNotFoundException(jobId.toString());
                    } else {
                        return new OperationResult(result);
                    }
                });
    }

    private static JobStatus toJobStatus(org.apache.ignite.compute.JobStatus jobStatus) {
        return new JobStatus(
                jobStatus.id(),
                JobState.valueOf(jobStatus.state().toString()),
                jobStatus.createTime(),
                jobStatus.startTime(),
                jobStatus.finishTime()
        );
    }
}
