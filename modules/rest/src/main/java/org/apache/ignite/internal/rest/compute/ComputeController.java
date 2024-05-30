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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import io.micronaut.http.annotation.Controller;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.compute.IgniteComputeInternal;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.rest.api.compute.ComputeApi;
import org.apache.ignite.internal.rest.api.compute.JobState;
import org.apache.ignite.internal.rest.api.compute.JobStatus;
import org.apache.ignite.internal.rest.api.compute.UpdateJobPriorityBody;
import org.apache.ignite.internal.rest.compute.exception.ComputeJobNotFoundException;
import org.apache.ignite.internal.rest.compute.exception.ComputeJobStateException;
import org.jetbrains.annotations.Nullable;

/**
 * REST controller for compute operations.
 */
@Controller
public class ComputeController implements ComputeApi, ResourceHolder {
    private IgniteComputeInternal compute;

    public ComputeController(IgniteComputeInternal compute) {
        this.compute = compute;
    }

    @Override
    public CompletableFuture<Collection<JobStatus>> jobStatuses() {
        return compute.statusesAsync()
                .thenApply(statuses -> statuses.stream().map(ComputeController::toJobStatus).collect(toList()));
    }

    @Override
    public CompletableFuture<JobStatus> jobStatus(UUID jobId) {
        return jobStatus0(jobId);
    }

    @Override
    public CompletableFuture<Void> updatePriority(UUID jobId, UpdateJobPriorityBody updateJobPriorityBody) {
        return compute.changePriorityAsync(jobId, updateJobPriorityBody.priority())
                .thenCompose(result -> handleOperationResult(jobId, result));
    }

    @Override
    public CompletableFuture<Void> cancelJob(UUID jobId) {
        return compute.cancelAsync(jobId)
                .thenCompose(result -> handleOperationResult(jobId, result));
    }

    private CompletableFuture<Void> handleOperationResult(UUID jobId, @Nullable Boolean result) {
        if (result == null) {
            return failedFuture(new ComputeJobNotFoundException(jobId.toString()));
        } else if (!result) {
            return jobStatus0(jobId).thenCompose(status -> failedFuture(new ComputeJobStateException(jobId.toString(), status.state())));
        } else {
            return nullCompletedFuture();
        }
    }

    private CompletableFuture<JobStatus> jobStatus0(UUID jobId) {
        return compute.statusAsync(jobId)
                .thenApply(status -> {
                    if (status == null) {
                        throw new ComputeJobNotFoundException(jobId.toString());
                    } else {
                        return toJobStatus(status);
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

    @Override
    public void cleanResources() {
        compute = null;
    }
}
