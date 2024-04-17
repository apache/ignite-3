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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.version.Version;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.compute.message.JobCancelResponse;
import org.apache.ignite.internal.compute.message.JobChangePriorityResponse;
import org.apache.ignite.internal.compute.message.JobResultResponse;
import org.apache.ignite.internal.compute.message.JobStatusResponse;
import org.apache.ignite.internal.compute.message.JobStatusesResponse;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for compute.
 */
public class ComputeUtils {
    private static final ComputeMessagesFactory MESSAGES_FACTORY = new ComputeMessagesFactory();

    /**
     * Instantiate compute job via provided class loader by provided job class.
     *
     * @param computeJobClass Compute job class.
     * @param <R> Compute job return type.
     * @return Compute job instance.
     */
    public static <R> ComputeJob<R> instantiateJob(Class<? extends ComputeJob<R>> computeJobClass) {
        if (!(ComputeJob.class.isAssignableFrom(computeJobClass))) {
            throw new ComputeException(
                    CLASS_INITIALIZATION_ERR,
                    "'" + computeJobClass.getName() + "' does not implement ComputeJob interface"
            );
        }

        try {
            Constructor<? extends ComputeJob<R>> constructor = computeJobClass.getDeclaredConstructor();

            if (!constructor.canAccess(null)) {
                constructor.setAccessible(true);
            }

            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ComputeException(CLASS_INITIALIZATION_ERR, "Cannot instantiate job", e);
        }
    }

    /**
     * Resolve compute job class name to compute job class reference.
     *
     * @param jobClassLoader Class loader.
     * @param jobClassName Job class name.
     * @param <R> Compute job return type.
     * @return Compute job class.
     */
    public static <R> Class<ComputeJob<R>> jobClass(ClassLoader jobClassLoader, String jobClassName) {
        try {
            return (Class<ComputeJob<R>>) Class.forName(jobClassName, true, jobClassLoader);
        } catch (ClassNotFoundException e) {
            throw new ComputeException(CLASS_INITIALIZATION_ERR, "Cannot load job class by name '" + jobClassName + "'", e);
        }
    }

    /**
     * Instantiate map reduce task via provided class loader by provided task class.
     *
     * @param taskClass Map reduce task class.
     * @param <R> Map reduce task return type.
     * @return Map reduce task instance.
     */
    public static <R> MapReduceTask<R> instantiateTask(Class<? extends MapReduceTask<R>> taskClass) {
        if (!(MapReduceTask.class.isAssignableFrom(taskClass))) {
            throw new ComputeException(
                    CLASS_INITIALIZATION_ERR,
                    "'" + taskClass.getName() + "' does not implement ComputeTask interface"
            );
        }

        try {
            Constructor<? extends MapReduceTask<R>> constructor = taskClass.getDeclaredConstructor();

            if (!constructor.canAccess(null)) {
                constructor.setAccessible(true);
            }

            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ComputeException(CLASS_INITIALIZATION_ERR, "Cannot instantiate task", e);
        }
    }

    /**
     * Resolve map reduce task class name to map reduce task class reference.
     *
     * @param taskClassLoader Class loader.
     * @param taskClassName Map reduce task class name.
     * @param <R> Map reduce task return type.
     * @return Map reduce task class.
     */
    public static <R> Class<MapReduceTask<R>> taskClass(ClassLoader taskClassLoader, String taskClassName) {
        try {
            return (Class<MapReduceTask<R>>) Class.forName(taskClassName, true, taskClassLoader);
        } catch (ClassNotFoundException e) {
            throw new ComputeException(CLASS_INITIALIZATION_ERR, "Cannot load task class by name '" + taskClassName + "'", e);
        }
    }

    /**
     * Transform deployment unit object to message {@link DeploymentUnitMsg}.
     *
     * @param unit Deployment unit.
     * @return Deployment unit message.
     */
    public static DeploymentUnitMsg toDeploymentUnitMsg(DeploymentUnit unit) {
        return MESSAGES_FACTORY.deploymentUnitMsg()
                .name(unit.name())
                .version(unit.version().toString())
                .build();
    }

    /**
     * Extract compute job id from execute response.
     *
     * @param executeResponse Execution message response.
     * @return Completable future with result.
     */
    public static CompletableFuture<UUID> jobIdFromExecuteResponse(ExecuteResponse executeResponse) {
        Throwable throwable = executeResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture(executeResponse.jobId());
    }

    /**
     * Extract Compute job result from execute response.
     *
     * @param jobResultResponse Job execution result message response.
     * @param <R> Compute job return type.
     * @return Completable future with result.
     */
    public static <R> CompletableFuture<R> resultFromJobResultResponse(JobResultResponse jobResultResponse) {
        Throwable throwable = jobResultResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture((R) jobResultResponse.result());
    }

    /**
     * Extract compute job statuses from statuses response.
     *
     * @param jobStatusesResponse Job statuses result message response.
     * @return Completable future with result.
     */
    public static CompletableFuture<Collection<JobStatus>> statusesFromJobStatusesResponse(JobStatusesResponse jobStatusesResponse) {
        Throwable throwable = jobStatusesResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture(jobStatusesResponse.statuses());
    }

    /**
     * Extract compute job status from status response.
     *
     * @param jobStatusResponse Job status result message response.
     * @return Completable future with result.
     */
    public static CompletableFuture<@Nullable JobStatus> statusFromJobStatusResponse(JobStatusResponse jobStatusResponse) {
        Throwable throwable = jobStatusResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture(jobStatusResponse.status());
    }

    /**
     * Extract compute job cancel result from cancel response.
     *
     * @param jobCancelResponse Job cancel message response.
     * @return Completable future with result.
     */
    public static CompletableFuture<@Nullable Boolean> cancelFromJobCancelResponse(JobCancelResponse jobCancelResponse) {
        Throwable throwable = jobCancelResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture(jobCancelResponse.result());
    }

    /**
     * Extract compute job change priority result from change priority response.
     *
     * @param jobChangePriorityResponse Job change priority message response.
     * @return Completable future with result.
     */
    public static CompletableFuture<@Nullable Boolean> changePriorityFromJobChangePriorityResponse(
            JobChangePriorityResponse jobChangePriorityResponse
    ) {
        Throwable throwable = jobChangePriorityResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture(jobChangePriorityResponse.result());
    }

    /**
     * Transform list of deployment unit messages to list of deployment units.
     *
     * @param unitMsgs Deployment units messages.
     * @return Deployment units.
     */
    public static List<DeploymentUnit> toDeploymentUnit(List<DeploymentUnitMsg> unitMsgs) {
        return unitMsgs.stream()
                .map(it -> new DeploymentUnit(it.name(), Version.parseVersion(it.version())))
                .collect(Collectors.toList());
    }

    /**
     * Returns a new CompletableFuture that, when the given {@code origin} future completes exceptionally, maps the origin's exception to a
     * public Compute exception if it is needed.
     *
     * @param origin The future to use to create a new stage.
     * @param <R> Type os result.
     * @return New CompletableFuture.
     */
    public static <R> CompletableFuture<R> convertToComputeFuture(CompletableFuture<R> origin) {
        return origin.handle((res, err) -> {
            if (err != null) {
                throw new CompletionException(mapToComputeException(unwrapCause(err)));
            }

            return res;
        });
    }

    private static Throwable mapToComputeException(Throwable origin) {
        if (origin instanceof IgniteException || origin instanceof IgniteCheckedException) {
            return origin;
        } else {
            return new ComputeException(COMPUTE_JOB_FAILED_ERR, "Job execution failed: " + origin, origin);
        }
    }
}
