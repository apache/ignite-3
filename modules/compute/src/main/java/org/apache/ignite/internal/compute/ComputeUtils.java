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
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_CANCELLED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.MARSHALLING_TYPE_MISMATCH_ERR;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.deployment.version.Version;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.compute.message.JobCancelResponse;
import org.apache.ignite.internal.compute.message.JobChangePriorityResponse;
import org.apache.ignite.internal.compute.message.JobResultResponse;
import org.apache.ignite.internal.compute.message.JobStateResponse;
import org.apache.ignite.internal.compute.message.JobStatesResponse;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.Tuple;
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
    public static <T, R> ComputeJob<T, R> instantiateJob(Class<? extends ComputeJob<T, R>> computeJobClass) {
        if (!(ComputeJob.class.isAssignableFrom(computeJobClass))) {
            throw new ComputeException(
                    CLASS_INITIALIZATION_ERR,
                    "'" + computeJobClass.getName() + "' does not implement ComputeJob interface"
            );
        }

        try {
            Constructor<? extends ComputeJob<T, R>> constructor = computeJobClass.getDeclaredConstructor();

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
    public static <T, R> Class<ComputeJob<T, R>> jobClass(JobClassLoader jobClassLoader, String jobClassName) {
        try {
            return (Class<ComputeJob<T, R>>) Class.forName(jobClassName, true, jobClassLoader.classLoader());
        } catch (ClassNotFoundException e) {
            String message = "Cannot load job class by name '" + jobClassName + "'";
            if (jobClassLoader.units().isEmpty()) {
                throw new ComputeException(CLASS_INITIALIZATION_ERR, message + ". Deployment units list is empty.", e);
            }
            throw new ComputeException(CLASS_INITIALIZATION_ERR, message, e);
        }
    }

    /**
     * Instantiate map reduce task via provided class loader by provided task class.
     *
     * @param taskClass Map reduce task class.
     * @param <R> Map reduce task return type.
     * @return Map reduce task instance.
     */
    public static <I, M, T, R> MapReduceTask<I, M, T, R> instantiateTask(Class<? extends MapReduceTask<I, M, T, R>> taskClass) {
        if (!(MapReduceTask.class.isAssignableFrom(taskClass))) {
            throw new ComputeException(
                    CLASS_INITIALIZATION_ERR,
                    "'" + taskClass.getName() + "' does not implement ComputeTask interface"
            );
        }

        try {
            Constructor<? extends MapReduceTask<I, M, T, R>> constructor = taskClass.getDeclaredConstructor();

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
     * @param <R> Map reduce task return type.
     * @param taskClassLoader Class loader.
     * @param taskClassName Map reduce task class name.
     * @return Map reduce task class.
     */
    public static <I, M, T, R> Class<MapReduceTask<I, M, T, R>> taskClass(JobClassLoader taskClassLoader, String taskClassName) {
        try {
            return (Class<MapReduceTask<I, M, T, R>>) Class.forName(taskClassName, true, taskClassLoader.classLoader());
        } catch (ClassNotFoundException e) {
            String message = "Cannot load task class by name '" + taskClassName + "'.";
            if (taskClassLoader.units().isEmpty()) {
                throw new ComputeException(CLASS_INITIALIZATION_ERR, message + " Deployment units list is empty.", e);
            }
            throw new ComputeException(CLASS_INITIALIZATION_ERR, message, e);
        }
    }

    /**
     * Instantiate data streamer receiver.
     *
     * @param recvClass Receiver class.
     * @param <T> Receiver item type.
     * @param <R> Receiver return type.
     * @return Receiver instance.
     */
    public static <T, R, A> DataStreamerReceiver<T, R, A> instantiateReceiver(Class<? extends DataStreamerReceiver<T, R, A>> recvClass) {
        if (!(DataStreamerReceiver.class.isAssignableFrom(recvClass))) {
            throw new ComputeException(
                    CLASS_INITIALIZATION_ERR,
                    "'" + recvClass.getName() + "' does not implement DataStreamerReceiver interface"
            );
        }

        try {
            Constructor<? extends DataStreamerReceiver<T, R, A>> constructor = recvClass.getDeclaredConstructor();

            if (!constructor.canAccess(null)) {
                constructor.setAccessible(true);
            }

            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new ComputeException(CLASS_INITIALIZATION_ERR, "Cannot instantiate streamer receiver", e);
        }
    }

    /**
     * Resolve receiver class name.
     *
     * @param classLoader Class loader.
     * @param className Class name.
     * @param <R> Return type.
     * @return Receiver class.
     */
    public static <T, R, A> Class<DataStreamerReceiver<T, R, A>> receiverClass(ClassLoader classLoader, String className) {
        try {
            return (Class<DataStreamerReceiver<T, R, A>>) Class.forName(className, true, classLoader);
        } catch (ClassNotFoundException e) {
            throw new ComputeException(CLASS_INITIALIZATION_ERR, "Cannot load receiver class by name '" + className + "'", e);
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
     * @return Completable future with result.
     */
    public static CompletableFuture<ComputeJobDataHolder> resultFromJobResultResponse(JobResultResponse jobResultResponse) {
        Throwable throwable = jobResultResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture(jobResultResponse.result());
    }

    /**
     * Extract compute job states from states response.
     *
     * @param jobStatesResponse Job states result message response.
     * @return Completable future with result.
     */
    public static CompletableFuture<Collection<JobState>> statesFromJobStatesResponse(JobStatesResponse jobStatesResponse) {
        Throwable throwable = jobStatesResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture(jobStatesResponse.states());
    }

    /**
     * Extract compute job state from state response.
     *
     * @param jobStateResponse Job state result message response.
     * @return Completable future with result.
     */
    public static CompletableFuture<@Nullable JobState> stateFromJobStateResponse(JobStateResponse jobStateResponse) {
        Throwable throwable = jobStateResponse.throwable();
        if (throwable != null) {
            return failedFuture(throwable);
        }

        return completedFuture(jobStateResponse.state());
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
        } else if (origin instanceof CancellationException) {
            return new ComputeException(COMPUTE_JOB_CANCELLED_ERR, "Job execution cancelled", origin);
        } else {
            return new ComputeException(COMPUTE_JOB_FAILED_ERR, "Job execution failed: " + origin, origin);
        }
    }

    /**
     * Unmarshals the input using provided marshaller if input is a byte array. If no marshaller is provided, then, if the input is a
     * {@link Tuple} and provided pojo type is not {@code null} and not a {@link Tuple}, unmarshals the input as a pojo using the provided
     * pojo type. If the input is a {@link ComputeJobDataHolder}, extracts the data from it and unmarshals using the same strategy.
     *
     * @param marshaller Optional marshaller to unmarshal the input.
     * @param input Input object.
     * @param pojoType Pojo type to use when unmarshalling as a pojo.
     * @param classLoader Class loader to set before unmarshalling.
     * @param <T> Result type.
     * @return Unmarshalled object.
     */
    public static <T> @Nullable T unmarshalOrNotIfNull(
            @Nullable Marshaller<T, byte[]> marshaller,
            @Nullable Object input,
            @Nullable Class<?> pojoType,
            ClassLoader classLoader
    ) {
        if (input == null) {
            return null;
        }

        if (input instanceof ComputeJobDataHolder) {
            return SharedComputeUtils.unmarshalArgOrResult((ComputeJobDataHolder) input, marshaller, pojoType, classLoader);
        }

        if (marshaller == null) {
            if (input instanceof Tuple) {
                // If input was marshalled as Tuple and argument type is not tuple then it's a pojo.
                if (pojoType != null && pojoType != Tuple.class) {
                    return (T) SharedComputeUtils.unmarshalPojo(pojoType, (Tuple) input);
                }
            }
            return (T) input;
        }

        if (input instanceof byte[]) {
            return SharedComputeUtils.unmarshalData(marshaller, classLoader, (byte[]) input);
        }

        throw new ComputeException(
                MARSHALLING_TYPE_MISMATCH_ERR,
                "Marshaller is defined, expected argument type: `byte[]`, actual: `" + input.getClass() + "`."
                        + "If you want to use default marshalling strategy, "
                        + "then you should not define your marshaller in the job. "
                        + "If you would like to use your own marshaller, then double-check "
                        + "that both of them are defined in the client and in the server."
        );
    }

    /**
     * Finds the second argument type of the {@link ComputeJob#executeAsync(JobExecutionContext, T)} method in the provided job class.
     *
     * @param jobClass Job class to introspect.
     * @param <T> Type of the job argument.
     * @param <R> Type of the job result.
     * @return Type of the second argument of the method or {@code null} if no corresponding method is found.
     */
    public static <T, R> @Nullable Class<?> getJobExecuteArgumentType(Class<? extends ComputeJob<T, R>> jobClass) {
        for (Method method : jobClass.getDeclaredMethods()) {
            if (method.getParameterCount() == 2
                    && method.getParameterTypes()[0] == JobExecutionContext.class
                    && method.getParameterTypes()[1] != Object.class // skip type erased method
                    && method.getReturnType() == CompletableFuture.class
                    && "executeAsync".equals(method.getName())
            ) {
                return method.getParameterTypes()[1];
            }
        }
        return null;
    }

    /**
     * Finds the second argument type of the {@link MapReduceTask#splitAsync(TaskExecutionContext, I)} method in the provided task class.
     *
     * @param taskClass Task class to introspect.
     * @param <I> Split task (I)nput type.
     * @param <M> (M)ap job input type.
     * @param <T> Map job output (T)ype and reduce job input (T)ype.
     * @param <R> Reduce (R)esult type.
     * @return Type of the second argument of the method or {@code null} if no corresponding method is found.
     */
    public static <I, M, T, R> @Nullable Class<?> getTaskSplitArgumentType(Class<? extends MapReduceTask<I, M, T, R>> taskClass) {
        for (Method method : taskClass.getDeclaredMethods()) {
            if (method.getParameterCount() == 2
                    && method.getParameterTypes()[0] == TaskExecutionContext.class
                    && method.getParameterTypes()[1] != Object.class // skip type erased method
                    && method.getReturnType() == CompletableFuture.class
                    && "splitAsync".equals(method.getName())
            ) {
                return method.getParameterTypes()[1];
            }
        }
        return null;
    }
}
