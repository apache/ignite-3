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
import static org.apache.ignite.internal.compute.ComputeJobDataType.MARSHALLED_CUSTOM;
import static org.apache.ignite.internal.compute.ComputeJobDataType.NATIVE;
import static org.apache.ignite.internal.compute.ComputeJobDataType.POJO;
import static org.apache.ignite.internal.compute.ComputeJobDataType.TUPLE;
import static org.apache.ignite.internal.compute.ComputeJobDataType.TUPLE_COLLECTION;
import static org.apache.ignite.internal.compute.PojoConverter.fromTuple;
import static org.apache.ignite.internal.compute.PojoConverter.toTuple;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;
import static org.apache.ignite.lang.ErrorGroups.Compute.CLASS_INITIALIZATION_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.COMPUTE_JOB_FAILED_ERR;
import static org.apache.ignite.lang.ErrorGroups.Compute.MARSHALLING_TYPE_MISMATCH_ERR;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.binarytuple.inlineschema.TupleWithSchemaMarshalling;
import org.apache.ignite.internal.client.proto.ClientBinaryTupleUtils;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.message.DeploymentUnitMsg;
import org.apache.ignite.internal.compute.message.ExecuteResponse;
import org.apache.ignite.internal.compute.message.JobCancelResponse;
import org.apache.ignite.internal.compute.message.JobChangePriorityResponse;
import org.apache.ignite.internal.compute.message.JobResultResponse;
import org.apache.ignite.internal.compute.message.JobStateResponse;
import org.apache.ignite.internal.compute.message.JobStatesResponse;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteCheckedException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.MarshallingException;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class for compute.
 */
public class ComputeUtils {
    private static final ComputeMessagesFactory MESSAGES_FACTORY = new ComputeMessagesFactory();

    private static final Set<Class<?>> NATIVE_TYPES = Arrays.stream(ColumnType.values())
            .map(ColumnType::javaClass)
            .collect(Collectors.toUnmodifiableSet());

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
            return (Class<ComputeJob<T, R>>) Class.forName(jobClassName, true, jobClassLoader);
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
     * @param taskClassLoader Class loader.
     * @param taskClassName Map reduce task class name.
     * @param <R> Map reduce task return type.
     * @return Map reduce task class.
     */
    public static <I, M, T, R> Class<MapReduceTask<I, M, T, R>> taskClass(ClassLoader taskClassLoader, String taskClassName) {
        try {
            return (Class<MapReduceTask<I, M, T, R>>) Class.forName(taskClassName, true, taskClassLoader);
        } catch (ClassNotFoundException e) {
            throw new ComputeException(CLASS_INITIALIZATION_ERR, "Cannot load task class by name '" + taskClassName + "'", e);
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
     * @param <T> Result type.
     * @return Unmarshalled object.
     */
    public static <T> @Nullable T unmarshalOrNotIfNull(
            @Nullable Marshaller<T, byte[]> marshaller,
            @Nullable Object input,
            @Nullable Class<?> pojoType
    ) {
        if (input == null) {
            return null;
        }

        if (input instanceof ComputeJobDataHolder) {
            return unmarshalArgumentFromDataHolder(marshaller, (ComputeJobDataHolder) input, pojoType);
        }

        if (marshaller == null) {
            if (input instanceof Tuple) {
                // If input was marshalled as Tuple and argument type is not tuple then it's a pojo.
                if (pojoType != null && pojoType != Tuple.class) {
                    return (T) unmarshalPojo(pojoType, (Tuple) input);
                }
            }
            return (T) input;
        }

        if (input instanceof byte[]) {
            try {
                return marshaller.unmarshal((byte[]) input);
            } catch (Exception ex) {
                throw new ComputeException(MARSHALLING_TYPE_MISMATCH_ERR, "Exception in user-defined marshaller: " + ex.getMessage(), ex);
            }
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
     * Unmarshals the input from the {@link ComputeJobDataHolder} using provided marshaller if input was marshalled on the client. If the
     * input was marshalled as a {@link Tuple} or POJO, then, if provided pojo type is not {@code null} and not a {@link Tuple}, unmarshals
     * the input as a pojo using the provided pojo type, otherwise unmarshals it as a {@link Tuple}.
     *
     * @param marshaller Optional marshaller to unmarshal the input.
     * @param argumentHolder Argument holder.
     * @param pojoType Pojo type to use when unmarshalling as a pojo.
     * @param <T> Result type.
     * @return Unmarshalled object.
     */
    private static <T> @Nullable T unmarshalArgumentFromDataHolder(
            @Nullable Marshaller<T, byte[]> marshaller,
            ComputeJobDataHolder argumentHolder,
            @Nullable Class<?> pojoType
    ) {
        ComputeJobDataType type = argumentHolder.type();
        if (type != MARSHALLED_CUSTOM && marshaller != null) {
            throw new ComputeException(
                    MARSHALLING_TYPE_MISMATCH_ERR,
                    "Marshaller is defined on the server, but the argument was not marshalled on the client. "
                            + "If you want to use default marshalling strategy, "
                            + "then you should not define your marshaller in the job. "
                            + "If you would like to use your own marshaller, then double-check "
                            + "that both of them are defined in the client and in the server."
            );
        }
        switch (type) {
            case NATIVE: {
                var reader = new BinaryTupleReader(3, argumentHolder.data());
                return (T) ClientBinaryTupleUtils.readObject(reader, 0);
            }

            case TUPLE: // Fallthrough TODO https://issues.apache.org/jira/browse/IGNITE-23320
            case POJO:
                Tuple tuple = TupleWithSchemaMarshalling.unmarshal(argumentHolder.data());
                if (pojoType != null && pojoType != Tuple.class) {
                    return (T) unmarshalPojo(pojoType, tuple);
                }
                return (T) tuple;

            case MARSHALLED_CUSTOM:
                if (marshaller == null) {
                    throw new ComputeException(MARSHALLING_TYPE_MISMATCH_ERR, "Marshaller should be defined on the client");
                }
                try {
                    return marshaller.unmarshal(argumentHolder.data());
                } catch (Exception ex) {
                    throw new ComputeException(MARSHALLING_TYPE_MISMATCH_ERR, "Exception in user-defined marshaller", ex);
                }

            case TUPLE_COLLECTION: {
                // TODO: IGNITE-24059 Deduplicate with ClientComputeJobUnpacker.
                ByteBuffer collectionBuf = ByteBuffer.wrap(argumentHolder.data()).order(ByteOrder.LITTLE_ENDIAN);
                int count = collectionBuf.getInt();
                BinaryTupleReader reader = new BinaryTupleReader(count, collectionBuf.slice().order(ByteOrder.LITTLE_ENDIAN));

                List<Tuple> res = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    ByteBuffer elementBytes = reader.bytesValueAsBuffer(i);

                    if (elementBytes == null) {
                        res.add(null);
                        continue;
                    }

                    res.add(TupleWithSchemaMarshalling.unmarshal(elementBytes));
                }

                return (T) res;
            }

            default:
                throw new ComputeException(MARSHALLING_TYPE_MISMATCH_ERR, "Unexpected job argument type: " + type);
        }
    }

    private static Object unmarshalPojo(Class<?> pojoType, Tuple input) {
        try {
            Object obj = pojoType.getConstructor().newInstance();

            fromTuple(obj, input);

            return obj;
        } catch (NoSuchMethodException e) {
            throw new UnmarshallingException("Class " + pojoType.getName() + " doesn't have public default constructor. "
                    + "Add the constructor or define argument marshaller in the compute job.", e);
        } catch (InvocationTargetException e) {
            throw new UnmarshallingException("Constructor has thrown an exception", e);
        } catch (InstantiationException e) {
            throw new UnmarshallingException("Can't instantiate an object of class " + pojoType.getName(), e);
        } catch (IllegalAccessException e) {
            throw new UnmarshallingException("Constructor is inaccessible", e);
        } catch (PojoConversionException e) {
            throw new UnmarshallingException("Can't unpack object", e);
        }
    }

    /**
     * Marshals the job result using either provided marshaller if not {@code null} or depending on the type of the result either as a
     * {@link Tuple}, a native type (see {@link ColumnType}) or a POJO. Wraps the marshalled data with the data type in the
     * {@link ComputeJobDataHolder} to be unmarshalled on the client.
     *
     * @param result Compute job result.
     * @param marshaller Optional result marshaller.
     *
     * @return Data holder.
     */
    @Nullable
    static ComputeJobDataHolder marshalAndWrapResult(Object result, @Nullable Marshaller<Object, byte[]> marshaller) {
        if (result == null) {
            return null;
        }

        if (marshaller != null) {
            byte[] data = marshaller.marshal(result);
            if (data == null) {
                return null;
            }
            return new ComputeJobDataHolder(MARSHALLED_CUSTOM, data);
        }

        if (result instanceof Tuple) {
            Tuple tuple = (Tuple) result;
            return new ComputeJobDataHolder(TUPLE, TupleWithSchemaMarshalling.marshal(tuple));
        }

        if (result instanceof Collection) {
            Collection<?> col = (Collection<?>) result;

            // Pack entire collection into a single binary blob, starting with the number of elements (4 bytes, little-endian).
            BinaryTupleBuilder tupleBuilder = SharedComputeUtils.packCollectionToBinaryTuple(col);

            ByteBuffer binTupleBytes = tupleBuilder.build();

            byte[] resArr = new byte[Integer.BYTES + binTupleBytes.remaining()];
            ByteBuffer resBuf = ByteBuffer.wrap(resArr).order(ByteOrder.LITTLE_ENDIAN);
            resBuf.putInt(col.size());
            resBuf.put(binTupleBytes);

            return new ComputeJobDataHolder(TUPLE_COLLECTION, resArr);
        }


        if (isNativeType(result.getClass())) {
            // Builder with inline schema.
            // Value is represented by 3 tuple elements: type, scale, value.
            var builder = new BinaryTupleBuilder(3, 3, false);
            ClientBinaryTupleUtils.appendObject(builder, result);
            return new ComputeJobDataHolder(NATIVE, IgniteUtils.byteBufferToByteArray(builder.build()));
        }

        try {
            // TODO https://issues.apache.org/jira/browse/IGNITE-23320
            Tuple tuple = toTuple(result);
            return new ComputeJobDataHolder(POJO, TupleWithSchemaMarshalling.marshal(tuple));
        } catch (PojoConversionException e) {
            throw new MarshallingException("Can't pack object", e);
        }
    }

    private static boolean isNativeType(Class<?> clazz) {
        return NATIVE_TYPES.contains(clazz);
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
