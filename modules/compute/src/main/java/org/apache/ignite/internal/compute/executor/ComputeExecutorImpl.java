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

package org.apache.ignite.internal.compute.executor;

import static org.apache.ignite.internal.client.proto.pojo.PojoConverter.fromTuple;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_READ;
import static org.apache.ignite.internal.thread.ThreadOperation.STORAGE_WRITE;
import static org.apache.ignite.lang.ErrorGroups.Compute.MARSHALLING_TYPE_MISMATCH_ERR;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.compute.task.TaskExecutionContext;
import org.apache.ignite.internal.client.proto.pojo.PojoConversionException;
import org.apache.ignite.internal.compute.ComputeUtils;
import org.apache.ignite.internal.compute.ExecutionOptions;
import org.apache.ignite.internal.compute.JobExecutionContextImpl;
import org.apache.ignite.internal.compute.configuration.ComputeConfiguration;
import org.apache.ignite.internal.compute.loader.JobClassLoader;
import org.apache.ignite.internal.compute.queue.PriorityQueueExecutor;
import org.apache.ignite.internal.compute.queue.QueueExecution;
import org.apache.ignite.internal.compute.state.ComputeStateMachine;
import org.apache.ignite.internal.compute.task.JobSubmitter;
import org.apache.ignite.internal.compute.task.TaskExecutionContextImpl;
import org.apache.ignite.internal.compute.task.TaskExecutionInternal;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.marshalling.Marshaller;
import org.apache.ignite.marshalling.UnmarshallingException;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;

/**
 * Base implementation of {@link ComputeExecutor}.
 */
public class ComputeExecutorImpl implements ComputeExecutor {
    private static final IgniteLogger LOG = Loggers.forClass(ComputeExecutorImpl.class);

    private final Ignite ignite;

    private final ComputeConfiguration configuration;

    private final ComputeStateMachine stateMachine;

    private PriorityQueueExecutor executorService;

    /**
     * Constructor.
     *
     * @param ignite Ignite instance for public API access.
     * @param stateMachine Compute jobs state machine.
     * @param configuration Compute configuration.
     */
    public ComputeExecutorImpl(
            Ignite ignite,
            ComputeStateMachine stateMachine,
            ComputeConfiguration configuration
    ) {
        this.ignite = ignite;
        this.configuration = configuration;
        this.stateMachine = stateMachine;
    }

    @Override
    public <T, R> JobExecutionInternal<R> executeJob(
            ExecutionOptions options,
            Class<? extends ComputeJob<T, R>> jobClass,
            JobClassLoader classLoader,
            T input
    ) {
        assert executorService != null;

        AtomicBoolean isInterrupted = new AtomicBoolean();
        JobExecutionContext context = new JobExecutionContextImpl(ignite, isInterrupted, classLoader);
        ComputeJob<T, R> jobInstance = ComputeUtils.instantiateJob(jobClass);
        Marshaller<T, byte[]> inputMarshaller = jobInstance.inputMarshaller();
        Marshaller<R, byte[]> resultMarshaller = jobInstance.resultMarshaller();

        QueueExecution<R> execution = executorService.submit(
                unmarshalExecMarshal(input, jobClass, jobInstance, context, inputMarshaller),
                options.priority(),
                options.maxRetries()
        );

        return new JobExecutionInternal<>(execution, isInterrupted, resultMarshaller);
    }

    private static <T, R> Callable<CompletableFuture<R>> unmarshalExecMarshal(
            T input,
            Class<? extends ComputeJob<T, R>> jobClass,
            ComputeJob<T, R> jobInstance,
            JobExecutionContext context,
            @Nullable Marshaller<T, byte[]> inputMarshaller
    ) {
        return () -> jobInstance.executeAsync(context, unmarshallOrNotIfNull(inputMarshaller, input, jobClass));
    }

    private static <T, R> @Nullable T unmarshallOrNotIfNull(
            @Nullable Marshaller<T, byte[]> marshaller,
            @Nullable Object input,
            Class<? extends ComputeJob<T, R>> jobClass
    ) {
        if (input == null) {
            return null;
        }

        if (marshaller == null) {
            if (input instanceof Tuple) {
                Class<?> actualArgumentType = getArgumentType(jobClass);
                // If input was marshalled as Tuple and argument type is not tuple then it's a pojo.
                if (actualArgumentType != null && actualArgumentType != Tuple.class) {
                    return (T) unmarshalPojo(actualArgumentType, (Tuple) input);
                }
            }
            return (T) input;
        }

        if (input instanceof byte[]) {
            try {
                return marshaller.unmarshal((byte[]) input);
            } catch (Exception ex) {
                throw new ComputeException(MARSHALLING_TYPE_MISMATCH_ERR,
                        "Exception in user-defined marshaller: " + ex.getMessage(),
                        ex
                );
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

    static <T, R> @Nullable Class<?> getArgumentType(Class<? extends ComputeJob<T, R>> jobClass) {
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

    private static Object unmarshalPojo(Class<?> actualArgumentType, Tuple input) {
        try {
            Object obj = actualArgumentType.getConstructor().newInstance();

            fromTuple(obj, input);

            return obj;
        } catch (NoSuchMethodException e) {
            throw new UnmarshallingException("Class " + actualArgumentType.getName() + " doesn't have public default constructor. "
                    + "Add the constructor or define argument marshaller in the compute job.", e);
        } catch (InvocationTargetException e) {
            throw new UnmarshallingException("Constructor has thrown an exception", e);
        } catch (InstantiationException e) {
            throw new UnmarshallingException("Can't instantiate an object of class " + actualArgumentType.getName(), e);
        } catch (IllegalAccessException e) {
            throw new UnmarshallingException("Constructor is inaccessible", e);
        } catch (PojoConversionException e) {
            throw new UnmarshallingException("Can't unpack object", e);
        }
    }

    @Override
    public <I, M, T, R> TaskExecutionInternal<I, M, T, R> executeTask(
            JobSubmitter<M, T> jobSubmitter,
            Class<? extends MapReduceTask<I, M, T, R>> taskClass,
            I input
    ) {
        assert executorService != null;

        AtomicBoolean isCancelled = new AtomicBoolean();
        TaskExecutionContext context = new TaskExecutionContextImpl(ignite, isCancelled);

        return new TaskExecutionInternal<>(executorService, jobSubmitter, taskClass, context, isCancelled, input);
    }

    @Override
    public void start() {
        stateMachine.start();
        executorService = new PriorityQueueExecutor(
                configuration,
                IgniteThreadFactory.create(ignite.name(), "compute", LOG, STORAGE_READ, STORAGE_WRITE),
                stateMachine
        );
    }

    @Override
    public void stop() {
        stateMachine.stop();
        executorService.shutdown();
    }
}
