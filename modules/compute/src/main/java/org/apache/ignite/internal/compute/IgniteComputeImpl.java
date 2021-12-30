/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.compute.util.GenericUtils.getTypeArguments;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.IgniteCompute;
import org.apache.ignite.internal.compute.message.ComputeMessagesFactory;
import org.apache.ignite.internal.compute.message.ComputeMessagesType;
import org.apache.ignite.internal.compute.message.ExecuteMessage;
import org.apache.ignite.internal.compute.message.ResultMessage;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.TopologyEventHandler;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the compute service.
 */
public class IgniteComputeImpl implements IgniteCompute, IgniteComponent {
    private final ClusterService clusterService;

    private final ExecutorService computeExecutor = Executors.newSingleThreadExecutor();

    private final ComputeMessagesFactory messagesFactory = new ComputeMessagesFactory();

    private final ConcurrentMap<UUID, Execution<?>> executions = new ConcurrentHashMap<>();

    public IgniteComputeImpl(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /** {@inheritDoc} */
    @Override
    public <T, I, R> R execute(@NotNull Class<? extends ComputeTask<T, I, R>> clazz, @Nullable T argument) throws ComputeException {
        UUID executionId = UUID.randomUUID();

        Execution<I> execution = registerExecution(clazz, argument, executionId);

        Collection<ExecutionResult<I>> executionResults = execution.getResult();

        executions.remove(executionId);

        @SuppressWarnings("unchecked") ComputeTask<T, I, R> computeTask = (ComputeTask<T, I, R>) createComputeTask(clazz);

        return computeTask.reduce(executionResults.stream().map(ExecutionResult::result).collect(toList()));
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        clusterService.messagingService().addMessageHandler(ComputeMessagesType.class, new ComputeNetworkHandler());

        clusterService.topologyService().addEventHandler(new TopologyHandler());
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        IgniteUtils.shutdownAndAwaitTermination(computeExecutor, 5, TimeUnit.SECONDS);
    }

    /**
     * Creates new execution based on a task, an argument and an id.
     *
     * @param clazz Task's class.
     * @param argument Task's argument.
     * @param executionId Execution id.
     * @param <T> Argument's type.
     * @param <I> Intermediate result's type.
     * @param <R> Result's type.
     * @return Execution.
     */
    @NotNull
    private <T, I, R> Execution<I> registerExecution(Class<? extends ComputeTask<T, I, R>> clazz, T argument, UUID executionId) {
        Collection<ClusterNode> currentTopology = this.clusterService.topologyService().allMembers();

        List<String> nodeIds = currentTopology.stream().map(ClusterNode::id).collect(toList());

        var execution = new Execution<I>(nodeIds);

        Execution<?> prev = executions.putIfAbsent(executionId, execution);
        assert prev == null;

        MessagingService messagingService = this.clusterService.messagingService();

        ExecuteMessage executeMessage = messagesFactory.executeMessage()
                .executionId(executionId)
                .className(clazz.getName())
                .argument(argument)
                .build();

        currentTopology.forEach(clusterNode -> {
            try {
                messagingService.send(clusterNode, executeMessage).get(3, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                executions.remove(executionId);

                throw new ComputeException("Failed to send compute message: " + e.getMessage(), e);
            }
        });
        return execution;
    }

    /**
     * Notifies execution's of node disappearances.
     */
    private class TopologyHandler implements TopologyEventHandler {
        @Override
        public void onAppeared(ClusterNode member) {
            // No-op.
        }

        @Override
        public void onDisappeared(ClusterNode member) {
            executions.values().forEach(execution -> execution.notifyNodeDisappeared(member));
        }
    }

    /** Handles execute commands and execution results. */
    private class ComputeNetworkHandler implements NetworkMessageHandler {
        /** {@inheritDoc} */
        @Override
        public void onReceived(NetworkMessage message, NetworkAddress senderAddr, String correlationId) {
            short messageType = message.messageType();

            switch (messageType) {
                case ComputeMessagesType.EXECUTE_MESSAGE:
                    assert message instanceof ExecuteMessage;

                    handleExecuteCommand((ExecuteMessage) message, senderAddr);
                    break;

                case ComputeMessagesType.RESULT_MESSAGE:
                    assert message instanceof ResultMessage;

                    handleResult((ResultMessage) message, senderAddr);
                    break;

                default:
                    throw new IllegalArgumentException("Unknown message type: " + messageType);
            }
        }
    }

    /**
     * Handles an intermediate result sent by a node.
     *
     * @param message Message with the intermediate result.
     * @param senderAddr Node's address.
     */
    private void handleResult(ResultMessage message, NetworkAddress senderAddr) {
        UUID executionId = message.executionId();

        @SuppressWarnings("rawtypes") Execution execution = executions.get(executionId);

        ClusterNode clusterNode = clusterService.topologyService().getByAddress(senderAddr);

        if (clusterNode == null) {
            // Node left the topology, this will be handled by the TopologyHandler
            return;
        }

        //noinspection unchecked
        execution.onResultReceived(clusterNode.id(), message.result(), message.exception());
    }

    /**
     * Handles the execution command sent by a node.
     *
     * @param msg Message with the execution command.
     * @param senderAddr Node's address.
     */
    private void handleExecuteCommand(ExecuteMessage msg, NetworkAddress senderAddr) {
        String className = msg.className();
        Object argument = msg.argument();

        executeOnExecutor(className, argument).whenComplete((result, throwable) -> {
            if (throwable != null) {
                throwable = throwable.getCause();

                assert throwable instanceof ComputeException;
            }

            ResultMessage resultMessage = messagesFactory.resultMessage()
                    .executionId(msg.executionId())
                    .result(result)
                    .exception(throwable)
                    .build();

            this.clusterService.messagingService().send(senderAddr, resultMessage, null);
        });
    }

    private CompletableFuture<Object> executeOnExecutor(@NotNull String className, @Nullable Object argument) {
        return CompletableFuture.supplyAsync(() -> execute0(className, argument), computeExecutor);
    }

    /**
     * Executes compute task and returns the execution's result.
     *
     * @param className {@link ComputeTask}'s class name.
     * @param argument Task's argument.
     * @return Execution's result.
     * @throws ComputeException If failed to execute compute task.
     */
    Object execute0(@NotNull String className, @Nullable Object argument) throws ComputeException {
        Class<? extends ComputeTask<?, ?, ?>> clazz;
        try {
            //noinspection unchecked
            clazz = (Class<ComputeTask<?, ?, ?>>) Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new ComputeException("Failed to find class " + className, e);
        }

        if (!ComputeTask.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(clazz + " does not implement ComputeTask");
        }

        @SuppressWarnings("rawtypes") ComputeTask task = createComputeTask(clazz);

        try {
            checkComputeTaskMethod(clazz, argument);

            @SuppressWarnings("unchecked") Object result = task.execute(argument);
            return result;
        } catch (Exception e) {
            throw new ComputeException("Failed to execute compute task: " + e.getMessage(), e);
        }
    }

    /**
     * Checks that {@link ComputeTask}'s method accepts argument.
     *
     * @param clazz {@link ComputeTask} class.
     * @param argument Task's argument.
     */
    private void checkComputeTaskMethod(@NotNull Class<?> clazz, @Nullable Object argument) {
        if (argument == null) {
            return;
        }

        Type[] typeArguments = getTypeArguments(clazz, ComputeTask.class);

        Type typeArgument = typeArguments[0];

        Class<?> computeParameterType = null;

        if (typeArgument instanceof Class) {
            computeParameterType = (Class<?>) typeArgument;
        }

        if (computeParameterType == null) {
            computeParameterType = Object.class;
        }

        Class<?> argumentClass = argument.getClass();

        if (!computeParameterType.isAssignableFrom(argumentClass)) {
            throw new IllegalArgumentException("Task " + clazz.getName() + " expects " + computeParameterType
                    + ", but argument's type is " + argumentClass);
        }
    }

    @NotNull
    private static ComputeTask<?, ?, ?> createComputeTask(Class<? extends ComputeTask<?, ?, ?>> clazz) {
        ComputeTask<?, ?, ?> computeTask;
        try {
            Constructor<? extends ComputeTask<?, ?, ?>> declaredConstructor = clazz.getDeclaredConstructor();
            declaredConstructor.setAccessible(true);
            computeTask = declaredConstructor.newInstance();
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            throw new ComputeException("Failed to instantiate compute task: " + clazz, e);
        }
        return computeTask;
    }
}
