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
package org.apache.ignite.raft.jraft.rpc.impl;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.raft.service.BeforeApplyHandler;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupListener.ShutdownException;
import org.apache.ignite.internal.raft.service.SafeTimeAwareCommandClosure;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.closure.ReadIndexClosure;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RaftRpcFactory;
import org.apache.ignite.raft.jraft.rpc.ReadActionRequest;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.rpc.WriteActionRequest;
import org.apache.ignite.raft.jraft.util.BytesUtil;
import org.jetbrains.annotations.Nullable;

/**
 * Process action request.
 */
public class ActionRequestProcessor implements RpcProcessor<ActionRequest> {
    private static final IgniteLogger LOG = Loggers.forClass(ActionRequestProcessor.class);

    private final Executor executor;

    private final RaftMessagesFactory factory;

    /**
    * Mapping from group IDs to monitors used to synchronized on (only used when
    * RaftGroupListener instance implements {@link BeforeApplyHandler} and the command is a write command.
    */
    private final Map<String, Object> groupIdsToMonitors = new ConcurrentHashMap<>();

    public ActionRequestProcessor(Executor executor, RaftMessagesFactory factory) {
        this.executor = executor;
        this.factory = factory;
    }

    /** {@inheritDoc} */
    @Override
    public final void handleRequest(RpcContext rpcCtx, ActionRequest request) {
        Node node = rpcCtx.getNodeManager().get(request.groupId(), new PeerId(rpcCtx.getLocalConsistentId()));

        if (node == null) {
            rpcCtx.sendResponse(factory.errorResponse().errorCode(RaftError.UNKNOWN.getNumber()).build());

            return;
        }

        Marshaller commandsMarshaller = node.getOptions().getCommandsMarshaller();

        assert commandsMarshaller != null : "Marshaller for group " + request.groupId() + " is not found.";

        handleRequestInternal(rpcCtx, node, request, commandsMarshaller);
    }

    /**
     * Internal part of the {@link #handleRequest(RpcContext, ActionRequest)}, that contains resolved RAFT node, as well as a commands
     * marshaller instance. May be conveniently reused in subclasses.
     */
    protected void handleRequestInternal(RpcContext rpcCtx, Node node, ActionRequest request, Marshaller commandsMarshaller) {
        DelegatingStateMachine fsm = (DelegatingStateMachine) node.getOptions().getFsm();
        RaftGroupListener listener = fsm.getListener();

        if (request instanceof WriteActionRequest) {
            WriteActionRequest writeRequest = (WriteActionRequest)request;

            WriteCommand command = writeRequest.deserializedCommand();

            if (command == null) {
                command = commandsMarshaller.unmarshall(writeRequest.command());
            }

            if (listener instanceof BeforeApplyHandler) {
                synchronized (groupIdSyncMonitor(request.groupId())) {
                    Command patchedCommand = ((BeforeApplyHandler) listener).onBeforeApply(command);

                    writeRequest = patchRequest(writeRequest, command, patchedCommand, commandsMarshaller);

                    applyWrite(node, writeRequest, (WriteCommand) patchedCommand, rpcCtx);
                }
            } else {
                applyWrite(node, writeRequest, command, rpcCtx);
            }
        } else {
            ReadActionRequest readRequest = (ReadActionRequest) request;

            if (listener instanceof BeforeApplyHandler) {
                ReadCommand command = readRequest.command();

                Command patchedCommand = ((BeforeApplyHandler) listener).onBeforeApply(command);

                readRequest = patchRequest(readRequest, command, patchedCommand, commandsMarshaller);
            }

            applyRead(node, readRequest, rpcCtx);
        }
    }

    /**
     * This method returns action request with a serialized version of the updated command, if it has been updated. Otherwise, the method
     * returns the original {@code request} instance. The reason for such behavior is the fact that we use {@code byte[]} in action
     * requests, thus modified command should be serialized twice.
     */
    private <AR extends ActionRequest> AR patchRequest(
            AR request,
            Command originalCommand,
            Command patchedCommand,
            Marshaller commandsMarshaller
    ) {
        if (originalCommand == patchedCommand) {
            return request;
        }

        if (request instanceof WriteActionRequest) {
            return (AR) factory.writeActionRequest()
                .groupId(request.groupId())
                .command(commandsMarshaller.marshall(patchedCommand))
                .deserializedCommand((WriteCommand)patchedCommand)
                .build();
        } else {
            return (AR) factory.readActionRequest()
                .groupId(request.groupId())
                .command((ReadCommand)patchedCommand)
                .readOnlySafe(((ReadActionRequest)request).readOnlySafe())
                .build();
        }
    }

    private Object groupIdSyncMonitor(String groupId) {
        assert groupId != null;

        return groupIdsToMonitors.computeIfAbsent(groupId, k -> groupId);
    }

    /**
     * @param node The node.
     * @param request The request.
     * @param command The command.
     * @param rpcCtx The context.
     */
    private void applyWrite(Node node, WriteActionRequest request, WriteCommand command, RpcContext rpcCtx) {
        ByteBuffer wrapper = ByteBuffer.wrap(request.command());
        node.apply(new Task(wrapper, new LocalAwareWriteCommandClosure() {
            private HybridTimestamp safeTs;

            @Override
            public void result(Serializable res) {
                sendResponse(res, rpcCtx);
            }

            @Override
            public WriteCommand command() {
                return command;
            }

            @Override
            public @Nullable HybridTimestamp safeTimestamp() {
                return safeTs;
            }

            @Override
            public void run(Status status) {
                assert !status.isOk() : status;

                sendRaftError(rpcCtx, status, node);
            }

            @Override
            public void safeTimestamp(HybridTimestamp safeTs) {
                assert this.safeTs == null : "Safe time can be set only once";
                // Apply binary patch.
                node.getOptions().getCommandsMarshaller().patch(wrapper, safeTs);
                // Avoid modifying WriteCommand object, because it's shared between raft pipeline threads.
                this.safeTs = safeTs;
            }
        }));
    }

    /**
     * @param node The node.
     * @param request The request.
     * @param rpcCtx The context.
     */
    private void applyRead(Node node, ReadActionRequest request, RpcContext rpcCtx) {
        if (request.readOnlySafe()) {
            node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                @Override public void run(Status status, long index, byte[] reqCtx) {
                    if (status.isOk()) {
                        JraftServerImpl.DelegatingStateMachine fsm =
                                (JraftServerImpl.DelegatingStateMachine) node.getOptions().getFsm();

                        try {
                            fsm.getListener().onRead(List.<CommandClosure<ReadCommand>>of(new CommandClosure<>() {
                                @Override public ReadCommand command() {
                                    return request.command();
                                }

                                @Override public void result(Serializable res) {
                                    sendResponse(res, rpcCtx);
                                }
                            }).iterator());
                        }
                        catch (Exception e) {
                            sendRaftError(rpcCtx, RaftError.ESTATEMACHINE, e.getMessage());
                        }
                    }
                    else
                        sendRaftError(rpcCtx, status, node);
                }
            });
        } else {
            // TODO asch remove copy paste, batching https://issues.apache.org/jira/browse/IGNITE-14832
            JraftServerImpl.DelegatingStateMachine fsm =
                    (JraftServerImpl.DelegatingStateMachine) node.getOptions().getFsm();

            try {
                fsm.getListener().onRead(List.<CommandClosure<ReadCommand>>of(new CommandClosure<>() {
                    @Override public ReadCommand command() {
                        return request.command();
                    }

                    @Override public void result(Serializable res) {
                        sendResponse(res, rpcCtx);
                    }
                }).iterator());
            }
            catch (Exception e) {
                sendRaftError(rpcCtx, RaftError.ESTATEMACHINE, e.getMessage());
            }
        }
    }

    private void sendResponse(Serializable res, RpcContext rpcCtx) {
        if (res instanceof ShutdownException) {
            rpcCtx.sendResponse(factory.errorResponse().errorCode(RaftError.ESHUTDOWN.getNumber()).build());
        } else if (res instanceof Throwable) {
            sendSMError(rpcCtx, (Throwable)res, false);
        } else {
            rpcCtx.sendResponse(factory.actionResponse().result(res).build());
        }
    }

    /** {@inheritDoc} */
    @Override public String interest() {
        return ActionRequest.class.getName();
    }

    /** {@inheritDoc} */
    @Override public Executor executor() {
        return executor;
    }

    /**
     * Sends raft error response with raft error code and message.
     *
     * @param ctx   Context.
     * @param error RaftError code.
     * @param msg   Message.
     */
    private void sendRaftError(RpcContext ctx, RaftError error, String msg) {
        RpcRequests.ErrorResponse resp = factory.errorResponse()
            .errorCode(error.getNumber())
            .errorMsg(msg)
            .build();

        ctx.sendResponse(resp);
    }

    /**
     * Sends client's state machine error response with passed throwable.
     *
     * @param ctx       The context.
     * @param th        Throwable that must be passes to response.
     * @param compacted {@code true} if throwable must be changed to compacted version of throwable.
     * See {@link SMCompactedThrowable}
     */
    private void sendSMError(RpcContext ctx, Throwable th, boolean compacted) {
        RpcRequests.SMErrorResponse resp = factory.sMErrorResponse()
            .error(compacted ? new SMCompactedThrowable(th) : new SMFullThrowable(th))
            .build();

        ctx.sendResponse(resp);

        LOG.warn("Error occurred on a user's state machine", th);
    }

    /**
     * @param ctx    The context.
     * @param status The status.
     * @param node   Raft node.
     */
    private void sendRaftError(RpcContext ctx, Status status, Node node) {
        RaftError raftError = status.getRaftError();

        Message response = null;

        if (raftError == RaftError.EPERM) {
            PeerId leaderId = node.getLeaderId();

            if (leaderId != null)
                response = RaftRpcFactory.DEFAULT
                    .newResponse(leaderId.toString(), factory, RaftError.EPERM, status.getErrorMsg());
        }

        if (response == null)
            response = RaftRpcFactory.DEFAULT
                .newResponse(factory, raftError, status.getErrorMsg());

        ctx.sendResponse(response);
    }

    /**
     * The command closure which is used to propagate replication state to local FSM.
     */
    private interface LocalAwareWriteCommandClosure extends Closure, SafeTimeAwareCommandClosure {
    }
}
