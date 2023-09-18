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
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl.DelegatingStateMachine;
import org.apache.ignite.internal.raft.service.BeforeApplyHandler;
import org.apache.ignite.internal.raft.service.CommandClosure;
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
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.apache.ignite.raft.jraft.util.BytesUtil;
import org.apache.ignite.raft.jraft.util.Marshaller;

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
    public void handleRequest(RpcContext rpcCtx, ActionRequest request) {
        Node node = rpcCtx.getNodeManager().get(request.groupId(), new PeerId(rpcCtx.getLocalConsistentId()));

        if (node == null) {
            rpcCtx.sendResponse(factory.errorResponse().errorCode(RaftError.UNKNOWN.getNumber()).build());

            return;
        }

        JraftServerImpl.DelegatingStateMachine fsm = (JraftServerImpl.DelegatingStateMachine) node.getOptions().getFsm();

        if (request.command() instanceof WriteCommand) {
            if (fsm.getListener() instanceof BeforeApplyHandler) {
                synchronized (groupIdSyncMonitor(request.groupId())) {
                    callOnBeforeApply(request, fsm);
                    applyWrite(node, request, rpcCtx);
                }
            } else {
                applyWrite(node, request, rpcCtx);
            }
        } else {
            if (fsm.getListener() instanceof BeforeApplyHandler) {
                callOnBeforeApply(request, fsm);
            }

            applyRead(node, request, rpcCtx);
        }
    }
    private static void callOnBeforeApply(ActionRequest request, DelegatingStateMachine fsm) {
        ((BeforeApplyHandler) fsm.getListener()).onBeforeApply(request.command());
    }

    private Object groupIdSyncMonitor(String groupId) {
        assert groupId != null;

        return groupIdsToMonitors.computeIfAbsent(groupId, k -> groupId);
    }

    /**
     * @param node    The node.
     * @param request The request.
     * @param rpcCtx  The context.
     */
    private void applyWrite(Node node, ActionRequest request, RpcContext rpcCtx) {
        Marshaller commandsMarshaller = node.getOptions().getCommandsMarshaller();

        assert commandsMarshaller != null;

        node.apply(new Task(ByteBuffer.wrap(commandsMarshaller.marshall(request.command())),
                new CommandClosureImpl<>(request.command()) {
                    @Override
                    public void result(Serializable res) {
                        if (res instanceof Throwable) {
                            sendSMError(rpcCtx, (Throwable)res, true);

                            return;
                        }

                        rpcCtx.sendResponse(factory.actionResponse().result(res).build());
                    }

                    @Override
                    public void run(Status status) {
                        assert !status.isOk() : status;

                        sendRaftError(rpcCtx, status, node);
                    }
                }));
    }

    /**
     * @param node    The node.
     * @param request The request.
     * @param rpcCtx  The context.
     */
    private void applyRead(Node node, ActionRequest request, RpcContext rpcCtx) {
        if (request.readOnlySafe()) {
            node.readIndex(BytesUtil.EMPTY_BYTES, new ReadIndexClosure() {
                @Override public void run(Status status, long index, byte[] reqCtx) {
                    if (status.isOk()) {
                        JraftServerImpl.DelegatingStateMachine fsm =
                                (JraftServerImpl.DelegatingStateMachine) node.getOptions().getFsm();

                        try {
                            fsm.getListener().onRead(List.<CommandClosure<ReadCommand>>of(new CommandClosure<>() {
                                @Override public ReadCommand command() {
                                    return (ReadCommand)request.command();
                                }

                                @Override public void result(Serializable res) {
                                    if (res instanceof Throwable) {
                                        sendSMError(rpcCtx, (Throwable)res, true);

                                        return;
                                    }

                                    rpcCtx.sendResponse(factory.actionResponse().result(res).build());
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
                        return (ReadCommand)request.command();
                    }

                    @Override public void result(Serializable res) {
                        if (res instanceof Throwable) {
                            sendSMError(rpcCtx, (Throwable)res, true);

                            return;
                        }
                        rpcCtx.sendResponse(factory.actionResponse().result(res).build());
                    }
                }).iterator());
            }
            catch (Exception e) {
                sendRaftError(rpcCtx, RaftError.ESTATEMACHINE, e.getMessage());
            }
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

        LOG.info("Error occurred on a user's state machine", th);
    }

    /**
     * @param ctx    The context.
     * @param status The status.
     * @param node   Raft node.
     */
    private void sendRaftError(RpcContext ctx, Status status, Node node) {
        RaftError raftError = status.getRaftError();

        Message response;

        if (raftError == RaftError.EPERM && node.getLeaderId() != null)
            response = RaftRpcFactory.DEFAULT
                .newResponse(node.getLeaderId().toString(), factory, RaftError.EPERM, status.getErrorMsg());
        else
            response = RaftRpcFactory.DEFAULT
                .newResponse(factory, raftError, status.getErrorMsg());

        ctx.sendResponse(response);
    }

    /** The implementation. */
    private abstract static class CommandClosureImpl<T extends Command> implements Closure, CommandClosure<T> {
        private final T command;

        /**
         * @param command The command.
         */
        public CommandClosureImpl(T command) {
            this.command = command;
        }

        /** {@inheritDoc} */
        @Override public T command() {
            return command;
        }
    }
}
