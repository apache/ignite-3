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

package org.apache.ignite.internal.sql.engine.message;

import static org.apache.ignite.internal.sql.engine.message.SqlQueryMessageGroup.GROUP_TYPE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.sneakyThrow;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.Nullable;

/**
 * MessageServiceImpl.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class MessageServiceImpl implements MessageService {
    private final MessagingService messagingSrvc;

    private final InternalClusterNode localNode;

    private final QueryTaskExecutor taskExecutor;

    private final IgniteSpinBusyLock busyLock;

    private final ClockService clockService;

    private volatile Int2ObjectMap<MessageListener> lsnrs;

    /**
     * Constructors the object.
     *
     * @param localNode The local node.
     * @param messagingSrvc Actual service to send messages over network.
     * @param taskExecutor An executor to delegate processing of received message.
     * @param busyLock A lock to synchronize message processing and parent service stop.
     * @param clockService A clock to propagate updated timestamp.
     */
    public MessageServiceImpl(
            InternalClusterNode localNode,
            MessagingService messagingSrvc,
            QueryTaskExecutor taskExecutor,
            IgniteSpinBusyLock busyLock,
            ClockService clockService
    ) {
        this.localNode = localNode;
        this.messagingSrvc = messagingSrvc;
        this.taskExecutor = taskExecutor;
        this.busyLock = busyLock;
        this.clockService = clockService;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        messagingSrvc.addMessageHandler(SqlQueryMessageGroup.class, this::onMessage);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> send(String nodeName, NetworkMessage msg) {
        if (!busyLock.enterBusy()) {
            return nullCompletedFuture();
        }

        try {
            if (localNode.name().equals(nodeName)) {
                onMessage(localNode, msg);

                return nullCompletedFuture();
            } else {
                return messagingSrvc.send(nodeName, ChannelType.DEFAULT, msg)
                        .exceptionally(ex -> {
                            if (ex instanceof UnresolvableConsistentIdException) {
                                ex = new UnknownNodeException(nodeName);
                            }

                            sneakyThrow(ex);

                            throw new AssertionError("Should not get here"); 
                        });
            }
        } catch (Exception ex) {
            return CompletableFuture.failedFuture(ex);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void register(MessageListener lsnr, short type) {
        if (lsnrs == null) {
            lsnrs = new Int2ObjectOpenHashMap<>();
        }

        MessageListener old = lsnrs.put(type, lsnr);

        assert old == null : old;
    }

    private void onMessage(InternalClusterNode sender, NetworkMessage msg) {
        if (msg instanceof CancelOperationRequest) {
            return;
        }

        if (msg instanceof ExecutionContextAwareMessage) {
            ExecutionContextAwareMessage msg0 = (ExecutionContextAwareMessage) msg;
            taskExecutor.execute(msg0.queryId(), msg0.fragmentId(), () -> onMessageInternal(sender, msg));
        } else {
            taskExecutor.execute(() -> onMessageInternal(sender, msg));
        }
    }

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private void onMessage(NetworkMessage msg, InternalClusterNode sender, @Nullable Long correlationId) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            assert msg.groupType() == GROUP_TYPE : "unexpected message group grpType=" + msg.groupType();

            // TODO https://issues.apache.org/jira/browse/IGNITE-21709
            if (msg instanceof TimestampAware) {
                clockService.updateClock(((TimestampAware) msg).timestamp());
            }

            onMessage(sender, msg);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void onMessageInternal(InternalClusterNode sender, NetworkMessage msg) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            MessageListener lsnr = Objects.requireNonNull(
                    lsnrs.get(msg.messageType()),
                    "there is no listener for msgType=" + msg.messageType()
            );

            lsnr.onMessage(sender, msg);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        if (lsnrs != null) {
            lsnrs.clear();
        }
    }
}
