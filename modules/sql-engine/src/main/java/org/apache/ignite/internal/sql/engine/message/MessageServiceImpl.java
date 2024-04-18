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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.replicator.message.TimestampAware;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * MessageServiceImpl.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class MessageServiceImpl implements MessageService {
    private final MessagingService messagingSrvc;

    private final String localNodeName;

    private final QueryTaskExecutor taskExecutor;

    private final IgniteSpinBusyLock busyLock;

    private final ClockService clockService;

    private volatile Map<Short, MessageListener> lsnrs;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public MessageServiceImpl(
            String localNodeName,
            MessagingService messagingSrvc,
            QueryTaskExecutor taskExecutor,
            IgniteSpinBusyLock busyLock,
            ClockService clockService
    ) {
        this.localNodeName = localNodeName;
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
            if (localNodeName.equals(nodeName)) {
                onMessage(nodeName, msg);

                return nullCompletedFuture();
            } else {
                return messagingSrvc.send(nodeName, ChannelType.DEFAULT, msg);
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
            lsnrs = new HashMap<>();
        }

        MessageListener old = lsnrs.put(type, lsnr);

        assert old == null : old;
    }

    private void onMessage(String consistentId, NetworkMessage msg) {
        if (msg instanceof ExecutionContextAwareMessage) {
            ExecutionContextAwareMessage msg0 = (ExecutionContextAwareMessage) msg;
            taskExecutor.execute(msg0.queryId(), msg0.fragmentId(), () -> onMessageInternal(consistentId, msg));
        } else {
            taskExecutor.execute(() -> onMessageInternal(consistentId, msg));
        }
    }

    private void onMessage(NetworkMessage msg, ClusterNode sender, @Nullable Long correlationId) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            assert msg.groupType() == GROUP_TYPE : "unexpected message group grpType=" + msg.groupType();

            // TODO https://issues.apache.org/jira/browse/IGNITE-21709
            if (msg instanceof TimestampAware) {
                clockService.updateClock(((TimestampAware) msg).timestamp());
            }

            onMessage(sender.name(), msg);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void onMessageInternal(String consistentId, NetworkMessage msg) {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            MessageListener lsnr = Objects.requireNonNull(
                    lsnrs.get(msg.messageType()),
                    "there is no listener for msgType=" + msg.messageType()
            );

            lsnr.onMessage(consistentId, msg);
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
