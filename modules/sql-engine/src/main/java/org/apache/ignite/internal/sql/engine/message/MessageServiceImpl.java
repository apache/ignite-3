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
import static org.apache.ignite.lang.ErrorGroups.Sql.MESSAGE_SEND_ERR;
import static org.apache.ignite.lang.ErrorGroups.Sql.NODE_LEFT_ERR;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TopologyService;
import org.jetbrains.annotations.Nullable;

/**
 * MessageServiceImpl.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class MessageServiceImpl implements MessageService {
    private final TopologyService topSrvc;

    private final MessagingService messagingSrvc;

    private final String locNodeName;

    private final QueryTaskExecutor taskExecutor;

    private final IgniteSpinBusyLock busyLock;

    private volatile Map<Short, MessageListener> lsnrs;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public MessageServiceImpl(
            TopologyService topSrvc,
            MessagingService messagingSrvc,
            QueryTaskExecutor taskExecutor,
            IgniteSpinBusyLock busyLock
    ) {
        this.topSrvc = topSrvc;
        this.messagingSrvc = messagingSrvc;
        this.taskExecutor = taskExecutor;
        this.busyLock = busyLock;

        locNodeName = topSrvc.localMember().name();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        messagingSrvc.addMessageHandler(SqlQueryMessageGroup.class, this::onMessage);
    }

    /** {@inheritDoc} */
    @Override
    public void send(String nodeName, NetworkMessage msg) throws IgniteInternalCheckedException {
        if (!busyLock.enterBusy()) {
            return;
        }

        try {
            if (locNodeName.equals(nodeName)) {
                onMessage(nodeName, msg);
            } else {
                ClusterNode node = topSrvc.getByConsistentId(nodeName);

                if (node == null) {
                    throw new IgniteInternalException(
                            NODE_LEFT_ERR, "Failed to send message to node (has node left grid?): " + nodeName
                    );
                }

                try {
                    messagingSrvc.send(node, msg).join();
                } catch (Exception ex) {
                    if (ex instanceof IgniteInternalCheckedException) {
                        throw (IgniteInternalCheckedException) ex;
                    }

                    throw new IgniteInternalCheckedException(MESSAGE_SEND_ERR, ex);
                }
            }
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

    /** {@inheritDoc} */
    @Override
    public boolean alive(String nodeName) {
        return topSrvc.getByConsistentId(nodeName) != null;
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
