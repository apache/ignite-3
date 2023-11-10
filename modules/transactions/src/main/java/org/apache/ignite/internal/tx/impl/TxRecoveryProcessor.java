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

package org.apache.ignite.internal.tx.impl;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterService;

/**
 * The processor receives transaction recovery messages. The recovery message is received when the node has a commit partition for the
 * specific transaction.
 */
public class TxRecoveryProcessor {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxRecoveryProcessor.class);

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Cluster service. */
    private final ClusterService clusterService;

    /** The local map for tx states. */
    private ConcurrentHashMap<UUID, TxStateMeta> txStateMap;

    /**
     * The constructor.
     *
     * @param clusterService Cluster service.
     */
    public TxRecoveryProcessor(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    /**
     * Starts the recovery processor.
     *
     * @param txStateMap Transaction state map.
     */
    public void start(ConcurrentHashMap<UUID, TxStateMeta> txStateMap) {
        this.txStateMap = txStateMap;

        clusterService.messagingService().addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof TxRecoveryMessage) {
                if (busyLock.enterBusy()) {
                    try {
                        processTxRecoveryMessage((TxRecoveryMessage) msg);
                    } finally {
                        busyLock.leaveBusy();
                    }
                } else {
                    LOG.info("Transaction recovery message was ignored [msg={}]", msg);
                }
            }
        });
    }

    /**
     * Processes the recovery transaction message.
     *
     * @param msg Transaction recovery message.
     */
    private void processTxRecoveryMessage(TxRecoveryMessage msg) {
        TxStateMeta txState = txStateMap.get(msg.txId());

        if (txState == null || !TxState.isFinalState(txState.txState())) {
            assert txState == null || clusterService.topologyService().getById(txState.txCoordinatorId()) == null :
                    "Transaction coordinator is alive [tx=" + msg.txId() + ", crd=" + txState.txCoordinatorId()
                            + ']';

            LOG.warn("Transaction have to be aborted [tx={}].", msg.txId());
        }
    }

    /**
     * Stops the recovery processor.
     */
    public void stop() {
        busyLock.block();
    }
}
