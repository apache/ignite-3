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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.event.LockEvent;
import org.apache.ignite.internal.tx.event.LockEventParameters;
import org.apache.ignite.internal.tx.message.TxMessageGroup;
import org.apache.ignite.internal.tx.message.TxMessagesFactory;
import org.apache.ignite.internal.tx.message.TxRecoveryMessage;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;

/**
 * The manager sends and receives transaction recovery messages.
 * <ul>
 * <li>The recovery messages are sent when a lock conflict is found and the transaction coordinator is not available.</li>
 * <li>The recovery message is received when the node has a commit partition for the specific transaction.</li>
 * </ul>
 */
public class TxRecoveryManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(TxRecoveryManager.class);

    /** Tx messages factory. */
    private static final TxMessagesFactory FACTORY = new TxMessagesFactory();

    /** Lock manager. */
    private final LockManager lockManager;

    /** Transaction manager. */
    private final TxManager txManager;

    /** Cluster service. */
    private final ClusterService clusterService;

    /** The listener tries to clarify the holder transaction state to avoid permanent locking. */
    private final EventListener<LockEventParameters> lockConflictListener;

    /**
     * The constructor.
     *
     * @param lockManager Lock manager.
     * @param txManager Transaction manager.
     * @param clusterService Cluster service.
     */
    public TxRecoveryManager(
            LockManager lockManager,
            TxManager txManager,
            ClusterService clusterService
    ) {
        this.lockManager = lockManager;
        this.txManager = txManager;
        this.clusterService = clusterService;

        lockConflictListener = (parameters, exception) -> {
            TxStateMeta txSate = txManager.stateMeta(parameters.getLockHolderTx());

            if (txSate == null || clusterService.topologyService().getById(txSate.txCoordinatorId()) == null) {
                LOG.info("Conflict was found, and the coordinator of the transaction that holds a lock is not available "
                                + "[lockHolderTx={}, lockAcquirerTx={}].",
                        parameters.getLockHolderTx(),
                        parameters.getLockAcquirerTx()
                );

                //TODO:IGNITE-20735 Obviously, the commit partition node does not have to be at the local node.
                ClusterNode cmpPartNode = clusterService.topologyService().localMember();

                clusterService.messagingService().weakSend(cmpPartNode, FACTORY.txRecoveryMessage()
                        .txId(parameters.getLockHolderTx())
                        .build());
            }

            return CompletableFuture.completedFuture(false);
        };
    }

    @Override
    public void start() {
        lockManager.listen(LockEvent.CONFLICT_FOUND, lockConflictListener);

        clusterService.messagingService().addMessageHandler(TxMessageGroup.class, (msg, sender, correlationId) -> {
            if (msg instanceof TxRecoveryMessage) {
                var txRecoveryMsg = (TxRecoveryMessage) msg;

                TxStateMeta txState = txManager.stateMeta(txRecoveryMsg.txId());

                if (txState == null || !TxState.isFinalState(txState.txState())) {
                    assert txState == null || clusterService.topologyService().getById(txState.txCoordinatorId()) == null :
                            "Transaction coordinator is alive [tx=" + txRecoveryMsg.txId() + ", crd=" + txState.txCoordinatorId() + ']';

                    LOG.warn("Transaction have to be aborted [tx={}].", txRecoveryMsg.txId());
                }
            }
        });
    }

    @Override
    public void beforeNodeStop() {
        lockManager.removeListener(LockEvent.CONFLICT_FOUND, lockConflictListener);
    }

    @Override
    public void stop() throws Exception {

    }
}
