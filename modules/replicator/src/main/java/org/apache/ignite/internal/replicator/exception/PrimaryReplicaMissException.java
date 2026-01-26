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

package org.apache.ignite.internal.replicator.exception;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_MISS_ERR;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.tx.RetriableTransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Unchecked exception that is thrown when a replica is not the current primary replica.
 */
public class PrimaryReplicaMissException extends IgniteInternalException implements ExpectedReplicationException,
        RetriableTransactionException {
    private static final long serialVersionUID = 8755220779942651494L;

    /**
     * Constructor.
     *
     * @param txId Transaction id.
     * @param expectedEnlistmentConsistencyToken Expected enlistment consistency token, {@code null} if absent.
     * @param currentEnlistmentConsistencyToken Current enlistment consistency token, {@code null} if absent.
     * @param replicationGroupId Replication group id, {@code null} if absent.
     */
    public PrimaryReplicaMissException(
            UUID txId, 
            Long expectedEnlistmentConsistencyToken, 
            Long currentEnlistmentConsistencyToken,
            @Nullable ReplicationGroupId replicationGroupId
    ) {
        super(REPLICA_MISS_ERR, format("The primary replica has changed [txId={}, expectedEnlistmentConsistencyToken={}, "
                + "currentEnlistmentConsistencyToken={}, replicationGroupId={}].", 
                txId, expectedEnlistmentConsistencyToken, currentEnlistmentConsistencyToken, replicationGroupId));
    }

    /**
     * Constructor.
     *
     * @param expectedLeaseholderName Expected leaseholder name.
     * @param currentLeaseholderName Current leaseholder name, {@code null} if absent.
     * @param expectedLeaseholderId Expected leaseholder id.
     * @param currentLeaseholderId Current leaseholder id, {@code null} if absent.
     * @param expectedEnlistmentConsistencyToken Expected enlistment consistency token, {@code null} if absent.
     * @param currentEnlistmentConsistencyToken Current enlistment consistency token, {@code null} if absent.
     * @param replicationGroupId Replication group id, {@code null} if absent.
     * @param cause Cause exception, {@code null} if absent.
     */
    public PrimaryReplicaMissException(
            String expectedLeaseholderName,
            @Nullable String currentLeaseholderName,
            UUID expectedLeaseholderId,
            @Nullable UUID currentLeaseholderId,
            @Nullable Long expectedEnlistmentConsistencyToken,
            @Nullable Long currentEnlistmentConsistencyToken,
            @Nullable ReplicationGroupId replicationGroupId,
            @Nullable Throwable cause
    ) {
        super(
                REPLICA_MISS_ERR,
                "The primary replica has changed "
                        + "[expectedLeaseholderName={}, currentLeaseholderName={}, expectedLeaseholderId={}, currentLeaseholderId={},"
                        + " expectedEnlistmentConsistencyToken={}, currentEnlistmentConsistencyToken={}, replicationGroupId={}]",
                cause,
                expectedLeaseholderName,
                currentLeaseholderName,
                expectedLeaseholderId,
                currentLeaseholderId,
                expectedEnlistmentConsistencyToken,
                currentEnlistmentConsistencyToken,
                replicationGroupId
        );
    }
}
