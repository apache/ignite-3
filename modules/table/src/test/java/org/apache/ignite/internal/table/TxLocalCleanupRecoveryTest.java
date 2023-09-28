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

package org.apache.ignite.internal.table;

import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_COMMON_ERR;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.message.ReplicaRequest;
import org.apache.ignite.internal.tx.message.TxCleanupReplicaRequest;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

/**
 * Durable cleanup test with successfull recovery after the fauilures.
 */
public class TxLocalCleanupRecoveryTest extends TxLocalTest {

    private AtomicInteger failureCounter;

    @BeforeEach
    void initTest() {
        // The value of 3 is less than the allowed number of cleanup retries.
        failureCounter = new AtomicInteger(3);
    }


    @Override
    protected MethodAnswer invokeOnMessagingMock(InvocationOnMock invocationOnMock) {
        ReplicaRequest request = invocationOnMock.getArgument(1);

        if (request instanceof TxCleanupReplicaRequest && failureCounter.getAndDecrement() > 0) {
            return new MethodAnswer(CompletableFuture.failedFuture(new ReplicationException(
                    REPLICA_COMMON_ERR,
                    "Test Tx Cleanup exception [replicaGroupId=" + request.groupId() + ']')));
        }
        // Otherwise use the parent stub.
        return super.invokeOnMessagingMock(invocationOnMock);
    }


    @Test
    @Override
    public void testDeleteUpsertCommit() throws TransactionException {
        failureCounter = new AtomicInteger(6);

        assertThrows(TransactionException.class, () -> deleteUpsert().commit());
    }
}
