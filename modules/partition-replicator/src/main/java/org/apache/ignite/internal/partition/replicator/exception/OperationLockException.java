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

package org.apache.ignite.internal.partition.replicator.exception;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import org.apache.ignite.internal.partition.replicator.network.replication.RequestType;
import org.apache.ignite.internal.tx.LockException;

/**
 * Exception that wraps one of root cause {@link LockException} but provides an operation type that failed to acquire, release or downgrade
 * a lock.
 */
public class OperationLockException extends LockException {
    /**
     * Constructor.
     *
     * @param requestOperationType Request's operation type that faced with failure to acquire, release or downgrade a lock.
     * @param cause Detail cause of the failure.
     */
    public OperationLockException(RequestType requestOperationType, LockException cause) {
        super(
                cause.code(),
                format("Lock acquiring failed during request handling [requestOperationType={}].", requestOperationType),
                cause
        );
    }
}
