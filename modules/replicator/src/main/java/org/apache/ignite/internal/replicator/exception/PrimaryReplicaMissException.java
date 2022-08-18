/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.UUID;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.network.NetworkAddress;

/**
 * The exception is thrown when the replica is not the current primary replica.
 */
public class PrimaryReplicaMissException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param requestAddress Primary replica address from request.
     * @param localAddress Local primary replica address.
     * @param requestTerm Raft term from request.
     * @param localTerm Local raft term.
     */
    public PrimaryReplicaMissException(NetworkAddress requestAddress, NetworkAddress localAddress, Long requestTerm, Long localTerm) {
        this(requestAddress, localAddress, requestTerm, localTerm, null);
    }

    /**
     * The constructor.
     *
     * @param requestAddress Primary replica address from request.
     * @param localAddress Local primary replica address.
     * @param requestTerm Raft term from request.
     * @param localTerm Local raft term.
     * @param cause Cause exception.
     */
    public PrimaryReplicaMissException(NetworkAddress requestAddress, NetworkAddress localAddress, Long requestTerm, Long localTerm,
            Throwable cause) {
        super(Replicator.REPLICA_MISS_ERR, IgniteStringFormatter.format(
                "The replica is not the current primary replica [requestAddress={}, "
                + "localAddress={}, requestTerm={}, localTerm={}]", requestAddress, localAddress, requestTerm, localTerm),
                cause);
    }

    /**
     * The constructor is used for create an exception instance that has thrown in remote server.
     *
     * @param traceId Trace id.
     * @param code Error code.
     * @param message Error message.
     * @param cause Cause exception.
     */
    public PrimaryReplicaMissException(UUID traceId, int code, String message, Throwable cause) {
        super(traceId, code, message, cause);
    }
}
