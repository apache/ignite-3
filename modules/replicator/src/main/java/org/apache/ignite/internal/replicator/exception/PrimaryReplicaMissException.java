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

import java.util.UUID;
import org.apache.ignite.lang.ErrorGroups.Replicator;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * Unchecked exception that is thrown when a replica is not the current primary replica.
 */
public class PrimaryReplicaMissException extends IgniteInternalException {
    /**
     * The constructor.
     *
     * @param expectedPrimaryReplicaTerm Expected term from.
     * @param currentPrimaryReplicaTerm Current raft term.
     */
    public PrimaryReplicaMissException(Long expectedPrimaryReplicaTerm, Long currentPrimaryReplicaTerm) {
        this(expectedPrimaryReplicaTerm, currentPrimaryReplicaTerm, null);
    }

    /**
     * The constructor.
     *
     * @param expectedPrimaryReplicaTerm Expected term from.
     * @param currentPrimaryReplicaTerm Current raft term.
     * @param cause Cause exception.
     */
    public PrimaryReplicaMissException(Long expectedPrimaryReplicaTerm, Long currentPrimaryReplicaTerm, Throwable cause) {
        super(Replicator.REPLICA_MISS_ERR,
                IgniteStringFormatter.format(
                        "The primary replica has changed because the term has been changed "
                                + "[expectedPrimaryReplicaTerm={}, currentPrimaryReplicaTerm={}]",
                        expectedPrimaryReplicaTerm, currentPrimaryReplicaTerm
                ),
                cause);
    }

    /**
     * The constructor is used for creating an exception instance that is thrown from a remote server.
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
