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

import static org.apache.ignite.lang.ErrorGroups.Replicator.REPLICA_MISS_ERR;

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.jetbrains.annotations.Nullable;

/**
 * Unchecked exception that is thrown when a replica is not the current primary replica.
 */
public class PrimaryReplicaMissException extends IgniteInternalException {
    private static final long serialVersionUID = 8755220779942651494L;

    /**
     * The constructor.
     *
     * @param expectedLeaseholder Expected leaseholder.
     * @param currentLeaseholder Current leaseholder.
     */
    public PrimaryReplicaMissException(String expectedLeaseholder, @Nullable String currentLeaseholder) {
        super(
                REPLICA_MISS_ERR,
                "The primary replica has changed [expectedLeaseholder={}, currentLeaseholder={}]",
                expectedLeaseholder, currentLeaseholder
        );
    }

    /**
     * Constructor.
     *
     * @param expectedLeaseholder Expected leaseholder.
     * @param currentLeaseholder Current leaseholder, {@code null} if absent.
     * @param expectedPrimaryReplicaTerm Expected term from.
     * @param currentPrimaryReplicaTerm Current raft term, {@code null} if absent.
     * @param cause Cause exception, {@code null} if absent.
     */
    public PrimaryReplicaMissException(
            String expectedLeaseholder,
            @Nullable String currentLeaseholder,
            long expectedPrimaryReplicaTerm,
            @Nullable Long currentPrimaryReplicaTerm,
            @Nullable Throwable cause
    ) {
        super(
                REPLICA_MISS_ERR,
                "The primary replica has changed "
                        + "[expectedLeaseholder={}, currentLeaseholder={}, expectedPrimaryReplicaTerm={}, currentPrimaryReplicaTerm={}]",
                cause,
                expectedLeaseholder, currentLeaseholder, expectedPrimaryReplicaTerm, currentPrimaryReplicaTerm
        );
    }
}
