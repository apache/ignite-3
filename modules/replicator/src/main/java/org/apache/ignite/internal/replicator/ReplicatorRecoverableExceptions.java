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

package org.apache.ignite.internal.replicator;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.raft.GroupOverloadedException;
import org.apache.ignite.internal.replicator.exception.PrimaryReplicaMissException;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;
import org.apache.ignite.internal.util.ExceptionUtils;

/**
 * Class holding the set of recoverable exceptions for the partition replicator.
 */
public class ReplicatorRecoverableExceptions {
    private static final Set<Class<? extends Throwable>> RECOVERABLE = Set.of(
            TimeoutException.class,
            IOException.class,
            UnresolvableConsistentIdException.class,
            ReplicationException.class,
            ReplicaUnavailableException.class,
            ReplicationTimeoutException.class,
            PrimaryReplicaMissException.class,
            GroupOverloadedException.class
    );

    /**
     * Check if the provided exception is recoverable. A recoverable transaction is the one that we can send a 'retry' request for.
     *
     * @param throwable Exception to test.
     * @return {@code true} if recoverable, {@code false} otherwise.
     */
    public static boolean isRecoverable(Throwable throwable) {
        if (throwable == null) {
            return false;
        }

        Throwable candidate = ExceptionUtils.unwrapRootCause(throwable);

        for (Class<? extends Throwable> recoverableClass : RECOVERABLE) {
            if (recoverableClass.isAssignableFrom(candidate.getClass())) {
                return true;
            }
        }

        return false;
    }
}
