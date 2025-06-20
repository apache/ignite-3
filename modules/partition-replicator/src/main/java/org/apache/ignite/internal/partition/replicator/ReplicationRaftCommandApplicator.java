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

package org.apache.ignite.internal.partition.replicator;

import static org.apache.ignite.internal.util.ExceptionUtils.unwrapRootCause;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.internal.raft.Command;
import org.apache.ignite.internal.raft.GroupOverloadedException;
import org.apache.ignite.internal.raft.service.RaftCommandRunner;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.replicator.exception.ReplicationTimeoutException;

/**
 * Applies Raft commands adding error handling specific to replication protocol.
 */
public class ReplicationRaftCommandApplicator {
    private final RaftCommandRunner raftCommandRunner;
    private final ReplicationGroupId replicationGroupId;

    /** Constructor. */
    public ReplicationRaftCommandApplicator(RaftCommandRunner raftCommandRunner, ReplicationGroupId replicationGroupId) {
        this.raftCommandRunner = raftCommandRunner;
        this.replicationGroupId = replicationGroupId;
    }

    /**
     * Executes a command and handles exceptions. A result future can be finished with exception by following rules:
     * <ul>
     *     <li>If RAFT command cannot finish due to timeout, the future finished with {@link ReplicationTimeoutException}.</li>
     *     <li>If RAFT command finish with a runtime exception, the exception is moved to the result future.</li>
     *     <li>If RAFT command finish with any other exception, the future finished with {@link ReplicationException}.
     *     The original exception is set as cause.</li>
     * </ul>
     *
     * @param command Raft command.
     * @return Raft future or raft decorated future with command that was processed.
     */
    // TODO https://issues.apache.org/jira/browse/IGNITE-22522 Perhaps, this method is not needed and can be removed in the future
    public CompletableFuture<Object> applyCommandWithExceptionHandling(Command command) {
        CompletableFuture<Object> resultFuture = new CompletableFuture<>();

        applyCommandWithExceptionHandling(command, resultFuture);

        return resultFuture.exceptionally(throwable -> {
            if (throwable instanceof TimeoutException) {
                throw new ReplicationTimeoutException(replicationGroupId);
            } else if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            } else {
                throw new ReplicationException(replicationGroupId, throwable);
            }
        });
    }

    private void applyCommandWithExceptionHandling(Command command, CompletableFuture<Object> resultFuture) {
        raftCommandRunner.run(command).whenComplete((res, ex) -> {
            if (ex != null) {
                Throwable cause = unwrapRootCause(ex);

                if (cause instanceof GroupOverloadedException) {
                    // Retry on overload. There is a delay on raft client side, so we can retry immediately.
                    applyCommandWithExceptionHandling(command, resultFuture);
                } else {
                    resultFuture.completeExceptionally(ex);
                }
            } else {
                resultFuture.complete(res);
            }
        });
    }

    /**
     * Executes the given {@code command} on the corresponding replication group.
     *
     * @param command The command to be executed.
     * @return A future with the execution result.
     */
    public CompletableFuture<?> applyCommand(Command command) {
        return raftCommandRunner.run(command);
    }
}
