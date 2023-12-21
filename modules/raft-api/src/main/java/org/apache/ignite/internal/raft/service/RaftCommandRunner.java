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

package org.apache.ignite.internal.raft.service;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.raft.Command;

/**
 * Raft client that is only able to run commands on a Raft group. For a more potent interface, take a look at {@link RaftGroupService}.
 *
 * @see RaftGroupService
 */
public interface RaftCommandRunner {
    /**
     * Runs a command on a replication group leader.
     *
     * <p>Read commands always see up to date data.
     *
     * @param cmd The command.
     * @param <R> Execution result type.
     * @return A future with the execution result.
     */
    <R> CompletableFuture<R> run(Command cmd);
}
