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

package org.apache.ignite.internal.metastorage.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.close.ManuallyCloseable;
import org.apache.ignite.internal.metastorage.command.MetaStorageCommandsFactory;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Class containing some useful fields shared between Meta Storage classes.
 */
class MetaStorageServiceContext implements ManuallyCloseable {
    /** Meta storage raft group service. */
    private final RaftGroupService raftService;

    /** Commands factory. */
    private final MetaStorageCommandsFactory commandsFactory;

    /** Thread pool used to dispatch Publishers of range commands. */
    private final ExecutorService executorService;

    private final IgniteSpinBusyLock busyLock;

    MetaStorageServiceContext(
            RaftGroupService raftService,
            MetaStorageCommandsFactory commandsFactory,
            ExecutorService executorService,
            IgniteSpinBusyLock busyLock
    ) {
        this.raftService = raftService;
        this.commandsFactory = commandsFactory;
        this.executorService = executorService;
        this.busyLock = busyLock;
    }

    RaftGroupService raftService() {
        return raftService;
    }

    MetaStorageCommandsFactory commandsFactory() {
        return commandsFactory;
    }

    ExecutorService executorService() {
        return executorService;
    }

    IgniteSpinBusyLock busyLock() {
        return busyLock;
    }

    @Override
    public void close() {
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);

        raftService.shutdown();
    }
}
