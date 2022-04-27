/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.cluster.management.raft;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.NodesLeaveCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadLogicalTopologyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.WriteStateCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.LogicalTopologyResponse;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.Nullable;

/**
 * {@link RaftGroupListener} implementation for the CMG.
 */
public class CmgRaftGroupListener implements RaftGroupListener {
    private static final IgniteLogger log = IgniteLogger.forClass(CmgRaftGroupListener.class);

    private final RaftStorageManager storage;

    public CmgRaftGroupListener(ClusterStateStorage storage) {
        this.storage = new RaftStorageManager(storage);
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            ReadCommand command = clo.command();

            if (command instanceof ReadStateCommand) {
                clo.result(storage.getClusterState());
            } else if (command instanceof ReadLogicalTopologyCommand) {
                clo.result(new LogicalTopologyResponse(storage.getLogicalTopology()));
            }
        }
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            WriteCommand command = clo.command();

            if (command instanceof WriteStateCommand) {
                storage.putClusterState(((WriteStateCommand) command).clusterState());
            } else if (command instanceof JoinRequestCommand) {
                // TODO: perform validation https://issues.apache.org/jira/browse/IGNITE-16717
            } else if (command instanceof JoinReadyCommand) {
                storage.putLogicalTopologyNode(((JoinReadyCommand) command).node());
            } else if (command instanceof NodesLeaveCommand) {
                storage.removeLogicalTopologyNodes(((NodesLeaveCommand) command).nodes());
            }

            clo.result(null);
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storage.snapshot(path)
                .whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        try {
            storage.restoreSnapshot(path);

            return true;
        } catch (IgniteInternalException e) {
            log.error("Failed to restore snapshot at " + path, e);

            return false;
        }
    }

    @Override
    public void onShutdown() {
        // no-op
    }

    @Override
    public @Nullable CompletableFuture<Void> onBeforeApply(Command command) {
        return null;
    }
}
