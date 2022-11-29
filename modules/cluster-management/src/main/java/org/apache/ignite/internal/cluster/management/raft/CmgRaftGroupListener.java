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

package org.apache.ignite.internal.cluster.management.raft;

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.raft.commands.ClusterNodeMessage;
import org.apache.ignite.internal.cluster.management.raft.commands.InitCmgStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.NodesLeaveCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadLogicalTopologyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadStateCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.LogicalTopologyResponse;
import org.apache.ignite.internal.cluster.management.raft.responses.ValidationErrorResponse;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * {@link RaftGroupListener} implementation for the CMG.
 */
public class CmgRaftGroupListener implements RaftGroupListener {
    private static final IgniteLogger LOG = Loggers.forClass(CmgRaftGroupListener.class);

    private final RaftStorageManager storage;

    private final LogicalTopology logicalTopology;

    private final ValidationManager validationManager;

    /**
     * Creates a new instance.
     */
    public CmgRaftGroupListener(ClusterStateStorage storage, LogicalTopology logicalTopology) {
        this.storage = new RaftStorageManager(storage);
        this.logicalTopology = logicalTopology;
        this.validationManager = new ValidationManager(this.storage, this.logicalTopology);
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            ReadCommand command = clo.command();

            if (command instanceof ReadStateCommand) {
                clo.result(storage.getClusterState());
            } else if (command instanceof ReadLogicalTopologyCommand) {
                clo.result(new LogicalTopologyResponse(logicalTopology.getLogicalTopology()));
            }
        }
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            WriteCommand command = clo.command();

            if (command instanceof InitCmgStateCommand) {
                Serializable response = initCmgState((InitCmgStateCommand) command);

                clo.result(response);
            } else if (command instanceof JoinRequestCommand) {
                ValidationResult response = validateNode((JoinRequestCommand) command);

                clo.result(response.isValid() ? null : new ValidationErrorResponse(response.errorDescription()));
            } else if (command instanceof JoinReadyCommand) {
                Serializable response = completeValidation((JoinReadyCommand) command);

                clo.result(response);
            } else if (command instanceof NodesLeaveCommand) {
                removeNodesFromLogicalTopology((NodesLeaveCommand) command);

                clo.result(null);
            }
        }
    }

    @Nullable
    private Serializable initCmgState(InitCmgStateCommand command) {
        ClusterState state = storage.getClusterState();

        if (state == null) {
            storage.putClusterState(command.clusterState());

            return command.clusterState();
        } else {
            ValidationResult validationResult = ValidationManager.validateState(
                    state,
                    command.node().asClusterNode(),
                    command.clusterState()
            );

            return validationResult.isValid() ? state : new ValidationErrorResponse(validationResult.errorDescription());
        }
    }

    private ValidationResult validateNode(JoinRequestCommand command) {
        return validationManager.validateNode(
                storage.getClusterState(),
                command.node().asClusterNode(),
                command.igniteVersion(),
                command.clusterTag()
        );
    }

    @Nullable
    private Serializable completeValidation(JoinReadyCommand command) {
        ClusterNode node = command.node().asClusterNode();

        if (validationManager.isNodeValidated(node)) {
            logicalTopology.putLogicalTopologyNode(node);

            LOG.info("Node added to the logical topology [node={}]", node.name());

            validationManager.completeValidation(node);

            return null;
        } else {
            return new ValidationErrorResponse(String.format("Node \"%s\" has not yet passed the validation step", node));
        }
    }

    private void removeNodesFromLogicalTopology(NodesLeaveCommand command) {
        Set<ClusterNode> nodes = command.nodes().stream().map(ClusterNodeMessage::asClusterNode).collect(Collectors.toSet());

        logicalTopology.removeLogicalTopologyNodes(nodes);

        if (LOG.isInfoEnabled()) {
            LOG.info("Nodes removed from the logical topology [nodes={}]", nodes.stream().map(ClusterNode::name).collect(toList()));
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
            LOG.debug("Failed to restore snapshot [path={}]", path, e);

            return false;
        }
    }

    @Override
    public void onShutdown() {
        // Raft storage lifecycle is managed by outside components.
        validationManager.close();
    }

    @TestOnly
    public RaftStorageManager storage() {
        return storage;
    }
}
