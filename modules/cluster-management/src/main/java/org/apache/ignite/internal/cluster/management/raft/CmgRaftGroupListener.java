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
import static org.apache.ignite.internal.util.IgniteUtils.capacity;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.ClusterIdStore;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.MetaStorageInfo;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.raft.commands.ChangeMetaStorageInfoCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ClusterNodeMessage;
import org.apache.ignite.internal.cluster.management.raft.commands.InitCmgStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinReadyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.JoinRequestCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.NodesLeaveCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadLogicalTopologyCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadMetaStorageInfoCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadStateCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.ReadValidatedNodesCommand;
import org.apache.ignite.internal.cluster.management.raft.commands.UpdateClusterStateCommand;
import org.apache.ignite.internal.cluster.management.raft.responses.LogicalTopologyResponse;
import org.apache.ignite.internal.cluster.management.raft.responses.ValidationErrorResponse;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopology;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * {@link RaftGroupListener} implementation for the CMG.
 */
public class CmgRaftGroupListener implements RaftGroupListener {
    private static final IgniteLogger LOG = Loggers.forClass(CmgRaftGroupListener.class);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final ClusterStateStorageManager storageManager;

    private final LogicalTopology logicalTopology;

    private final ValidationManager validationManager;

    private final LongConsumer onLogicalTopologyChanged;

    private final ClusterIdStore clusterIdStore;

    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();

    /**
     * Creates a new instance.
     *
     * @param storageManager Storage where this listener local data will be stored.
     * @param logicalTopology Logical topology that will be updated by this listener.
     * @param validationManager Validator for cluster nodes.
     * @param onLogicalTopologyChanged Callback invoked (with the corresponding RAFT term) when logical topology gets changed.
     * @param clusterIdStore Used to store cluster ID when init command is executed.
     */
    public CmgRaftGroupListener(
            ClusterStateStorageManager storageManager,
            LogicalTopology logicalTopology,
            ValidationManager validationManager,
            LongConsumer onLogicalTopologyChanged,
            ClusterIdStore clusterIdStore
    ) {
        this.storageManager = storageManager;
        this.logicalTopology = logicalTopology;
        this.validationManager = validationManager;
        this.onLogicalTopologyChanged = onLogicalTopologyChanged;
        this.clusterIdStore = clusterIdStore;
    }

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        if (!busyLock.enterBusy()) {
            iterator.forEachRemaining(clo -> clo.result(new ShutdownException()));
        }

        try {
            onReadBusy(iterator);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void onReadBusy(Iterator<CommandClosure<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<ReadCommand> clo = iterator.next();

            ReadCommand command = clo.command();

            if (command instanceof ReadStateCommand) {
                clo.result(storageManager.getClusterState());
            } else if (command instanceof ReadLogicalTopologyCommand) {
                clo.result(new LogicalTopologyResponse(logicalTopology.getLogicalTopology()));
            } else if (command instanceof ReadValidatedNodesCommand) {
                clo.result(getValidatedNodes());
            } else if (command instanceof ReadMetaStorageInfoCommand) {
                clo.result(getMetaStorageInfo());
            }
        }
    }

    private HashSet<LogicalNode> getValidatedNodes() {
        List<LogicalNode> validatedNodes = storageManager.getValidatedNodes();
        Set<LogicalNode> logicalTopologyNodes = logicalTopology.getLogicalTopology().nodes();

        var result = new HashSet<LogicalNode>(capacity(validatedNodes.size() + logicalTopologyNodes.size()));

        result.addAll(validatedNodes);
        result.addAll(logicalTopologyNodes);

        return result;
    }

    private @Nullable MetaStorageInfo getMetaStorageInfo() {
        ClusterState clusterState = storageManager.getClusterState();
        if (clusterState == null) {
            return null;
        }

        return cmgMessagesFactory.metaStorageInfo()
                .metaStorageNodes(clusterState.metaStorageNodes())
                .metastorageRepairClusterId(storageManager.getMetastorageRepairClusterId())
                .metastorageRepairingConfigIndex(storageManager.getMetastorageRepairingConfigIndex())
                .build();
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        if (!busyLock.enterBusy()) {
            iterator.forEachRemaining(clo -> clo.result(new ShutdownException()));
        }

        try {
            onWriteBusy(iterator);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void onWriteBusy(Iterator<CommandClosure<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandClosure<WriteCommand> clo = iterator.next();

            WriteCommand command = clo.command();

            if (command instanceof InitCmgStateCommand) {
                Serializable response = initCmgState((InitCmgStateCommand) command);

                clo.result(response);
            } else if (command instanceof UpdateClusterStateCommand) {
                UpdateClusterStateCommand updateClusterStateCommand = (UpdateClusterStateCommand) command;
                storageManager.putClusterState(updateClusterStateCommand.clusterState());
                clo.result(null);
            } else if (command instanceof JoinRequestCommand) {
                ValidationResult response = validateNode((JoinRequestCommand) command);

                clo.result(response.isValid() ? null : new ValidationErrorResponse(response.errorDescription()));
            } else if (command instanceof JoinReadyCommand) {
                ValidationResult response = completeValidation((JoinReadyCommand) command);

                if (response.isValid()) {
                    // It is valid, the topology has been changed.
                    onLogicalTopologyChanged.accept(clo.term());
                }

                clo.result(response.isValid() ? null : new ValidationErrorResponse(response.errorDescription()));
            } else if (command instanceof NodesLeaveCommand) {
                removeNodesFromLogicalTopology((NodesLeaveCommand) command);

                onLogicalTopologyChanged.accept(clo.term());

                clo.result(null);
            } else if (command instanceof ChangeMetaStorageInfoCommand) {
                changeMetastorageNodes((ChangeMetaStorageInfoCommand) command);

                clo.result(null);
            }
        }
    }

    @Nullable
    private Serializable initCmgState(InitCmgStateCommand command) {
        ClusterState state = storageManager.getClusterState();

        if (state == null) {
            storageManager.putClusterState(command.clusterState());

            clusterIdStore.clusterId(command.clusterState().clusterTag().clusterId());

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
        ClusterNode node = command.node().asClusterNode();

        Optional<LogicalNode> previousVersion = logicalTopology.getLogicalTopology().nodes()
                .stream()
                .filter(n -> n.name().equals(node.name()))
                .findAny();

        if (previousVersion.isPresent()) {
            LogicalNode previousNode = previousVersion.get();

            if (previousNode.id().equals(node.id())) {
                return ValidationResult.successfulResult();
            } else {
                // Remove the previous node from the Logical Topology in case we haven't received the disappeared event yet.
                logicalTopology.removeNodes(Set.of(previousNode));
            }
        }

        LogicalNode logicalNode = new LogicalNode(
                node,
                command.node().userAttributes(),
                command.node().systemAttributes(),
                command.node().storageProfiles()
        );

        return validationManager.validateNode(storageManager.getClusterState(), logicalNode, command.igniteVersion(), command.clusterTag());
    }

    private ValidationResult completeValidation(JoinReadyCommand command) {
        ClusterNode node = command.node().asClusterNode();

        LogicalNode logicalNode = new LogicalNode(
                node,
                command.node().userAttributes(),
                command.node().systemAttributes(),
                command.node().storageProfiles()
        );

        if (validationManager.isNodeValidated(logicalNode)) {
            ValidationResult validationResponse = validationManager.completeValidation(logicalNode);

            if (validationResponse.isValid()) {
                logicalTopology.putNode(logicalNode);
            }

            return validationResponse;
        } else {
            return ValidationResult.errorResult(String.format("Node \"%s\" has not yet passed the validation step", node));
        }
    }

    private void removeNodesFromLogicalTopology(NodesLeaveCommand command) {
        Set<ClusterNode> nodes = command.nodes().stream().map(ClusterNodeMessage::asClusterNode).collect(Collectors.toSet());

        // Nodes will be removed from a topology, so it is safe to set nodeAttributes to the default value
        Set<LogicalNode> logicalNodes = nodes.stream()
                .map(n -> new LogicalNode(n, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyList()))
                .collect(Collectors.toSet());

        logicalTopology.removeNodes(logicalNodes);
        validationManager.removeValidatedNodes(logicalNodes);

        if (LOG.isInfoEnabled()) {
            LOG.info("Nodes removed from the logical topology [nodes={}]", nodes.stream().map(ClusterNode::name).collect(toList()));
        }
    }

    private void changeMetastorageNodes(ChangeMetaStorageInfoCommand command) {
        ClusterState existingState = storageManager.getClusterState();

        assert existingState != null : "Cluster state was not initialized when got " + command;

        ClusterState newState = cmgMessagesFactory.clusterState()
                .cmgNodes(Set.copyOf(existingState.cmgNodes()))
                .metaStorageNodes(Set.copyOf(command.metaStorageNodes()))
                .version(existingState.version())
                .clusterTag(existingState.clusterTag())
                .initialClusterConfiguration(existingState.initialClusterConfiguration())
                .formerClusterIds(existingState.formerClusterIds())
                .build();

        storageManager.putClusterState(newState);

        assert (command.metastorageRepairClusterId() == null) == (command.metastorageRepairingConfigIndex() == null)
                : "Repair-related properties must either all be present or all be absent [command=" + command + "]";
        if (command.metastorageRepairClusterId() != null) {
            storageManager.saveMetastorageRepairInfo(command.metastorageRepairClusterId(), command.metastorageRepairingConfigIndex());
        }
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        storageManager.snapshot(path)
                .whenComplete((unused, throwable) -> doneClo.accept(throwable));
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        try {
            storageManager.restoreSnapshot(path);

            logicalTopology.fireTopologyLeap();

            return true;
        } catch (IgniteInternalException e) {
            LOG.error("Failed to restore snapshot [path={}]", path, e);

            return false;
        }
    }

    @Override
    public void onShutdown() {
        busyLock.block();

        // Raft storage lifecycle is managed by outside components.
    }

    @TestOnly
    public ClusterStateStorageManager storageManager() {
        return storageManager;
    }
}
