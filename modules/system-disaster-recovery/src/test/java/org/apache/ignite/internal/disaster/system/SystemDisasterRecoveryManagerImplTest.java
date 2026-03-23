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

package org.apache.ignite.internal.disaster.system;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.cluster.management.ClusterTag.clusterTag;
import static org.apache.ignite.internal.cluster.management.ClusterTag.randomClusterTag;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.uuidToBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.ClusterStatePersistentSerializer;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.SuccessResponseMessage;
import org.apache.ignite.internal.disaster.system.exception.ClusterResetException;
import org.apache.ignite.internal.disaster.system.exception.MigrateException;
import org.apache.ignite.internal.disaster.system.message.BecomeMetastorageLeaderMessage;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessageBuilder;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairRequest;
import org.apache.ignite.internal.disaster.system.message.StartMetastorageRepairResponse;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessageGroup;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.impl.MetastorageGroupMaintenance;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ConstantClusterIdSupplier;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.raft.IndexWithTerm;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class SystemDisasterRecoveryManagerImplTest extends BaseIgniteAbstractTest {
    private static final String CLUSTER_NAME = "cluster";

    private static final ByteArray INIT_CONFIG_APPLIED_VAULT_KEY = new ByteArray("systemRecovery.initConfigApplied");
    private static final ByteArray CLUSTER_STATE_VAULT_KEY = new ByteArray("systemRecovery.clusterState");
    private static final ByteArray RESET_CLUSTER_MESSAGE_VAULT_KEY = new ByteArray("systemRecovery.resetClusterMessage");
    private static final ByteArray WITNESSED_METASTORAGE_REPAIR_CLUSTER_ID_VAULT_KEY
            = new ByteArray("systemRecovery.witnessedMetastorageRepairClusterId");

    private static final String INITIAL_CONFIGURATION = "initial-config";

    @WorkDirectory
    private Path workDir;

    private static final String thisNodeName = "node1";

    @Mock
    private TopologyService topologyService;

    @Mock
    private MessagingService messagingService;

    private VaultManager vaultManager;

    @Mock
    private ServerRestarter restarter;

    @Mock
    private MetastorageGroupMaintenance metastorageMaintenance;

    private SystemDisasterRecoveryManagerImpl manager;

    private final ComponentContext componentContext = new ComponentContext();

    private final InternalClusterNode thisNode = new ClusterNodeImpl(randomUUID(), thisNodeName, new NetworkAddress("host", 1001));

    private final InternalClusterNode node2 = new ClusterNodeImpl(randomUUID(), "node2", new NetworkAddress("host", 1002));
    private final InternalClusterNode node3 = new ClusterNodeImpl(randomUUID(), "node3", new NetworkAddress("host", 1003));
    private final InternalClusterNode node4 = new ClusterNodeImpl(randomUUID(), "node4", new NetworkAddress("host", 1004));
    private final InternalClusterNode node5 = new ClusterNodeImpl(randomUUID(), "node5", new NetworkAddress("host", 1005));

    private final UUID clusterId = new UUID(1, 2);

    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();
    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();

    private final ClusterState usualClusterState = cmgMessagesFactory.clusterState()
            .cmgNodes(Set.of(thisNodeName))
            .metaStorageNodes(Set.of(node2.name()))
            .version(IgniteProductVersion.CURRENT_VERSION.toString())
            .clusterTag(randomClusterTag(cmgMessagesFactory, CLUSTER_NAME))
            .initialClusterConfiguration(INITIAL_CONFIGURATION)
            .build();

    private final SuccessResponseMessage successResponseMessage = cmgMessagesFactory.successResponseMessage().build();

    @BeforeEach
    void init() {
        vaultManager = spy(new VaultManager(new PersistentVaultService(workDir.resolve("vault"))));
        assertThat(vaultManager.startAsync(componentContext), willCompleteSuccessfully());

        lenient().when(messagingService.respond(any(InternalClusterNode.class), any(NetworkMessage.class), anyLong()))
                .thenReturn(nullCompletedFuture());

        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);

        lenient().when(cmgManager.clusterState()).thenReturn(completedFuture(usualClusterState));

        manager = new SystemDisasterRecoveryManagerImpl(
                thisNodeName,
                topologyService,
                messagingService,
                vaultManager,
                restarter,
                metastorageMaintenance,
                cmgManager,
                new ConstantClusterIdSupplier(clusterId)
        );
        assertThat(manager.startAsync(componentContext), willCompleteSuccessfully());
    }

    @AfterEach
    void cleanup() {
        assertThat(vaultManager.stopAsync(), willCompleteSuccessfully());
        assertThat(manager.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void marksInitConfigApplied() {
        manager.markInitConfigApplied();

        VaultEntry entry = vaultManager.get(INIT_CONFIG_APPLIED_VAULT_KEY);
        assertThat(entry, is(notNullValue()));
        assertThat(entry.value(), is(notNullValue()));
    }

    @Test
    void savesClusterState() {
        manager.saveClusterState(usualClusterState);

        VaultEntry entry = vaultManager.get(CLUSTER_STATE_VAULT_KEY);
        assertThat(entry, is(notNullValue()));

        ClusterState savedState = VersionedSerialization.fromBytes(entry.value(), ClusterStatePersistentSerializer.INSTANCE);
        assertThat(savedState, is(equalTo(usualClusterState)));
    }

    @ParameterizedTest
    @EnumSource(ResetCluster.class)
    void resetClusterRejectsDuplicateNodeNames(ResetCluster action) {
        ClusterResetException ex = assertWillThrow(
                action.resetCluster(manager, List.of(thisNodeName, thisNodeName)),
                ClusterResetException.class
        );
        assertThat(ex.getMessage(), is("New CMG node names have repetitions: [node1, node1]."));
    }

    @ParameterizedTest
    @EnumSource(ResetCluster.class)
    void resetClusterRequiresThisNodeToBeNewCmg(ResetCluster action) {
        ClusterResetException ex = assertWillThrow(action.resetCluster(manager, List.of("abc")), ClusterResetException.class);
        assertThat(ex.getMessage(), is("Current node is not contained in the new CMG, so it cannot conduct a cluster reset."));
    }

    @ParameterizedTest
    @EnumSource(ResetCluster.class)
    void resetClusterRequiresNewCmgNodesToBeOnline(ResetCluster action) {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode));

        ClusterResetException ex = assertWillThrow(action.resetCluster(manager, List.of(thisNodeName, "abc")), ClusterResetException.class);
        assertThat(ex.getMessage(), is("Some of proposed CMG nodes are not online: [abc]."));
    }

    @Test
    void resetClusterRepairingMgUsesCurrentCmgNodesIfNotSpecified() {
        int replicationFactor = 1;

        ArgumentCaptor<ResetClusterMessage> messageCaptor = ArgumentCaptor.forClass(ResetClusterMessage.class);

        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node3, node4));
        prepareNodeStateForClusterReset();

        when(messagingService.invoke(any(InternalClusterNode.class), any(ResetClusterMessage.class), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));

        CompletableFuture<Void> future = manager.resetClusterRepairingMetastorage(null, replicationFactor);
        assertThat(future, willCompleteSuccessfully());

        verify(messagingService).invoke(eq(thisNode), messageCaptor.capture(), anyLong());

        assertThat(messageCaptor.getValue().newCmgNodes(), is(usualClusterState.cmgNodes()));
    }

    @ParameterizedTest
    @EnumSource(ResetCluster.class)
    void resetClusterRequiresClusterState(ResetCluster action) {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode));
        markInitConfigApplied();

        ClusterResetException ex = assertWillThrow(action.resetCluster(manager, List.of(thisNodeName)), ClusterResetException.class);
        assertThat(ex.getMessage(), is("Node does not have cluster state."));
    }

    @ParameterizedTest
    @EnumSource(ResetCluster.class)
    void resetClusterRequiresInitConfigToBeApplied(ResetCluster action) {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode));
        putClusterState();

        ClusterResetException ex = assertWillThrow(action.resetCluster(manager, List.of(thisNodeName)), ClusterResetException.class);
        assertThat(ex.getMessage(), is("Initial configuration is not applied, so the node cannot serve as a cluster reset conductor."));
    }

    private void putClusterState() {
        vaultManager.put(
                CLUSTER_STATE_VAULT_KEY,
                VersionedSerialization.toBytes(usualClusterState, ClusterStatePersistentSerializer.INSTANCE)
        );
    }

    private void markInitConfigApplied() {
        vaultManager.put(INIT_CONFIG_APPLIED_VAULT_KEY, BYTE_EMPTY_ARRAY);
    }

    @ParameterizedTest
    @EnumSource(ResetCluster.class)
    void resetClusterSendsMessages(ResetCluster action) {
        ArgumentCaptor<ResetClusterMessage> messageCaptor = ArgumentCaptor.forClass(ResetClusterMessage.class);

        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node3, node4));
        prepareNodeStateForClusterReset();

        when(messagingService.invoke(any(InternalClusterNode.class), any(ResetClusterMessage.class), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));

        CompletableFuture<Void> future = action.resetCluster(manager, List.of(thisNodeName, node3.name()));
        assertThat(future, willCompleteSuccessfully());

        verify(messagingService).invoke(eq(thisNode), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToSelf = messageCaptor.getValue();
        assertThatResetClusterMessageIsAsExpected(messageToSelf, action.mgRepair());

        verify(messagingService).invoke(eq(node3), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToOtherNewCmgNode = messageCaptor.getValue();
        assertThatResetClusterMessageIsAsExpected(messageToOtherNewCmgNode, action.mgRepair());

        verify(messagingService).invoke(eq(node4), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToOtherNonCmgNode = messageCaptor.getValue();
        assertThatResetClusterMessageIsAsExpected(messageToOtherNonCmgNode, action.mgRepair());

        assertThat(messageToSelf.clusterId(), is(messageToOtherNewCmgNode.clusterId()));
        assertThat(messageToSelf.clusterId(), is(messageToOtherNonCmgNode.clusterId()));
    }

    private void prepareNodeStateForClusterReset() {
        markInitConfigApplied();
        putClusterState();
    }

    private void assertThatResetClusterMessageIsAsExpected(ResetClusterMessage message, boolean mgRepair) {
        assertThatResetClusterMessageContentIsAsExpected(message, mgRepair);
    }

    private void assertThatResetClusterMessageContentIsAsExpected(@Nullable ResetClusterMessage message, boolean mgRepair) {
        assertThat(message, is(notNullValue()));
        assertThat(message.newCmgNodes(), containsInAnyOrder(thisNodeName, node3.name()));
        assertThat(message.currentMetaStorageNodes(), is(usualClusterState.metaStorageNodes()));
        assertThat(message.clusterName(), is(CLUSTER_NAME));
        assertThat(message.clusterId(), is(not(usualClusterState.clusterTag().clusterId())));
        assertThat(message.formerClusterIds(), contains(usualClusterState.clusterTag().clusterId()));
        assertThat(message.initialClusterConfiguration(), is(INITIAL_CONFIGURATION));
        if (mgRepair) {
            assertThat(message.metastorageReplicationFactor(), is(1));
            assertThat(message.conductor(), is(thisNodeName));
            assertThat(message.participatingNodes(), containsInAnyOrder(thisNodeName, node3.name(), node4.name()));
        }
    }

    @ParameterizedTest
    @EnumSource(ResetCluster.class)
    void resetClusterInitiatesRestartOnSuccess(ResetCluster action) {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node3));
        prepareNodeStateForClusterReset();

        when(messagingService.invoke(any(InternalClusterNode.class), any(ResetClusterMessage.class), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));

        CompletableFuture<Void> future = action.resetCluster(manager, List.of(thisNodeName, node3.name()));
        assertThat(future, willCompleteSuccessfully());

        verify(restarter).initiateRestart();
    }

    @Test
    void repairCmgInitiatesRestartWhenMajorityOfCmgNodesRespondsWithOk() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3, node4, node5));
        prepareNodeStateForClusterReset();

        respondSuccessfullyFrom(thisNode, node2);
        respondWithExceptionFrom(node3, node4, node5);

        CompletableFuture<Void> future = manager.resetCluster(List.of(thisNodeName, node2.name(), node3.name()));
        assertThat(future, willCompleteSuccessfully());

        verify(restarter).initiateRestart();
    }

    @Test
    void repairMgInitiatesRestartWhenAllParticipatingNodesRespondWithOk() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node3, node4, node5));
        prepareNodeStateForClusterReset();

        respondSuccessfullyFrom(thisNode, node3, node4, node5);

        CompletableFuture<Void> future = manager.resetClusterRepairingMetastorage(List.of(thisNodeName, node3.name()), 1);
        assertThat(future, willCompleteSuccessfully());

        verify(restarter).initiateRestart();
    }

    private void respondSuccessfullyFrom(InternalClusterNode... nodes) {
        for (InternalClusterNode node : nodes) {
            respondSuccessfullyFrom(node);
        }
    }

    private void respondSuccessfullyFrom(InternalClusterNode node) {
        when(messagingService.invoke(eq(node), any(ResetClusterMessage.class), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));
    }

    private void respondWithExceptionFrom(InternalClusterNode... nodes) {
        for (InternalClusterNode node : nodes) {
            respondWithExceptionFrom(node);
        }
    }

    private void respondWithExceptionFrom(InternalClusterNode node) {
        when(messagingService.invoke(eq(node), any(), anyLong()))
                .thenReturn(failedFuture(new TimeoutException()));
    }

    @Test
    @DisplayName("resetCluster() fails and does not restart node when majority of new CMG nodes do not respond")
    void repairCmgFailsWhenNewCmgMajorityDoesNotRespond() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3, node4, node5));
        prepareNodeStateForClusterReset();

        respondSuccessfullyFrom(thisNode, node4, node5);
        respondWithExceptionFrom(node2, node3);

        CompletableFuture<Void> future = manager.resetCluster(List.of(thisNodeName, node2.name(), node3.name()));
        ClusterResetException ex = assertWillThrow(future, ClusterResetException.class);
        assertThat(ex.getMessage(), is("Did not get successful responses from new CMG majority, failing cluster reset."));

        verify(restarter, never()).initiateRestart();
    }

    @Test
    @DisplayName("resetClusterRepairingMetastorage() fails and does not restart node when any participating node does not respond")
    void resetClusterFailsWhenNewCmgMajorityDoesNotRespond() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node3, node4, node5));
        prepareNodeStateForClusterReset();

        respondSuccessfullyFrom(thisNode, node4, node5);
        respondWithExceptionFrom(node3);

        CompletableFuture<Void> future = manager.resetClusterRepairingMetastorage(List.of(thisNodeName, node3.name()), 1);
        ClusterResetException ex = assertWillThrow(future, ClusterResetException.class);
        assertThat(
                ex.getMessage(),
                is("Did not get successful response from at least one node, failing cluster reset [failedNode=node3].")
        );

        verify(restarter, never()).initiateRestart();
    }

    @CartesianTest
    void savesToVaultWhenGetsMessage(@Values(booleans = {false, true}) boolean fromSelf, @Values(booleans = {false, true}) boolean mgRepair)
            throws Exception {
        NetworkMessageHandler handler = extractMessageHandler();

        InternalClusterNode conductor = fromSelf ? thisNode : node3;
        handler.onReceived(resetClusterMessageOn2Nodes(mgRepair), conductor, 0L);

        waitTillResetClusterMessageGetsSavedToVault();
        VaultEntry entry = vaultManager.get(RESET_CLUSTER_MESSAGE_VAULT_KEY);
        assertThat(entry, is(notNullValue()));

        ResetClusterMessage savedMessage = VersionedSerialization.fromBytes(
                entry.value(),
                ResetClusterMessagePersistentSerializer.INSTANCE
        );

        assertThatResetClusterMessageContentIsAsExpected(savedMessage, mgRepair);
    }

    private void waitTillResetClusterMessageGetsSavedToVault() throws InterruptedException {
        assertTrue(waitForCondition(() -> vaultManager.get(RESET_CLUSTER_MESSAGE_VAULT_KEY) != null, 10_000));
    }

    private NetworkMessageHandler extractMessageHandler() {
        assertThat(manager.stopAsync(), willCompleteSuccessfully());

        var handlerRef = new AtomicReference<NetworkMessageHandler>();

        doAnswer(invocation -> {
            handlerRef.set(invocation.getArgument(1));
            return null;
        }).when(messagingService).addMessageHandler(eq(SystemDisasterRecoveryMessageGroup.class), any());

        assertThat(manager.startAsync(componentContext), willCompleteSuccessfully());

        NetworkMessageHandler handler = handlerRef.get();
        assertThat("Handler was not installed", handler, is(notNullValue()));

        return handler;
    }

    private ResetClusterMessage resetClusterMessageOn2Nodes() {
        return resetClusterMessageOn2Nodes(false);
    }

    private ResetClusterMessage resetClusterMessageOn2Nodes(boolean mgRepair) {
        ResetClusterMessageBuilder builder = messagesFactory.resetClusterMessage()
                .newCmgNodes(Set.of(thisNodeName, node3.name()))
                .currentMetaStorageNodes(usualClusterState.metaStorageNodes())
                .clusterName(CLUSTER_NAME)
                .clusterId(randomUUID())
                .formerClusterIds(List.of(usualClusterState.clusterTag().clusterId()))
                .initialClusterConfiguration(INITIAL_CONFIGURATION);

        if (mgRepair) {
            builder.metastorageReplicationFactor(1);
            builder.conductor(thisNodeName);
            builder.participatingNodes(Set.of(thisNodeName, node3.name(), node4.name()));
        }

        return builder.build();
    }

    @Test
    void respondsWhenGetsMessageFromSelf() {
        ArgumentCaptor<NetworkMessage> messageCaptor = ArgumentCaptor.forClass(NetworkMessage.class);

        NetworkMessageHandler handler = extractMessageHandler();
        InternalClusterNode conductor = thisNode;

        handler.onReceived(resetClusterMessageOn2Nodes(), conductor, 123L);

        InOrder inOrder = inOrder(messagingService, vaultManager);

        inOrder.verify(vaultManager, timeout(SECONDS.toMillis(10))).put(eq(RESET_CLUSTER_MESSAGE_VAULT_KEY), any());
        inOrder.verify(messagingService, timeout(SECONDS.toMillis(10))).respond(eq(conductor), messageCaptor.capture(), eq(123L));

        assertThat(messageCaptor.getValue(), instanceOf(SuccessResponseMessage.class));
    }

    @Test
    void respondsWhenGetsMessageFromOtherNode() {
        ArgumentCaptor<NetworkMessage> messageCaptor = ArgumentCaptor.forClass(NetworkMessage.class);

        NetworkMessageHandler handler = extractMessageHandler();
        InternalClusterNode conductor = node2;

        handler.onReceived(resetClusterMessageOn2Nodes(), conductor, 123L);

        InOrder inOrder = inOrder(messagingService, vaultManager, restarter);

        inOrder.verify(vaultManager, timeout(SECONDS.toMillis(10))).put(eq(RESET_CLUSTER_MESSAGE_VAULT_KEY), any());
        inOrder.verify(messagingService, timeout(SECONDS.toMillis(10))).respond(eq(conductor), messageCaptor.capture(), eq(123L));
        inOrder.verify(restarter, timeout(SECONDS.toMillis(10))).initiateRestart();

        assertThat(messageCaptor.getValue(), instanceOf(SuccessResponseMessage.class));
    }

    @Test
    void initiatesRestartWhenGetsMessageFromOtherNode() throws Exception {
        NetworkMessageHandler handler = extractMessageHandler();

        handler.onReceived(resetClusterMessageOn2Nodes(), node2, 0L);

        verify(restarter, timeout(SECONDS.toMillis(10))).initiateRestart();

        // Wait till it gets saved to Vault to avoid an attempt to write to it after the after-each method stops the Vault.
        // TODO: IGNITE-23144 - remove when busy locks are added to VaultManager.
        waitTillResetClusterMessageGetsSavedToVault();
    }

    @Test
    void doesNotInitiateRestartWhenGetsMessageFromSelf() throws Exception {
        NetworkMessageHandler handler = extractMessageHandler();

        handler.onReceived(resetClusterMessageOn2Nodes(), thisNode, 0L);

        verify(restarter, never()).initiateRestart();

        // Wait till it gets saved to Vault to avoid an attempt to write to it after the after-each method stops the Vault.
        // TODO: IGNITE-23144 - remove when busy locks are added to VaultManager.
        waitTillResetClusterMessageGetsSavedToVault();
    }

    @Test
    void migrateRequiresFormerClusterIdsToBePresent() {
        ClusterState stateWithoutFormerClusterIds = cmgMessagesFactory.clusterState()
                .cmgNodes(Set.of("node5"))
                .metaStorageNodes(Set.of("node6"))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(randomClusterTag(cmgMessagesFactory, CLUSTER_NAME))
                .formerClusterIds(null)
                .build();

        MigrateException ex = assertWillThrow(manager.migrate(stateWithoutFormerClusterIds), MigrateException.class);
        assertThat(ex.getMessage(), is("Migration can only happen using cluster state from a node that saw a cluster reset"));
    }

    @Test
    void migrateSendsMessagesToAllNodes() {
        putClusterState();

        ClusterState newState = newClusterState();

        ArgumentCaptor<ResetClusterMessage> messageCaptor = ArgumentCaptor.forClass(ResetClusterMessage.class);

        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));
        when(messagingService.invoke(any(InternalClusterNode.class), any(ResetClusterMessage.class), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));

        assertThat(manager.migrate(newState), willCompleteSuccessfully());

        verify(messagingService).invoke(eq(thisNode), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToSelf = messageCaptor.getValue();
        assertThatResetClusterMessageContentIsAsExpectedAfterMigrate(messageToSelf, newState);

        verify(messagingService).invoke(eq(node2), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToOtherNewCmgNode = messageCaptor.getValue();
        assertThatResetClusterMessageContentIsAsExpectedAfterMigrate(messageToOtherNewCmgNode, newState);

        verify(messagingService).invoke(eq(node3), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToOtherNonCmgNode = messageCaptor.getValue();
        assertThatResetClusterMessageContentIsAsExpectedAfterMigrate(messageToOtherNonCmgNode, newState);
    }

    private ClusterState newClusterState() {
        return cmgMessagesFactory.clusterState()
                .cmgNodes(Set.of("node5"))
                .metaStorageNodes(Set.of("node6"))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(randomClusterTag(cmgMessagesFactory, CLUSTER_NAME))
                .formerClusterIds(List.of(randomUUID(), randomUUID()))
                .initialClusterConfiguration(INITIAL_CONFIGURATION)
                .build();
    }

    private static void assertThatResetClusterMessageContentIsAsExpectedAfterMigrate(
            ResetClusterMessage message,
            ClusterState clusterState
    ) {
        assertThat(message, is(notNullValue()));

        assertThat(message.newCmgNodes(), is(clusterState.cmgNodes()));
        assertThat(message.currentMetaStorageNodes(), is(clusterState.metaStorageNodes()));
        assertThat(message.clusterName(), is(clusterState.clusterTag().clusterName()));
        assertThat(message.clusterId(), is(clusterState.clusterTag().clusterId()));
        assertThat(message.formerClusterIds(), is(clusterState.formerClusterIds()));
        assertThat(message.initialClusterConfiguration(), is(clusterState.initialClusterConfiguration()));
    }

    @Test
    void migrateInitiatesRestart() {
        putClusterState();

        ClusterState newState = newClusterState();

        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));
        respondSuccessfullyFrom(thisNode, node2);
        respondWithExceptionFrom(node3);

        assertThat(manager.migrate(newState), willCompleteSuccessfully());

        verify(restarter).initiateRestart();
    }

    @Test
    void migrationInWrongDirectionIsProhibited() {
        UUID clusterId1 = randomUUID();
        UUID clusterId2 = randomUUID();
        UUID clusterId3 = randomUUID();

        ClusterState oldClusterState = cmgMessagesFactory.clusterState()
                .cmgNodes(Set.of(thisNodeName))
                .metaStorageNodes(Set.of(thisNodeName))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(clusterTag(cmgMessagesFactory, CLUSTER_NAME, clusterId2))
                .initialClusterConfiguration(INITIAL_CONFIGURATION)
                .formerClusterIds(List.of(clusterId1))
                .build();

        ClusterState newClusterState = cmgMessagesFactory.clusterState()
                .cmgNodes(Set.of(thisNodeName))
                .metaStorageNodes(Set.of(thisNodeName))
                .version(IgniteProductVersion.CURRENT_VERSION.toString())
                .clusterTag(clusterTag(cmgMessagesFactory, CLUSTER_NAME, clusterId3))
                .initialClusterConfiguration(INITIAL_CONFIGURATION)
                .formerClusterIds(List.of(clusterId1, clusterId2))
                .build();

        manager.saveClusterState(newClusterState);

        MigrateException ex = assertWillThrow(manager.migrate(oldClusterState), MigrateException.class);
        assertThat(ex.getMessage(), is("Migration can only happen from old cluster to new one, not the other way around"));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    void resetClusterWithMgRequiresPositiveMgReplicationFactor(int metastorageReplicationFactor) {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));

        ClusterResetException ex = assertWillThrow(
                manager.resetClusterRepairingMetastorage(List.of(thisNodeName), metastorageReplicationFactor),
                ClusterResetException.class
        );
        assertThat(ex.getMessage(), is("Metastorage replication factor must be positive."));
    }

    @Test
    void resetClusterWithMgRequiresCurrentTopologyBeEnoughForMgReplicationFactor() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode));

        ClusterResetException ex = assertWillThrow(
                manager.resetClusterRepairingMetastorage(List.of(thisNodeName), 2),
                ClusterResetException.class
        );
        assertThat(ex.getMessage(), is("Metastorage replication factor cannot exceed size of current physical topology (1)."));
    }

    @Test
    void startMetastorageRepairReturnsIndexAndTerm() {
        when(metastorageMaintenance.raftNodeIndex()).thenReturn(completedFuture(new IndexWithTerm(234, 2)));

        NetworkMessageHandler handler = extractMessageHandler();
        handler.onReceived(startMetastorageRepairRequest(), thisNode, 123L);

        ArgumentCaptor<StartMetastorageRepairResponse> captor = ArgumentCaptor.forClass(StartMetastorageRepairResponse.class);
        verify(messagingService).respond(eq(thisNode), captor.capture(), eq(123L));

        StartMetastorageRepairResponse response = captor.getValue();
        assertThat(response.raftIndex(), is(234L));
        assertThat(response.raftTerm(), is(2L));
    }

    @Test
    void startsMetastorageRepairSavesClusterIdToVaultAsWitnessed() {
        when(metastorageMaintenance.raftNodeIndex()).thenReturn(completedFuture(new IndexWithTerm(234, 2)));

        NetworkMessageHandler handler = extractMessageHandler();
        handler.onReceived(startMetastorageRepairRequest(), thisNode, 123L);

        VaultEntry entry = vaultManager.get(WITNESSED_METASTORAGE_REPAIR_CLUSTER_ID_VAULT_KEY);
        assertThat(entry, is(notNullValue()));
        assertThat(entry.value(), is(uuidToBytes(clusterId)));
    }

    private StartMetastorageRepairRequest startMetastorageRepairRequest() {
        return messagesFactory.startMetastorageRepairRequest().build();
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3})
    void initiatesBecomingMetastorageLeader(int targetVotingSetSize) {
        Set<String> targetSet = IntStream.range(0, targetVotingSetSize)
                .mapToObj(n -> "node" + n)
                .collect(toSet());

        NetworkMessageHandler handler = extractMessageHandler();
        BecomeMetastorageLeaderMessage message = messagesFactory.becomeMetastorageLeaderMessage()
                .termBeforeChange(1)
                .targetVotingSet(targetSet)
                .build();
        handler.onReceived(message, thisNode, 123L);

        ArgumentCaptor<SuccessResponseMessage> captor = ArgumentCaptor.forClass(SuccessResponseMessage.class);
        verify(messagingService).respond(eq(thisNode), captor.capture(), eq(123L));
    }

    @Test
    void allowsClusterResetWhenCmgMajorityIsOnline() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));
        prepareNodeStateForClusterReset();

        when(messagingService.invoke(any(InternalClusterNode.class), any(ResetClusterMessage.class), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));

        CompletableFuture<Void> future = manager.resetCluster(List.of(thisNodeName, node2.name(), node3.name()));
        assertThat(future, willCompleteSuccessfully());
    }

    @Test
    void prohibitsMetastorageRepairWhenMgMajorityIsOnline() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));
        prepareNodeStateForClusterReset();

        ClusterResetException ex = assertWillThrow(
                manager.resetClusterRepairingMetastorage(List.of(thisNodeName, node2.name(), node3.name()), 3),
                ClusterResetException.class
        );
        assertThat(
                ex.getMessage(),
                is("Majority repair is rejected because majority of Metastorage nodes are online [metastorageNodes=[node2], "
                        + "onlineNodes=[node2, node3, node1]].")
        );
    }

    private enum ResetCluster {
        CMG_ONLY {
            @Override
            CompletableFuture<Void> resetCluster(SystemDisasterRecoveryManager manager, List<String> proposedCmgConsistentIds) {
                return manager.resetCluster(proposedCmgConsistentIds);
            }
        },
        CMG_AND_METASTORAGE {
            @Override
            CompletableFuture<Void> resetCluster(SystemDisasterRecoveryManager manager, List<String> proposedCmgConsistentIds) {
                return manager.resetClusterRepairingMetastorage(proposedCmgConsistentIds, 1);
            }
        };

        abstract CompletableFuture<Void> resetCluster(SystemDisasterRecoveryManager manager, List<String> proposedCmgConsistentIds);

        boolean mgRepair() {
            return this == CMG_AND_METASTORAGE;
        }
    }
}
