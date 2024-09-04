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
import static org.apache.ignite.internal.cluster.management.ClusterTag.randomClusterTag;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.asserts.CompletableFutureAssert.assertWillThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ArrayUtils.BYTE_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.cluster.management.ClusterState;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.internal.cluster.management.network.messages.SuccessResponseMessage;
import org.apache.ignite.internal.disaster.system.message.ResetClusterMessage;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessageGroup;
import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

    private SystemDisasterRecoveryManagerImpl manager;

    private final ComponentContext componentContext = new ComponentContext();

    private final ClusterNode thisNode = new ClusterNodeImpl(randomUUID().toString(), thisNodeName, new NetworkAddress("host", 1001));

    private final ClusterNode node2 = new ClusterNodeImpl(randomUUID().toString(), "node2", new NetworkAddress("host", 1002));
    private final ClusterNode node3 = new ClusterNodeImpl(randomUUID().toString(), "node3", new NetworkAddress("host", 1003));
    private final ClusterNode node4 = new ClusterNodeImpl(randomUUID().toString(), "node4", new NetworkAddress("host", 1004));
    private final ClusterNode node5 = new ClusterNodeImpl(randomUUID().toString(), "node5", new NetworkAddress("host", 1005));

    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();
    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();

    private final ClusterState usualClusterState = cmgMessagesFactory.clusterState()
            .cmgNodes(Set.of(thisNodeName))
            .metaStorageNodes(Set.of(thisNodeName))
            .version(IgniteProductVersion.CURRENT_VERSION.toString())
            .clusterTag(randomClusterTag(cmgMessagesFactory, CLUSTER_NAME))
            .build();

    private final SuccessResponseMessage successResponseMessage = cmgMessagesFactory.successResponseMessage().build();

    @BeforeEach
    void init() {
        vaultManager = spy(new VaultManager(new PersistentVaultService(workDir.resolve("vault"))));
        assertThat(vaultManager.startAsync(componentContext), willCompleteSuccessfully());

        lenient().when(messagingService.respond(any(ClusterNode.class), any(NetworkMessage.class), anyLong()))
                .thenReturn(nullCompletedFuture());

        manager = new SystemDisasterRecoveryManagerImpl(
                thisNodeName,
                topologyService,
                messagingService,
                vaultManager,
                restarter
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

        ClusterState savedState = fromBytes(entry.value());
        assertThat(savedState, is(equalTo(usualClusterState)));
    }

    @Test
    void resetClusterRejectsDuplicateNodeNames() {
        ClusterResetException ex = assertWillThrow(
                manager.resetCluster(List.of(thisNodeName, thisNodeName)),
                ClusterResetException.class,
                10, SECONDS
        );
        assertThat(ex.getMessage(), is("New CMG node consistentIds have repetitions: [node1, node1]."));
    }

    @Test
    void resetClusterRequiresThisNodeToBeNewCmg() {
        ClusterResetException ex = assertWillThrow(
                manager.resetCluster(List.of("abc")),
                ClusterResetException.class,
                10, SECONDS
        );
        assertThat(ex.getMessage(), is("Current node is not contained in the new CMG, so it cannot conduct a cluster reset."));
    }

    @Test
    void resetClusterRequiresNewCmgNodesToBeOnline() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode));

        ClusterResetException ex = assertWillThrow(
                manager.resetCluster(List.of(thisNodeName, "abc")),
                ClusterResetException.class,
                10, SECONDS
        );
        assertThat(ex.getMessage(), is("Some of proposed CMG nodes are not online: [abc]."));
    }

    @Test
    void resetClusterRequiresClusterState() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode));
        markinitConfigApplied();

        ClusterResetException ex = assertWillThrow(
                manager.resetCluster(List.of(thisNodeName)),
                ClusterResetException.class,
                10, SECONDS
        );
        assertThat(ex.getMessage(), is("Node does not have cluster state."));
    }

    @Test
    void resetClusterRequiresInitConfigToBeApplied() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode));
        putClusterState();

        ClusterResetException ex = assertWillThrow(
                manager.resetCluster(List.of(thisNodeName)),
                ClusterResetException.class,
                10, SECONDS
        );
        assertThat(ex.getMessage(), is("Initial configuration is not applied and cannot serve as a cluster reset conductor."));
    }

    private void putClusterState() {
        vaultManager.put(CLUSTER_STATE_VAULT_KEY, toBytes(usualClusterState));
    }

    private void markinitConfigApplied() {
        vaultManager.put(INIT_CONFIG_APPLIED_VAULT_KEY, BYTE_EMPTY_ARRAY);
    }

    @Test
    void resetClusterSendsMessages() {
        ArgumentCaptor<ResetClusterMessage> messageCaptor = ArgumentCaptor.forClass(ResetClusterMessage.class);

        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));
        prepareNodeStateForClusterReset();

        when(messagingService.invoke(any(ClusterNode.class), any(), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));

        CompletableFuture<Void> future = manager.resetCluster(List.of(thisNodeName, node2.name()));
        assertThat(future, willCompleteSuccessfully());

        verify(messagingService).invoke(eq(thisNode), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToSelf = messageCaptor.getValue();
        assertThatResetClusterMessageIsAsExpected(messageToSelf);

        verify(messagingService).invoke(eq(node2), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToOtherNewCmgNode = messageCaptor.getValue();
        assertThatResetClusterMessageIsAsExpected(messageToOtherNewCmgNode);

        verify(messagingService).invoke(eq(node3), messageCaptor.capture(), anyLong());
        ResetClusterMessage messageToOtherNonCmgNode = messageCaptor.getValue();
        assertThatResetClusterMessageIsAsExpected(messageToOtherNonCmgNode);

        assertThat(messageToSelf.clusterId(), is(messageToOtherNewCmgNode.clusterId()));
        assertThat(messageToSelf.clusterId(), is(messageToOtherNonCmgNode.clusterId()));
    }

    private void prepareNodeStateForClusterReset() {
        markinitConfigApplied();
        putClusterState();
    }

    private void assertThatResetClusterMessageIsAsExpected(ResetClusterMessage message) {
        assertThatResetClusterMessageContentIsAsExpected(message);
    }

    private void assertThatResetClusterMessageContentIsAsExpected(@Nullable ResetClusterMessage message) {
        assertThat(message, is(notNullValue()));
        assertThat(message.cmgNodes(), containsInAnyOrder(thisNodeName, node2.name()));
        assertThat(message.metaStorageNodes(), is(usualClusterState.metaStorageNodes()));
        assertThat(message.clusterName(), is(CLUSTER_NAME));
        assertThat(message.clusterId(), is(not(usualClusterState.clusterTag().clusterId())));
        assertThat(message.formerClusterIds(), contains(usualClusterState.clusterTag().clusterId()));
    }

    @Test
    void resetClusterInitiatesRestartOnSuccess() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));
        prepareNodeStateForClusterReset();

        when(messagingService.invoke(any(ClusterNode.class), any(), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));

        CompletableFuture<Void> future = manager.resetCluster(List.of(thisNodeName, node2.name(), node3.name()));
        assertThat(future, willCompleteSuccessfully());

        verify(restarter).initiateRestart();
    }

    @Test
    void resetClusterInitiatesRestartWhenMajorityOfCmgNodesRespondsWithOk() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3, node4, node5));
        prepareNodeStateForClusterReset();

        respondSuccessfullyFrom(thisNode, node2);
        respondWithExceptionFrom(node3, node4, node5);

        CompletableFuture<Void> future = manager.resetCluster(List.of(thisNodeName, node2.name(), node3.name()));
        assertThat(future, willCompleteSuccessfully());

        verify(restarter).initiateRestart();
    }

    private void respondSuccessfullyFrom(ClusterNode... nodes) {
        for (ClusterNode node : nodes) {
            respondSuccessfullyFrom(node);
        }
    }

    private void respondSuccessfullyFrom(ClusterNode node) {
        when(messagingService.invoke(eq(node), any(), anyLong()))
                .thenReturn(completedFuture(successResponseMessage));
    }

    private void respondWithExceptionFrom(ClusterNode... nodes) {
        for (ClusterNode node : nodes) {
            respondWithExceptionFrom(node);
        }
    }

    private void respondWithExceptionFrom(ClusterNode node) {
        when(messagingService.invoke(eq(node), any(), anyLong()))
                .thenReturn(failedFuture(new TimeoutException()));
    }

    @Test
    @DisplayName("resetCluster() fails and does not restart when majority of new CMG nodes do not respond")
    void resetClusterFailsWhenNewCmgMajorityDoesNotRespond() {
        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3, node4, node5));
        prepareNodeStateForClusterReset();

        respondSuccessfullyFrom(thisNode, node4, node5);
        respondWithExceptionFrom(node2, node3);

        CompletableFuture<Void> future = manager.resetCluster(List.of(thisNodeName, node2.name(), node3.name()));
        ClusterResetException ex = assertWillThrow(future, ClusterResetException.class, 10, SECONDS);
        assertThat(ex.getMessage(), is("Did not get successful responses from new CMG majority, failing cluster reset."));

        verify(restarter, never()).initiateRestart();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void savesToVaultWhenGetsMessage(boolean fromSelf) throws Exception {
        NetworkMessageHandler handler = extractMessageHandler();

        ClusterNode conductor = fromSelf ? thisNode : node2;
        handler.onReceived(resetClusterMessageOn2Nodes(), conductor, 0L);

        waitTillResetClusterMessageGetsSavedToVault();
        VaultEntry entry = vaultManager.get(RESET_CLUSTER_MESSAGE_VAULT_KEY);
        assertThat(entry, is(notNullValue()));

        ResetClusterMessage savedMessage = fromBytes(entry.value());

        assertThatResetClusterMessageContentIsAsExpected(savedMessage);
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
        return messagesFactory.resetClusterMessage()
                .cmgNodes(Set.of(thisNodeName, node2.name()))
                .metaStorageNodes(usualClusterState.metaStorageNodes())
                .clusterName(CLUSTER_NAME)
                .clusterId(randomUUID())
                .formerClusterIds(List.of(usualClusterState.clusterTag().clusterId()))
                .build();
    }

    @Test
    void respondsWhenGetsMessageFromSelf() {
        ArgumentCaptor<NetworkMessage> messageCaptor = ArgumentCaptor.forClass(NetworkMessage.class);

        NetworkMessageHandler handler = extractMessageHandler();
        ClusterNode conductor = thisNode;

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
        ClusterNode conductor = node2;

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
        waitTillResetClusterMessageGetsSavedToVault();
    }

    @Test
    void doesNotInitiateRestartWhenGetsMessageFromSelf() throws Exception {
        NetworkMessageHandler handler = extractMessageHandler();

        handler.onReceived(resetClusterMessageOn2Nodes(), thisNode, 0L);

        verify(restarter, never()).initiateRestart();

        // Wait till it gets saved to Vault to avoid an attempt to write to it after the after-each method stops the Vault.
        waitTillResetClusterMessageGetsSavedToVault();
    }

    @Test
    void migrateSendsMessagesToAllNodes() {
        ClusterState newState = newClusterState();

        ArgumentCaptor<ResetClusterMessage> messageCaptor = ArgumentCaptor.forClass(ResetClusterMessage.class);

        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));
        when(messagingService.invoke(any(ClusterNode.class), any(), anyLong()))
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
                .build();
    }

    private static void assertThatResetClusterMessageContentIsAsExpectedAfterMigrate(
            ResetClusterMessage message,
            ClusterState clusterState
    ) {
        assertThat(message, is(notNullValue()));

        assertThat(message.cmgNodes(), is(clusterState.cmgNodes()));
        assertThat(message.metaStorageNodes(), is(clusterState.metaStorageNodes()));
        assertThat(message.clusterName(), is(clusterState.clusterTag().clusterName()));
        assertThat(message.clusterId(), is(clusterState.clusterTag().clusterId()));
        assertThat(message.formerClusterIds(), is(clusterState.formerClusterIds()));
    }

    @Test
    void migrateInitiatesRestart() {
        ClusterState newState = newClusterState();

        when(topologyService.allMembers()).thenReturn(List.of(thisNode, node2, node3));
        respondSuccessfullyFrom(thisNode, node2);
        respondWithExceptionFrom(node3);

        assertThat(manager.migrate(newState), willCompleteSuccessfully());

        verify(restarter).initiateRestart();
    }
}
