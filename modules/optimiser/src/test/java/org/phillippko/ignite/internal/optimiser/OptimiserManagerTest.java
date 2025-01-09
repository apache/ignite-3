//package java.phillippko.org.optimiser;
//
//import static java.util.UUID.randomUUID;
//import static java.util.concurrent.CompletableFuture.completedFuture;
//import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
//import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.is;
//import static org.hamcrest.Matchers.notNullValue;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyLong;
//import static org.mockito.Mockito.lenient;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.spy;
//
//import java.nio.file.Path;
//import java.util.Set;
//import java.util.UUID;
//import org.apache.ignite.internal.disaster.system.message.SystemDisasterRecoveryMessagesFactory;
//import org.apache.ignite.internal.lang.ByteArray;
//import org.apache.ignite.internal.manager.ComponentContext;
//import org.apache.ignite.internal.metastorage.impl.MetastorageGroupMaintenance;
//import org.apache.ignite.internal.network.ClusterNodeImpl;
//import org.apache.ignite.internal.network.ConstantClusterIdSupplier;
//import org.apache.ignite.internal.network.MessagingService;
//import org.apache.ignite.internal.network.NetworkMessage;
//import org.apache.ignite.internal.network.TopologyService;
//import org.apache.ignite.internal.properties.IgniteProductVersion;
//import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
//import org.apache.ignite.internal.testframework.WorkDirectory;
//import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
//import org.apache.ignite.internal.vault.VaultEntry;
//import org.apache.ignite.internal.vault.VaultManager;
//import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
//import org.apache.ignite.network.ClusterNode;
//import org.apache.ignite.network.NetworkAddress;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.junit.jupiter.api.extension.ExtendWith;
//import org.mockito.Mock;
//import org.mockito.junit.jupiter.MockitoExtension;
//import phillippko.org.optimiser.OptimiserManager;
//
//@ExtendWith(MockitoExtension.class)
//@ExtendWith(WorkDirectoryExtension.class)
//class SystemDisasterRecoveryManagerImplTest extends BaseIgniteAbstractTest {
//    private static final String CLUSTER_NAME = "cluster";
//
//    private static final ByteArray INIT_CONFIG_APPLIED_VAULT_KEY = new ByteArray("systemRecovery.initConfigApplied");
//    private static final ByteArray CLUSTER_STATE_VAULT_KEY = new ByteArray("systemRecovery.clusterState");
//    private static final ByteArray RESET_CLUSTER_MESSAGE_VAULT_KEY = new ByteArray("systemRecovery.resetClusterMessage");
//    private static final ByteArray WITNESSED_METASTORAGE_REPAIR_CLUSTER_ID_VAULT_KEY
//            = new ByteArray("systemRecovery.witnessedMetastorageRepairClusterId");
//
//    private static final String INITIAL_CONFIGURATION = "initial-config";
//
//    @WorkDirectory
//    private Path workDir;
//
//    private static final String thisNodeName = "node1";
//
//    @Mock
//    private TopologyService topologyService;
//
//    @Mock
//    private MessagingService messagingService;
//
//    private VaultManager vaultManager;
//
//    @Mock
//    private ServerRestarter restarter;
//
//    @Mock
//    private MetastorageGroupMaintenance metastorageMaintenance;
//
//    private SystemDisasterRecoveryManagerImpl manager;
//
//    private final ComponentContext componentContext = new ComponentContext();
//
//    private final ClusterNode thisNode = new ClusterNodeImpl(randomUUID(), thisNodeName, new NetworkAddress("host", 1001));
//
//    private final ClusterNode node2 = new ClusterNodeImpl(randomUUID(), "node2", new NetworkAddress("host", 1002));
//    private final ClusterNode node3 = new ClusterNodeImpl(randomUUID(), "node3", new NetworkAddress("host", 1003));
//    private final ClusterNode node4 = new ClusterNodeImpl(randomUUID(), "node4", new NetworkAddress("host", 1004));
//    private final ClusterNode node5 = new ClusterNodeImpl(randomUUID(), "node5", new NetworkAddress("host", 1005));
//
//    private final UUID clusterId = new UUID(1, 2);
//
//    private final CmgMessagesFactory cmgMessagesFactory = new CmgMessagesFactory();
//    private final SystemDisasterRecoveryMessagesFactory messagesFactory = new SystemDisasterRecoveryMessagesFactory();
//
//    private final ClusterState usualClusterState = cmgMessagesFactory.clusterState()
//            .cmgNodes(Set.of(thisNodeName))
//            .metaStorageNodes(Set.of(thisNodeName))
//            .version(IgniteProductVersion.CURRENT_VERSION.toString())
//            .clusterTag(randomClusterTag(cmgMessagesFactory, CLUSTER_NAME))
//            .initialClusterConfiguration(INITIAL_CONFIGURATION)
//            .build();
//
//    private final SuccessResponseMessage successResponseMessage = cmgMessagesFactory.successResponseMessage().build();
//
//    @BeforeEach
//    void init() {
//        vaultManager = spy(new VaultManager(new PersistentVaultService(workDir.resolve("vault"))));
//        assertThat(vaultManager.startAsync(componentContext), willCompleteSuccessfully());
//
//        lenient().when(messagingService.respond(any(ClusterNode.class), any(NetworkMessage.class), anyLong()))
//                .thenReturn(nullCompletedFuture());
//
//        ClusterManagementGroupManager cmgManager = mock(ClusterManagementGroupManager.class);
//
//        lenient().when(cmgManager.clusterState()).thenReturn(completedFuture(usualClusterState));
//
//        manager = new OptimiserManager(
//                thisNodeName,
//                topologyService,
//                messagingService,
//                vaultManager,
//                restarter,
//                metastorageMaintenance,
//                cmgManager,
//                new ConstantClusterIdSupplier(clusterId),
//                topologyService
//        );
//        assertThat(manager.startAsync(componentContext), willCompleteSuccessfully());
//    }
//
//    @AfterEach
//    void cleanup() {
//        assertThat(vaultManager.stopAsync(), willCompleteSuccessfully());
//        assertThat(manager.stopAsync(), willCompleteSuccessfully());
//    }
//
//    @Test
//    void marksInitConfigApplied() {
//        manager.markInitConfigApplied();
//
//        VaultEntry entry = vaultManager.get(INIT_CONFIG_APPLIED_VAULT_KEY);
//        assertThat(entry, is(notNullValue()));
//        assertThat(entry.value(), is(notNullValue()));
//    }
