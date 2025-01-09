package java.phillippko.org.optimiser;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.phillippko.ignite.internal.optimiser.OptimiserManager.RESULT_PREFIX;

import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.persistence.PersistentVaultService;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.phillippko.ignite.internal.optimiser.BenchmarkRunner;
import org.phillippko.ignite.internal.optimiser.OptimiseRunner;
import org.phillippko.ignite.internal.optimiser.OptimiserManager;
import org.phillippko.ignite.internal.optimiser.OptimiserMessageFactory;

@ExtendWith(MockitoExtension.class)
@ExtendWith(WorkDirectoryExtension.class)
class SystemDisasterRecoveryManagerImplTest extends BaseIgniteAbstractTest {
    private static final boolean WRITE_INTENSIVE = true;

    private static final String thisNodeName = "node1";

    @Mock
    private TopologyService topologyService;

    @Mock
    private MessagingService messagingService;

    private final ComponentContext componentContext = new ComponentContext();

    private final ClusterNode thisNode = new ClusterNodeImpl(randomUUID(), thisNodeName, new NetworkAddress("host", 1001));

    private final OptimiserMessageFactory messageFactory = new OptimiserMessageFactory();

    private final OptimiseRunner optimiseRunner = mock(OptimiseRunner.class);

    private final BenchmarkRunner benchmarkRunner = mock(BenchmarkRunner.class);

    private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

    private final MetaStorageManager metastorageManager = mock(MetaStorageManager.class);

    private OptimiserManager manager;

    @BeforeEach
    void init() {
        manager = new OptimiserManager(
                metastorageManager,
                threadPool,
                messagingService,
                messageFactory,
                topologyService,
                optimiseRunner,
                benchmarkRunner
        );

        assertThat(manager.startAsync(componentContext), willCompleteSuccessfully());
    }

    @AfterEach
    void cleanup() {
        threadPool.shutdown();

        assertThat(manager.stopAsync(), willCompleteSuccessfully());
    }

    @Test
    void runOptimise() {
        CompletableFuture<UUID> optimise = manager.optimise(null, WRITE_INTENSIVE);

        // Вызов завершится успешно и вернет UUID
        assertThat(optimise, willBe(notNullValue()));

        UUID id = optimise.join();

        // Имя узла не указано, должно быть отправлено на локальный узел.
        verify(topologyService, times(1)).localMember();
        verifyNoMoreInteractions(topologyService);

        verify(metastorageManager).put(new ByteArray(RESULT_PREFIX + id), ByteUtils.stringToBytes("STARTED"));
    }
}