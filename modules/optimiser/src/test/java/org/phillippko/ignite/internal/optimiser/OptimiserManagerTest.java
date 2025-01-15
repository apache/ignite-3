package org.phillippko.ignite.internal.optimiser;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.ByteUtils.stringToBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.phillippko.ignite.internal.optimiser.OptimiserManager.RESULT_PREFIX;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OptimiserManagerTest extends BaseIgniteAbstractTest {
    private static final boolean WRITE_INTENSIVE = true;

    @Mock
    private TopologyService topologyService;

    @Mock
    private MessagingService messagingService;

    private final ComponentContext componentContext = new ComponentContext();

    private final OptimiserMessageFactory messageFactory = new OptimiserMessageFactory();

    private final OptimiseRunner optimiseRunner = mock(OptimiseRunner.class);

    private final BenchmarkRunner benchmarkRunner = mock(BenchmarkRunner.class);

    private final ExecutorService threadPool = Executors.newSingleThreadExecutor();

    private final MetaStorageManager metastorageManager = mock(MetaStorageManager.class);

    private static final String MOCK_OPTIMISER_RESULT = "mock optimiser result";

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

        when(optimiseRunner.getIssues(WRITE_INTENSIVE)).thenReturn(MOCK_OPTIMISER_RESULT);

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

        verify(metastorageManager).put(new ByteArray(RESULT_PREFIX + id), stringToBytes("STARTED"));
        verify(metastorageManager).put(new ByteArray(RESULT_PREFIX + id), stringToBytes(MOCK_OPTIMISER_RESULT));
    }
}