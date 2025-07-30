package org.apache.ignite.internal.pagememory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.fileio.AsyncFileIoFactory;
import org.apache.ignite.internal.pagememory.configuration.CheckpointConfiguration;
import org.apache.ignite.internal.pagememory.configuration.PersistentDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.freelist.io.PagesListNodeIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.FakePartitionMeta.FakePartitionMetaFactory;
import org.apache.ignite.internal.pagememory.persistence.GroupPartitionId;
import org.apache.ignite.internal.pagememory.persistence.PartitionMetaManager;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemory;
import org.apache.ignite.internal.pagememory.persistence.PersistentPageMemoryMetricSource;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointManager;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointProgress;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStore;
import org.apache.ignite.internal.pagememory.persistence.store.FilePageStoreManager;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Fork(1)
public class CheckpointBenchmark {
    private static final int PAGE_SIZE = 4096;
    private static final int GROUP_ID = 1;
    private static final int PARTITION_ID = 0;
    private static final int SEGMENT_SIZE = 128 * Constants.MiB;
    private static final int CHECKPOINT_BUFFER_SIZE = 16 * Constants.MiB;

    @Param({"60000"})
    public int pageCount;

    @Param({"NEW_ONLY", "UPDATE_ONLY", "MIXED"})
    public PageUpdateMode updateMode;

    private Path workDir;
    private FailureManager failureManager;
    private AsyncFileIoFactory fileIoFactory;
    private PageIoRegistry ioRegistry;
    private ExecutorService executorService;
    private GroupPartitionId groupPartitionId;

    // Components that will be recreated for each invocation
    private PersistentPageMemory pageMemory;
    private FilePageStoreManager pageStoreManager;
    private PartitionMetaManager partitionMetaManager;
    private CheckpointManager checkpointManager;
    private FilePageStore filePageStore;
    private List<FullPageId> existingPages;

    public enum PageUpdateMode {
        NEW_ONLY,    // Only new pages
        UPDATE_ONLY, // Only update existing pages
        MIXED        // 50% new, 50% updates
    }

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        // Create persistent resources that will exist throughout the benchmark
        workDir = workDir();
        ioRegistry = new PageIoRegistry();
        ioRegistry.loadFromServiceLoader();
        executorService = Executors.newSingleThreadExecutor();
        failureManager = new FailureManager(new NoOpFailureHandler());
        fileIoFactory = new AsyncFileIoFactory();
        groupPartitionId = new GroupPartitionId(GROUP_ID, PARTITION_ID);
        pageStoreManager = new FilePageStoreManager("test", workDir, fileIoFactory, PAGE_SIZE, failureManager);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        IgniteUtils.closeAll(
                checkpointManager == null ? null : checkpointManager::stop,
                pageMemory == null ? null : () -> pageMemory.stop(true),
                pageStoreManager == null ? null : pageStoreManager::stop
        );

        shutdownAndAwaitTermination(executorService, 30, SECONDS);
        IgniteUtils.deleteIfExists(workDir);
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws Exception {
        // Clean up any resources from previous runs
        if (checkpointManager != null) {
            checkpointManager.stop();
            checkpointManager = null;
        }

        if (pageMemory != null) {
            pageMemory.stop(true);
            pageMemory = null;
        }

        if (filePageStore != null) {
            filePageStore.markToDestroy();
            filePageStore = null;
            assertThat(pageStoreManager.destroyPartition(groupPartitionId), willCompleteSuccessfully());
        }


        // Recreate clean components for this invocation
        List<DataRegion<PersistentPageMemory>> dataRegions = new CopyOnWriteArrayList<>();

        partitionMetaManager = new PartitionMetaManager(ioRegistry, PAGE_SIZE, new FakePartitionMetaFactory());

        checkpointManager = new CheckpointManager(
                "test", null, failureManager,
                CheckpointConfiguration.builder().build(),
                pageStoreManager, partitionMetaManager, dataRegions, ioRegistry,
                () -> {}, executorService, PAGE_SIZE
        );

        pageMemory = new PersistentPageMemory(
                PersistentDataRegionConfiguration.builder().pageSize(PAGE_SIZE).size(SEGMENT_SIZE + CHECKPOINT_BUFFER_SIZE).build(),
                new PersistentPageMemoryMetricSource("test"),
                ioRegistry,
                new long[]{SEGMENT_SIZE},
                CHECKPOINT_BUFFER_SIZE,
                pageStoreManager,
                (pageMem, pageId, pageBuf, newPage) -> {
                    checkpointManager.writePageToFilePageStore(pageMem, pageId, pageBuf, newPage);

                    CheckpointProgress checkpointProgress = checkpointManager.currentCheckpointProgress();
                    assertNotNull(checkpointProgress);
                    checkpointProgress.evictedPagesCounter().incrementAndGet();
                },
                checkpointManager.checkpointTimeoutLock(),
                new OffheapReadWriteLock(2)
        );

        // Start everything in the right order
        pageStoreManager.start();
        pageMemory.start();

        DataRegion<PersistentPageMemory> dataRegion = () -> pageMemory;
        dataRegions.add(dataRegion);

        checkpointManager.start();

        // Create the file page store
        filePageStore = pageStoreManager.readOrCreateStore(groupPartitionId, ByteBuffer.allocate(PAGE_SIZE));
        filePageStore.ensure();

        pageStoreManager.addStore(groupPartitionId, filePageStore);
        partitionMetaManager.addMeta(groupPartitionId, partitionMetaManager.readOrCreateMeta(
                null,
                groupPartitionId,
                filePageStore,
                ByteBuffer.allocateDirect(PAGE_SIZE).order(ByteOrder.LITTLE_ENDIAN)
        ));

        // Initialize pages based on the test mode
        existingPages = new ArrayList<>();

        setupPagesForTestMode();
    }

    private void setupPagesForTestMode() throws Exception {
        switch (updateMode) {
            case NEW_ONLY:
                // For NEW_ONLY, just create pages but don't checkpoint them
                for (int i = 0; i < pageCount; i++) {
                    createDirtyPage(pageMemory);
                }
                break;

            case UPDATE_ONLY:
                // Create pages and checkpoint them to make them "existing"
                for (int i = 0; i < pageCount; i++) {
                    createDirtyPage(pageMemory);
                }

                checkpointManager.forceCheckpoint("initial_checkpoint").futureFor(CheckpointState.FINISHED).get(30, SECONDS);
                waitForCondition(() -> filePageStore.deltaFileCount() == 0, 10000);

                // Now update all the existing pages
                for (FullPageId pageId : existingPages) {
                    updateExistingPage(pageMemory, pageId);
                }
                break;

            case MIXED:
                // Create half the pages and checkpoint them
                int halfCount = pageCount / 2;
                for (int i = 0; i < halfCount; i++) {
                    createDirtyPage(pageMemory);
                }

                checkpointManager.forceCheckpoint("initial_checkpoint").futureFor(CheckpointState.FINISHED).get(30, SECONDS);
                waitForCondition(() -> filePageStore.deltaFileCount() == 0, 10000);

                List<FullPageId> firstHalfPages = new ArrayList<>(existingPages);

                // Update existing pages
                for (FullPageId pageId : firstHalfPages) {
                    updateExistingPage(pageMemory, pageId);
                }

                // Create second half as new pages
                for (int i = 0; i < halfCount; i++) {
                    createDirtyPage(pageMemory);
                }
                break;
        }
    }

    @Benchmark
    public void checkpointOperation() throws Exception {
        // Force checkpoint and measure
        CheckpointProgress checkpointProgress = checkpointManager.forceCheckpoint("benchmark_checkpoint");
        checkpointProgress.futureFor(CheckpointState.FINISHED).get(30, SECONDS);

        while (filePageStore.deltaFileCount() != 0) {
            Thread.sleep(5);
        }
    }

    private FullPageId createDirtyPage(PersistentPageMemory pageMemory) throws Exception {
        checkpointManager.checkpointTimeoutLock().checkpointReadLock();

        long pageId = -1;
        long page = -1;

        try {
            pageId = pageMemory.allocatePageNoReuse(GROUP_ID, PARTITION_ID, PageIdAllocator.FLAG_DATA);
            page = pageMemory.acquirePage(GROUP_ID, pageId);

            long pageAddr = pageMemory.writeLock(GROUP_ID, pageId, page, true);

            try {
                PagesListNodeIo.VERSIONS.latest().initNewPage(pageAddr, pageId, PAGE_SIZE);
            } finally {
                pageMemory.writeUnlock(GROUP_ID, pageId, page, true);
            }
        } finally {
            if (page != -1) {
                pageMemory.releasePage(GROUP_ID, pageId, page);
            }

            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }

        FullPageId fullPageId = new FullPageId(pageId, GROUP_ID);
        existingPages.add(fullPageId);

        return fullPageId;
    }

    private void updateExistingPage(PersistentPageMemory pageMemory, FullPageId pageId) throws Exception {
        checkpointManager.checkpointTimeoutLock().checkpointReadLock();

        long page = -1;

        try {
            page = pageMemory.acquirePage(pageId.groupId(), pageId.pageId());

            long pageAddr = pageMemory.writeLock(pageId.groupId(), pageId.pageId(), page);

            try {
                // Modify the page data
//                byte randomValue = (byte)(System.nanoTime() % 256);
//                for (int i = 0; i < 10; i++) {
//                    int offset = 100 + i;
//                    PageUtils.putByte(pageAddr, offset, randomValue);
//                }
            } finally {
                pageMemory.writeUnlock(pageId.groupId(), pageId.pageId(), page, true);
            }
        } finally {
            if (page != -1) {
                pageMemory.releasePage(pageId.groupId(), pageId.pageId(), page);
            }

            checkpointManager.checkpointTimeoutLock().checkpointReadUnlock();
        }
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(CheckpointBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

    protected Path workDir() throws Exception {
        return Files.createTempDirectory("tmpDirPrefix").toFile().toPath();
    }
}