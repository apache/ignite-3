package org.apache.ignite.internal.storage.pagememory.mv;

import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointWorkflow;
import org.apache.ignite.internal.storage.BaseMvStoragesTest;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class CpWritePagesTest extends BaseMvStoragesTest {
    private static final int ROW_COUNT = 10_000;

    @InjectConfiguration("mock: {pageSize=4096, checkpoint: {interval=100000000, useAsyncFileIoFactory=true}}")
    private PersistentPageMemoryStorageEngineConfiguration engineConfigUseAsync;

    @InjectConfiguration("mock: {pageSize=4096, checkpoint: {interval=100000000, useAsyncFileIoFactory=false}}")
    private PersistentPageMemoryStorageEngineConfiguration engineConfigNotUseAsync;

    @InjectConfiguration("mock.profiles.default = {engine = \"aipersist\"}")
    private StorageConfiguration storageConfig;

    @WorkDirectory
    private Path workDir;

    private PersistentPageMemoryStorageEngine engine;

    private MvTableStorage table;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        boolean writeMultiThreaded = !testInfo.getTags().contains("oneThread");
        boolean useAsync = !testInfo.getTags().contains("notUseAsync");

        log.info(">>>>> Info: writeMultiThreaded={}, useAsync={}", writeMultiThreaded, useAsync);

        CheckpointWorkflow.WRITE_MULTI_THREADED = writeMultiThreaded;

        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new PersistentPageMemoryStorageEngine(
                "test",
                useAsync ? engineConfigUseAsync : engineConfigNotUseAsync,
                storageConfig,
                ioRegistry,
                workDir,
                null,
                mock(FailureProcessor.class),
                mock(LogSyncer.class),
                clock
        );

        engine.start();

        table = engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                mock(StorageIndexDescriptorSupplier.class)
        );

        log.info("Info: writeMultiThreaded={}, useAsync={}", writeMultiThreaded, useAsync);
    }

    @AfterEach
    protected void tearDown() throws Exception {
        IgniteUtils.closeAllManually(
                table,
                engine == null ? null : engine::stop
        );
    }

    private static final TestValue BIG_VALUE = createBigValue();

    @RepeatedTest(10)
    @Tag("oneThread")
    @Tag("useAsync")
    void testOneThreadUseAsync() throws Exception {
        test();
    }

    @RepeatedTest(10)
    @Tag("oneThread")
    @Tag("notUseAsync")
    void testOneThreadNotUseAsync() throws Exception {
        test();
    }

    @RepeatedTest(10)
    @Tag("multiThread")
    @Tag("useAsync")
    void testMultiThreadUseAsync() throws Exception {
        test();
    }

    @RepeatedTest(10)
    @Tag("multiThread")
    @Tag("notUseAsync")
    void testMultiThreadNotUseAsync() throws Exception {
        test();
    }

    private void test() throws Exception {
        for (int partitionId = 0; partitionId < DEFAULT_PARTITION_COUNT; partitionId++) {
            int partId = partitionId;

            MvPartitionStorage storage = getOrCreateMvPartition(table, partId);

            storage.runConsistently(locker -> {
                for (int i = 0; i < ROW_COUNT; i++) {
                    var rowId = new RowId(partId);

                    locker.lock(rowId);

                    storage.addWriteCommitted(
                            rowId,
                            binaryRow(new TestKey(i, "_" + i), BIG_VALUE),
                            clock.now()
                    );
                }

                return null;
            });
        }

        engine.checkpointManager().forceCheckpoint("test").futureFor(FINISHED).get(10, TimeUnit.MINUTES);
    }

    private static TestValue createBigValue() {
        int pageSize = 4096;

        // A repetitive pattern of 19 different characters (19 is chosen as a prime number) to reduce probability of 'lucky' matches
        // hiding bugs.
        String pattern = IntStream.range(0, 20)
                .mapToObj(ch -> String.valueOf((char) ('a' + ch)))
                .collect(joining());

        return new TestValue(1, pattern.repeat((int) (2.5 * pageSize / pattern.length())));
    }
}
