package phillippko.org.optimiser;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.util.ByteUtils.stringToBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.pagememory.configuration.schema.PersistentPageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.schema.configuration.GcConfiguration;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfiguration;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfiguration;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.storage.configurations.StorageExtensionConfiguration;
import org.apache.ignite.internal.storage.configurations.StorageProfileView;
import org.apache.ignite.internal.storage.rocksdb.configuration.schema.RocksDbProfileConfiguration;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

public class OptimiserManager implements IgniteComponent {
    private static final String RESULT_PREFIX = "optimiser.result.";
    private final ExecutorService threadPool;
    private final MessagingService messagingService;
    private final MetaStorageManager metastorageManager;
    private final ConfigurationRegistry clusterConfigurationRegistry;
    private final ConfigurationRegistry nodeConfigurationRegistry;
    private final IgniteSqlImpl sql;
    private final OptimiserMessageFactory messageFactory;
    private final TopologyService topologyService;

    public OptimiserManager(
            IgniteSqlImpl sql,
            ConfigurationRegistry nodeConfigurationRegistry,
            ConfigurationRegistry clusterConfigurationRegistry,
            MetaStorageManager metastorageManager,
            ExecutorService threadPool,
            MessagingService messagingService,
            OptimiserMessageFactory messageFactory,
            TopologyService topologyService
    ) {
        this.messageFactory = messageFactory;
        this.topologyService = topologyService;
        this.sql = sql;
        this.clusterConfigurationRegistry = clusterConfigurationRegistry;
        this.nodeConfigurationRegistry = nodeConfigurationRegistry;
        this.threadPool = threadPool;
        this.messagingService = messagingService;
        this.metastorageManager = metastorageManager;
    }

    public CompletableFuture<UUID> optimise(@Nullable String nodeName, boolean writeIntensive) {
        UUID id = UUID.randomUUID();

        ClusterNode targetNode = getNode(nodeName);

        OptimiseMessage message = messageFactory.optimiseMessage()
                .writeIntensive(writeIntensive)
                .id(id)
                .build();

        messagingService.invoke(targetNode, message, 10_000);

        return completedFuture(id);
    }

    public CompletableFuture<UUID> runBenchmark(@Nullable String nodeName, String benchmarkFilePath) {
        UUID id = UUID.randomUUID();

        ClusterNode targetNode = getNode(nodeName);

        NetworkMessage message = messageFactory.runBenchmarkMessage()
                .benchmarkFileName(benchmarkFilePath)
                .id(id)
                .build();

        messagingService.invoke(targetNode, message, 10_000);

        return completedFuture(id);
    }

    public CompletableFuture<String> optimisationResult(UUID id) {
        return metastorageManager.get(ByteArray.fromString(RESULT_PREFIX + id))
                .thenApply(it -> ByteUtils.stringFromBytes(it.value()));
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(OptimiserMessageGroup.class, this::handleMessage);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return nullCompletedFuture();
    }

    private void runOptimiseInternal(UUID id, boolean writeIntensive) {
        ByteArray key = ByteArray.fromString(RESULT_PREFIX + id);

        metastorageManager.put(key, "STARTED".getBytes())
                .thenApplyAsync((v) -> getIssues(writeIntensive), threadPool)
                .thenAccept((issues) -> metastorageManager.put(key, stringToBytes(issues)))
                .exceptionally(e -> {
                    metastorageManager.put(key, e.getMessage().getBytes());

                    return null;
                });
    }

    private String getIssues(boolean writeIntensive) {
        List<String> issues = new ArrayList<>();

        NamedListView<StorageProfileView> profiles = nodeConfigurationRegistry.getConfiguration(StorageExtensionConfiguration.KEY).storage()
                .profiles().value();

        if ((double) Runtime.getRuntime().freeMemory() / Runtime.getRuntime().totalMemory() < 0.1) {
            issues.add("More than 90% of heap memory is used, consider increasing heap size");
        }

        MemoryUsage heapMemoryUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        GcConfiguration gcConfig = clusterConfigurationRegistry.getConfiguration(GcExtensionConfiguration.KEY).gc();

        boolean lowFreeMemory = (double) heapMemoryUsage.getUsed() / heapMemoryUsage.getMax() < 0.1;

        if (lowFreeMemory && gcConfig.threads().value() < Runtime.getRuntime().availableProcessors() / 2) {
            issues.add("Consider increasing garbage collection threads: " + gcConfig.threads());
        }

        LowWatermarkConfiguration lowWatermarkConfiguration = gcConfig.lowWatermark();
        if (lowWatermarkConfiguration.dataAvailabilityTime().value() > 1_000_000) {
            issues.add("Low watermark data availability is too high, causing extra disk usage: "
                    + lowWatermarkConfiguration.dataAvailabilityTime());
        }

        if (lowWatermarkConfiguration.updateInterval().value() > lowWatermarkConfiguration.dataAvailabilityTime().value() / 2) {
            issues.add(
                    "Low watermark update interval is too high [updateInterval=" +
                            lowWatermarkConfiguration.updateInterval() +
                            ", dataAvailabilityTime=" +
                            lowWatermarkConfiguration.dataAvailabilityTime() + "]"
            );
        }

        long volatileDataRegionsSize = profiles.stream()
                .filter(VolatilePageMemoryProfileConfiguration.class::isInstance)
                .map(VolatilePageMemoryProfileConfiguration.class::cast)
                .mapToLong(it -> it.maxSize().value())
                .sum();

        if (volatileDataRegionsSize > heapMemoryUsage.getMax()) {
            issues.add("Sum of volatile data regions exceeds heap size: " + volatileDataRegionsSize);
        }

        if (writeIntensive) {
            String persistentProfiles = profiles.stream()
                    .filter(PersistentPageMemoryProfileConfiguration.class::isInstance)
                    .map(StorageProfileView::name).collect(Collectors.joining());

            if (!persistentProfiles.isEmpty()) {
                issues.add("For write-intensive workloads RocksDB should be used instead page memory in these profiles: "
                        + persistentProfiles);
            }
        } else {
            String rocksDbProfiles = profiles.stream()
                    .filter(RocksDbProfileConfiguration.class::isInstance)
                    .map(StorageProfileView::name).collect(Collectors.joining());

            if (!rocksDbProfiles.isEmpty()) {
                issues.add(
                        "For read-intensive workloads page memory should be used instead of RocksDB in these profiles: " + rocksDbProfiles
                );
            }
        }

        return String.join("; ", issues);
    }

    private void runBenchmarkInternal(UUID id, String benchmarkFilePath) {
        ByteArray key = ByteArray.fromString(RESULT_PREFIX + id);

        metastorageManager.put(key, "STARTED".getBytes())
                .thenApplyAsync((v) -> runBenchmarkInternal(benchmarkFilePath), threadPool)
                .thenAccept((result) -> metastorageManager.put(key, stringToBytes(result)));
    }

    private String runBenchmarkInternal(String benchmarkFilePath) {
        long atStart = currentTimeMillis();

        try (var ignored = sql.execute(null, readFromFile(benchmarkFilePath))) {
            return String.valueOf(currentTimeMillis() - atStart);
        } catch (Throwable e) {
            return "FAILED: " + e.getMessage();
        }
    }

    private static String readFromFile(String benchmarkFilePath) {
        try {
            return Files.readString(Path.of(benchmarkFilePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ClusterNode getNode(@Nullable String nodeName) {
        if (nodeName == null) {
            return topologyService.localMember();
        }

        ClusterNode node = topologyService.getByConsistentId(nodeName);

        if (node == null) {
            throw new IllegalArgumentException("Node not found by name: " + nodeName);
        }

        return node;
    }

    private void handleMessage(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof RunBenchmarkMessage) {
            RunBenchmarkMessage runBenchmarkMessage = (RunBenchmarkMessage) message;

            UUID id = runBenchmarkMessage.id();

            runBenchmarkInternal(id, runBenchmarkMessage.benchmarkFileName());
        } else if (message instanceof OptimiseMessage) {
            OptimiseMessage optimiseMessage = (OptimiseMessage) message;

            runOptimiseInternal(optimiseMessage.id(), optimiseMessage.writeIntensive());
        }
    }
}
