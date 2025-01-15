package org.phillippko.ignite.internal.optimiser;

import static org.apache.ignite.internal.util.ByteUtils.stringFromBytes;
import static org.apache.ignite.internal.util.ByteUtils.stringToBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OptimiserManager implements IgniteComponent {
    public static final String RESULT_PREFIX = "optimiser.result.";
    private static final Logger LOG = LoggerFactory.getLogger(OptimiserManager.class);
    private final ExecutorService threadPool;
    private final MessagingService messagingService;
    private final MetaStorageManager metastorageManager;
    private final OptimiserMessageFactory messageFactory;
    private final TopologyService topologyService;
    private final OptimiseRunner optimiseRunner;
    private final BenchmarkRunner benchmarkRunner;

    public OptimiserManager(
            MetaStorageManager metastorageManager,
            ExecutorService threadPool,
            MessagingService messagingService,
            OptimiserMessageFactory messageFactory,
            TopologyService topologyService,
            OptimiseRunner optimiseRunner,
            BenchmarkRunner benchmarkRunner
    ) {
        this.messageFactory = messageFactory;
        this.optimiseRunner = optimiseRunner;
        this.benchmarkRunner = benchmarkRunner;
        this.topologyService = topologyService;
        this.threadPool = threadPool;
        this.messagingService = messagingService;
        this.metastorageManager = metastorageManager;
    }

    public CompletableFuture<UUID> optimise(@Nullable String nodeName, boolean writeIntensive) {
        LOG.info("Running optimisation");

        UUID id = UUID.randomUUID();

        ClusterNode targetNode = getNode(nodeName);

        OptimiseMessage message = messageFactory.optimiseMessage()
                .writeIntensive(writeIntensive)
                .id(id)
                .build();

        return messagingService.send(targetNode, message)
                .thenApply(ignored -> id);
    }

    public CompletableFuture<UUID> runBenchmark(@Nullable String nodeName, String benchmarkFilePath) {
        LOG.info("Running a benchmark");

        UUID id = UUID.randomUUID();

        ClusterNode targetNode = getNode(nodeName);

        NetworkMessage message = messageFactory.runBenchmarkMessage()
                .benchmarkFileName(benchmarkFilePath)
                .id(id)
                .build();

        return messagingService.send(targetNode, message)
                .thenApply(ignored -> id);
    }

    public CompletableFuture<String> optimisationResult(UUID id) {
        return metastorageManager.get(ByteArray.fromString(RESULT_PREFIX + id))
                .handle((res, e) ->  {
                    LOG.info("Getting info about " + id);

                    if (e != null) {
                        LOG.error("Couldn't get result: ", e);

                        throw new CompletionException(e);
                    }

                    if (res.empty() || res.tombstone()) {
                        throw new CompletionException(new IllegalArgumentException("Incorrect ID"));
                    }

                    return "Optimisation result: " + stringFromBytes(res.value());
                });
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
                .thenApplyAsync((v) -> optimiseRunner.getIssues(writeIntensive), threadPool)
                .thenAccept((issues) -> metastorageManager.put(key, stringToBytes(issues)))
                .exceptionally(e -> {
                    LOG.error("Error while optimising: ", e);

                    metastorageManager.put(key, stringToBytes("Optimisation FAILED: " + e.getMessage()));

                    return null;
                });
    }

    private void runBenchmarkInternal(UUID id, String benchmarkFilePath) {
        ByteArray key = ByteArray.fromString(RESULT_PREFIX + id);

        metastorageManager.put(key, "Benchmark STARTED".getBytes())
                .thenApplyAsync((v) -> benchmarkRunner.runBenchmark(benchmarkFilePath), threadPool)
                .thenAccept((result) -> metastorageManager.put(key, stringToBytes(result)));
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
