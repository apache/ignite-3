package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.compute.JobExecutorType;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Benchmark            Mode  Cnt      Score      Error   Units
 * execJavaLocal        avgt    3     24.401 ±   11.221   us/op
 * execDotNetLocal      avgt    3     67.523 ±   61.175   us/op
 * execJavaRemote       avgt    3    100.285 ±   45.470   us/op
 * execDotNetRemote     avgt    3    128.691 ±   73.251   us/op
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class ItComputeBenchmark {
    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  clientConnector.sendServerExceptionStackTraceToClient: true,\n"
            + "  rest.port: {},\n"
            + "  compute.threadPoolSize: 1,\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    /** Cluster nodes. */
    private Cluster CLUSTER;

    private IgniteClient CLIENT;

    @Setup
    public void startCluster() {
        ClusterConfiguration.Builder clusterConfiguration = ClusterConfiguration.builder(new TestInfo() {
                    @Override
                    public String getDisplayName() {
                        return "BENCH";
                    }

                    @Override
                    public Set<String> getTags() {
                        return Set.of();
                    }

                    @Override
                    public Optional<Class<?>> getTestClass() {
                        return Optional.empty();
                    }

                    @Override
                    public Optional<Method> getTestMethod() {
                        return Optional.empty();
                    }
                }, Paths.get(""))
                .defaultNodeBootstrapConfigTemplate(NODE_BOOTSTRAP_CFG_TEMPLATE);

        CLUSTER = new Cluster(clusterConfiguration.build());

        CLUSTER.startAndInit(initialNodes(), cmgMetastoreNodes());

        CLIENT = IgniteClient.builder().addresses("localhost:10802").build();
    }

    @TearDown
    public void stopCluster() {
        CLUSTER.shutdown();
    }

    private static int initialNodes() {
        return 3;
    }

    private static int[] cmgMetastoreNodes() {
        return new int[]{0};
    }

    @Benchmark
    public void execJavaLocal() {
        Ignite sourceNode = CLUSTER.node(2);
        JobTarget target = JobTarget.node(getClusterNode3(sourceNode).get());

        execJavaJob(sourceNode, target);
    }

    @Benchmark
    public void execJavaLocalClient() {
        Ignite sourceNode = CLIENT;
        JobTarget target = JobTarget.node(getClusterNode3(sourceNode).get());

        execJavaJob(sourceNode, target);
    }

    @Benchmark
    public void execJavaRemote() {
        Ignite sourceNode = CLUSTER.node(0);
        JobTarget target = JobTarget.node(getClusterNode3(sourceNode).get());

        execJavaJob(sourceNode, target);
    }

    @Benchmark
    public void execDotNetLocal() {
        Ignite sourceNode = CLUSTER.node(2);
        JobTarget target = JobTarget.node(getClusterNode3(sourceNode).get());

        execDotNetJob(sourceNode, target);
    }

    @Benchmark
    public void execDotNetRemote() {
        Ignite sourceNode = CLUSTER.node(0);
        JobTarget target = JobTarget.node(getClusterNode3(sourceNode).get());

        execDotNetJob(sourceNode, target);
    }

    private static void execJavaJob(Ignite sourceNode, JobTarget target) {
        JobDescriptor<Object, Object> desc = JobDescriptor.builder(EchoJob.class).build();

        var res = sourceNode.compute().execute(target, desc, "hello");
        assert res.equals("hello");
    }

    private static void execDotNetJob(Ignite sourceNode, JobTarget target) {
        JobExecutionOptions jobExecutionOptions = JobExecutionOptions.builder().executorType(JobExecutorType.DotNet).build();

        JobDescriptor<Object, Object> desc = JobDescriptor
                .builder("Apache.Ignite.Internal.ComputeExecutor.EchoJob, Apache.Ignite.Internal.ComputeExecutor")
                .options(jobExecutionOptions)
                .build();

        var res = sourceNode.compute().execute(target, desc, "hello");
        assert res.equals("hello");
    }

    private static @NotNull Optional<ClusterNode> getClusterNode3(Ignite sourceNode) {
        return sourceNode.clusterNodes()
                .stream()
                .filter(x -> "n_n_3346".equals(x.name()))
                .findFirst();
    }

    /**
     * Runner.
     *
     * @param args Arguments.
     * @throws RunnerException Exception.
     */
    public static void main(String[] args) throws RunnerException {
        ResourceLeakDetector.setLevel(Level.DISABLED);

        Options opt = new OptionsBuilder()
                .include(ItComputeBenchmark.class.getSimpleName())
                .addProfiler("gc")
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(7))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(15))
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    private static class EchoJob implements ComputeJob<Object, Object> {
        @Override
        public @Nullable CompletableFuture<Object> executeAsync(JobExecutionContext context, @Nullable Object arg) {
            return CompletableFuture.completedFuture(arg);
        }
    }
}
