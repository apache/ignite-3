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
import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

@State(Scope.Benchmark)
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
    public void get() {
        Ignite sourceNode = CLUSTER.node(0);

        Optional<ClusterNode> remoteNode = sourceNode.clusterNodes()
                .stream()
                .filter(x -> "n_n_3346".equals(x.name()))
                .findFirst();

        JobTarget target = JobTarget.node(remoteNode.get());

        JobDescriptor<Object, Object> desc = JobDescriptor.builder(EchoJob.class).build();

        var res = sourceNode.compute().execute(target, desc, "hello");
        assert res.equals("hello");
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
                .warmupTime(TimeValue.seconds(5))
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(5))
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
