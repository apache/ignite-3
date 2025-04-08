package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.ClusterConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
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

    /** Work directory. */
    @WorkDirectory
    private Path WORK_DIR;

    @Setup
    protected void startCluster() {
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
                }, WORK_DIR)
                .defaultNodeBootstrapConfigTemplate(NODE_BOOTSTRAP_CFG_TEMPLATE);

        CLUSTER = new Cluster(clusterConfiguration.build());

        CLUSTER.startAndInit(initialNodes(), cmgMetastoreNodes());
    }

    @TearDown
    protected void stopCluster() {
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
        CLUSTER.aliveNode().tables().tables();
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
}
