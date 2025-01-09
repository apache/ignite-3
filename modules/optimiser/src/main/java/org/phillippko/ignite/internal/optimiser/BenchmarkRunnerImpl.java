package org.phillippko.ignite.internal.optimiser;

import static java.lang.System.currentTimeMillis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.sql.IgniteSql;

public class BenchmarkRunnerImpl implements BenchmarkRunner {
    private static final IgniteLogger LOG = Loggers.forClass(BenchmarkRunnerImpl.class);
    private final IgniteSql sql;

    public BenchmarkRunnerImpl(IgniteSql sql) {
        this.sql = sql;
    }

    @Override
    public String runBenchmark(String benchmarkFilePath) {
        long atStart = currentTimeMillis();

        try (var ignored = sql.execute(null, readFromFile(benchmarkFilePath))) {
            return String.valueOf(currentTimeMillis() - atStart);
        } catch (Throwable e) {
            LOG.info("Error while running a benchmark: ", e);

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
}
