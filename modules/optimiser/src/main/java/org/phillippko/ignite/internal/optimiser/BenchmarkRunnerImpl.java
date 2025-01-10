package org.phillippko.ignite.internal.optimiser;

import static java.lang.System.currentTimeMillis;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlException;
import org.apache.ignite.sql.SqlRow;

public class BenchmarkRunnerImpl implements BenchmarkRunner {
    private static final IgniteLogger LOG = Loggers.forClass(BenchmarkRunnerImpl.class);
    private final IgniteSql sql;

    public BenchmarkRunnerImpl(IgniteSql sql) {
        this.sql = sql;
    }

    @Override
    public String runBenchmark(String benchmarkFilePath) {
        LOG.info("Benchmarking " + benchmarkFilePath + "...");

        long atStart = currentTimeMillis();

        Path path = Path.of(benchmarkFilePath);

        if (!Files.exists(path)) {
            return "File not found: " + path.toAbsolutePath();
        }

        List<String> statements = readFromFile(path);

        for (String statement : statements) {
            try (ResultSet<SqlRow> ignored = sql.execute(null, statement)) {
            } catch (SqlException e) {
                return "Benchmark " + benchmarkFilePath + " failed with SQL error: " + e.getMessage();
            }
        }

        String result = "Benchmark " + benchmarkFilePath + " finished in " + (currentTimeMillis() - atStart) + "MS";

        LOG.info(result);

        return result;
    }

    private static List<String> readFromFile(Path path) {
        try {
            return Files.readAllLines(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
