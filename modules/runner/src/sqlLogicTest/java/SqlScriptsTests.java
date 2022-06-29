/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.base.Strings;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.sql.script.ScriptRunnerTestsEnvironment;
import org.apache.ignite.internal.sql.script.SqlScriptRunner;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicContainer;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite to run SQL test scripts.
 *
 * <p>By default, only "*.test" and "*.test_slow" scripts are run.
 * Other files are ignored.
 *
 * <p>All test files consist of appropriate collection of queries.
 * A query record begins with a line of the following form: query &lt;type-string&gt; &lt;sort-mode&gt; &lt;label&gt;
 *
 * <p>The SQL for the query is found on second an subsequent lines of the record up to first line of the form "----"
 * or until the end of the record. Lines following the "----" are expected results of the query, one value per line. If the "----" and/or
 * the results are omitted, then the query is expected to return an empty set. The "----" and results are also omitted from prototype
 * scripts and are always ignored when the sqllogictest program is operating in completion mode. Another way of thinking about completion
 * mode is that it copies the script from input to output, replacing all "----" lines and subsequent result values with the actual results
 * from running the query.
 *
 * <p>The &lt;type-string&gt; argument to the query statement is a short string that specifies the number of result columns and
 * the expected datatype of each result column. There is one character in the &lt;type-string&gt; for each result column. The characters
 * codes are "T" for a text result, "I" for an integer result, and "R" for a floating-point result.
 *
 * <p>The &lt;sort-mode&gt; argument is optional. If included, it must be one of "nosort", "rowsort", or "valuesort".
 * The default is "nosort". In nosort mode, the results appear in exactly the order in which they were received from the database engine.
 * The nosort mode should only be used on queries that have an ORDER BY clause or which only have a single row of result, since otherwise
 * the order of results is undefined and might vary from one database engine to another. The "rowsort" mode gathers all output from the
 * database engine then sorts it by rows on the client side. Sort comparisons use strcmp() on the rendered ASCII text representation of the
 * values. Hence, "9" sorts after "10", not before. The "valuesort" mode works like rowsort except that it does not honor row groupings.
 * Each individual result value is sorted on its own.
 *
 * <p>The &lt;label&gt; argument is also optional. If included, sqllogictest stores a hash of the results of this query under
 * the given label. If the label is reused, then sqllogictest verifies that the results are the same. This can be used to verify that two or
 * more queries in the same test script that are logically equivalent always generate the same output.
 *
 * <p>In the results section, integer values are rendered as if by printf("%d"). Floating point values are rendered as
 * if by printf("%.3f"). NULL values are rendered as "NULL". Empty strings are rendered as "(empty)". Within non-empty strings, all control
 * characters and unprintable characters are rendered as "@".
 *
 * @see <a href="https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki">Extended format documentation.</a>
 */
@ExtendWith({WorkDirectoryExtension.class, SystemPropertiesExtension.class})
@WithSystemProperty(key = "IMPLICIT_PK_ENABLED", value = "true")
@ScriptRunnerTestsEnvironment(scriptsRoot = "src/sqlLogicTest/sql")
public class SqlScriptsTests {
    private static final String NODE_NAME_PREFIX = "sqllogic";

    private static final FileSystem FS = FileSystems.getDefault();

    private static final IgniteLogger LOG = IgniteLogger.forClass(SqlScriptsTests.class);

    /** Base port number. */
    private static final int BASE_PORT = 3344;

    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  \"network\": {\n"
            + "    \"port\":{},\n"
            + "    \"nodeFinder\":{\n"
            + "      \"netClusterNodes\": [ {} ]\n"
            + "    }\n"
            + "  }\n"
            + "}";

    /** Cluster nodes. */
    private static final List<Ignite> CLUSTER_NODES = new ArrayList<>();

    /** Work directory. */
    @WorkDirectory
    private static Path WORK_DIR;

    private static Path SCRIPTS_ROOT;

    private static int NODES;

    private static Pattern TEST_REGEX;

    private static boolean RESTART_CLUSTER;

    @BeforeAll
    static void init() throws Exception {
        config();

        startNodes();
    }

    @AfterAll
    static void shutdown() throws Exception {
        stopNodes();
    }

    @TestFactory
    @Timeout(3 * 60)
    public Stream<DynamicNode> sql() {
        return sqlTestsFolder(SCRIPTS_ROOT);
    }

    private Stream<DynamicNode> sqlTestsFolder(Path dir) {
        try {
            AtomicBoolean first = new AtomicBoolean(true);
            return Files.list(dir).sorted()
                    .filter(p -> {
                        if (TEST_REGEX != null) {
                            return (Files.isDirectory(p) && directoryMatch(p)) || TEST_REGEX.matcher(p.toString()).find();
                        } else {
                            return Files.isDirectory(p)
                                    || p.toString().endsWith(".test")
                                    || p.toString().endsWith(".test_slow");
                        }
                    })
                    .map((p) -> {
                        if (Files.isDirectory(p)) {
                            return DynamicContainer.dynamicContainer(
                                    p.getFileName().toString(),
                                    sqlTestsFolder(p)
                            );
                        } else {
                            try {
                                final boolean firstInDir = first.get();

                                return DynamicTest.dynamicTest(p.getFileName().toString(), p.toUri(), () -> {
                                    if (firstInDir && RESTART_CLUSTER) {
                                        stopNodes();

                                        startNodes();
                                    }

                                    run(p);
                                });
                            } finally {
                                first.set(false);
                            }
                        }
                    });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private boolean directoryMatch(Path dir) {
        try {
            return Files.walk(dir).anyMatch(p -> !Files.isDirectory(p) && TEST_REGEX.matcher(p.toString()).find());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void run(Path testPath) {
        beforeTest();

        LOG.info(">>> Start: " + SCRIPTS_ROOT.relativize(testPath));

        SqlScriptRunner r = new SqlScriptRunner(
                testPath,
                CollectionUtils.first(CLUSTER_NODES).sql(),
                LOG
        );

        try {
            r.run();
        } catch (Error | RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void beforeTest() {
        for (Table t : CLUSTER_NODES.get(0).tables().tables()) {
            try (Session s = CLUSTER_NODES.get(0).sql().createSession()) {
                try (ResultSet rs = s.execute(null, "DROP TABLE " + t.name())) {
                    assertTrue(rs.wasApplied());
                }
            }
        }
    }

    private static void config() {
        ScriptRunnerTestsEnvironment env = SqlScriptsTests.class.getAnnotation(ScriptRunnerTestsEnvironment.class);

        assert env != null;
        assert !Strings.isNullOrEmpty(env.scriptsRoot());

        SCRIPTS_ROOT = FS.getPath(env.scriptsRoot());
        NODES = env.nodes();
        TEST_REGEX = Strings.isNullOrEmpty(env.regex()) ? null : Pattern.compile(env.regex());
        RESTART_CLUSTER = env.restart();
    }

    private static void startNodes() {
        String connectNodeAddr = "\"localhost:" + BASE_PORT + '\"';

        List<CompletableFuture<Ignite>> futures = IntStream.range(0, NODES)
                .mapToObj(i -> {
                    String nodeName = NODE_NAME_PREFIX + i;

                    String config = IgniteStringFormatter.format(NODE_BOOTSTRAP_CFG, BASE_PORT + i, connectNodeAddr);

                    return IgnitionManager.start(nodeName, config, WORK_DIR.resolve(nodeName));
                })
                .collect(toList());

        String metaStorageNodeName = NODE_NAME_PREFIX + "0";

        IgnitionManager.init(metaStorageNodeName, List.of(metaStorageNodeName), "cluster");

        for (CompletableFuture<Ignite> future : futures) {
            assertThat(future, willCompleteSuccessfully());

            CLUSTER_NODES.add(future.join());
        }
    }

    private static void stopNodes() throws Exception {
        LOG.info(">>> Stopping cluster...");

        CLUSTER_NODES.clear();

        List<AutoCloseable> closeables = IntStream.range(0, NODES)
                .mapToObj(i -> NODE_NAME_PREFIX + i)
                .map(nodeName -> (AutoCloseable) () -> IgnitionManager.stop(nodeName))
                .collect(toList());

        IgniteUtils.closeAll(closeables);

        LOG.info(">>> Cluster is stopped.");
    }
}
