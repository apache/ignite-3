package org.apache.ignite.example.tx;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.example.ExampleTestUtils;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for transactional examples.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class TransactionsExamplesTest {
    /** Empty argument to invoke an example. */
    protected static final String[] EMPTY_ARGS = new String[0];

    /**
     * Runs TransactionsExample and checks its output.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testTransactionsExample() throws Exception {
        ExampleTestUtils.assertConsoleOutputContains(TransactionsExample::main, EMPTY_ARGS,
                "Initial balance: 1000.0",
                "Balance after the sync transaction: 1200.0",
                "Balance after the async transaction: 1500.0");
    }

    /**
     * Start node.
     *
     * @param workDir Work directory for the started node. Must not be {@code null}.
     */
    @BeforeEach
    public void startNode(@WorkDirectory Path workDir) throws IOException {
        IgnitionManager.start(
                "my-first-node",
                Files.readString(Path.of("config", "ignite-config.json")),
                workDir
        );
    }

    /**
     * Stop node.
     */
    @AfterEach
    public void stopNode() {
        IgnitionManager.stop("my-first-node");
    }

    /**
     * Removes a previously created work directory.
     */
    @BeforeEach
    @AfterEach
    public void removeWorkDir() {
        Path workDir = Path.of("work");

        if (Files.exists(workDir)) {
            IgniteUtils.deleteIfExists(workDir);
        }
    }
}
