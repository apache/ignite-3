package org.apache.ignite.internal.runner.app;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.app.IgniteCliRunner;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(WorkDirectoryExtension.class)
public class IgniteCliRunnerTest {
    @WorkDirectory
    private Path workDir;

    /** TODO: Replace this test by full integration test on the cli side IGNITE-15097. */
    @Test
    public void runnerArgsSmokeTest() throws IOException {
        Files.createDirectory(workDir.resolve("node1"));

        assertNotNull(IgniteCliRunner.start(
            new String[] {
                "--config", workDir.resolve("node1").toAbsolutePath().toString(),
                "--work-dir", workDir.resolve("node1").toAbsolutePath().toString(),
                "node1"
            }
        ));

        Files.createDirectory(workDir.resolve("node2"));

        assertNotNull(IgniteCliRunner.start(
            new String[] {
                "--work-dir", workDir.resolve("node2").toAbsolutePath().toString(),
                "node1"
            }
        ));
    }
}
