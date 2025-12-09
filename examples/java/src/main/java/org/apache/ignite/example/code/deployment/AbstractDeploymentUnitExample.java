package org.apache.ignite.example.code.deployment;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.ignite.example.util.DeployComputeUnit;

public class AbstractDeploymentUnitExample {

    // Root path of ignite-examples/
    private static final Path projectRoot =
            Paths.get("").toAbsolutePath();

    // Compiled class output when running from IDE/CLI
    private static final Path DEFAULT_CLASSES_DIR =
            projectRoot.resolve("examples/java/build/classes/java/main");

    // Default JAR output
    private static final Path DEFAULT_JAR_PATH =
            Path.of("build/libs/deploymentunit-example-1.0.0.jar");

    protected static String jarPathAsString = "";
    protected static Path jarPath = DEFAULT_JAR_PATH;
    protected static boolean runFromIDE = true;
    // ---------------------------------------------------

    /**
     * Processes the deployment unit.
     *
     * @param args Arguments passed to the deployment process.
     * @throws IOException if any error occurs.
     */
    protected static void processDeploymentUnit(String[] args)
            throws IOException {

        Map<String, Object> p = DeployComputeUnit.processArguments(args);

        boolean newRunFromIDE = (boolean) p.get("runFromIDE");
        String newJarPathStr = (String) p.get("jarPath");

        runFromIDE = newRunFromIDE;

        // Use isBlank() instead of trim().isEmpty() to avoid creating a new String
        if (newJarPathStr != null && !newJarPathStr.isBlank()) {
            jarPathAsString = newJarPathStr;
            jarPath = Path.of(newJarPathStr);
        }

        if (runFromIDE) {
            DeployComputeUnit.buildJar(
                    DEFAULT_CLASSES_DIR,
                    DEFAULT_JAR_PATH
            );
        }
    }
}