package org.apache.ignite.example.code.deployment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.ignite.example.util.DeployComputeUnit;

public class AbstractDeploymentUnitExample {

    // Root path of the project
    private static final Path projectRoot = Paths.get("").toAbsolutePath();

    // Pre-built JAR from deploymentUnitJar task (built at compile time)
    private static final Path DEFAULT_JAR_PATH =
            projectRoot.resolve("examples/java/build/libs/deploymentunit-example-1.0.0.jar");

    protected static String jarPathAsString = "";
    protected static Path jarPath = DEFAULT_JAR_PATH;
    protected static boolean runFromIDE = true;

    /**
     * Processes the deployment unit.
     *
     * @param args Arguments passed to the deployment process.
     * @throws IOException if any error occurs.
     */
    protected static void processDeploymentUnit(String[] args) throws IOException {
        Map<String, Object> p = DeployComputeUnit.processArguments(args);

        boolean newRunFromIDE = (boolean) p.get("runFromIDE");
        String newJarPathStr = (String) p.get("jarPath");

        runFromIDE = newRunFromIDE;

        // Use isBlank() instead of trim().isEmpty() to avoid creating a new String
        if (newJarPathStr != null && !newJarPathStr.isBlank()) {
            jarPathAsString = newJarPathStr;
            jarPath = Path.of(newJarPathStr);
        }

        // JAR is pre-built at compile time via deploymentUnitJar task
        // No runtime JAR building needed - just verify it exists
        if (!Files.exists(jarPath)) {
            throw new IllegalStateException(
                "Deployment unit JAR not found at: " + jarPath + "\n" +
                "Please build the project first: ./gradlew :ignite-examples:build"
            );
        }
    }
}